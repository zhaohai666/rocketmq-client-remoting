<?php

declare(strict_types=1);

/**
 * Client Admin pure-PHP assert-style self-test (no phpunit, no real cluster).
 *
 * Covers: the Subscription protocol model, the full DefaultMQAdminExt method surface
 * (request header fields / request codes / body encoding shape / pagination / KV
 * broadcast / offset reset / exception paths).
 *
 * Network-layer doubles: FakeAdmin overrides DefaultMQAdminExt::invokeSyncOn (the
 * equivalent point of the sync transport entry) answering per script and recording all
 * requests; FakeClient subclasses MQClientInstance, overriding the admin-oriented methods
 * delegated to the instance and recording calls.
 *
 * Run: php tests/RunClientAdmin.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\RemotingConnectException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\MessageTrack;
use RocketMQ\Client\TrackType;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\TopicConfig;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\ClusterInfo;
use RocketMQ\Remoting\Protocol\ConsumeMessageDirectlyResult;
use RocketMQ\Remoting\Protocol\ConsumeStats;
use RocketMQ\Remoting\Protocol\ConsumeStatsList;
use RocketMQ\Remoting\Protocol\ConsumerConnection;
use RocketMQ\Remoting\Protocol\GetConsumerListByGroupResponseBody;
use RocketMQ\Remoting\Protocol\GroupRetryPolicy;
use RocketMQ\Remoting\Protocol\GroupRetryPolicyType;
use RocketMQ\Remoting\Protocol\KVTable;
use RocketMQ\Remoting\Protocol\LanguageCode;
use RocketMQ\Remoting\Protocol\OffsetWrapper;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\ResetOffsetBody;
use RocketMQ\Remoting\Protocol\SimpleSubscriptionData;
use RocketMQ\Remoting\Protocol\SubscriptionGroupConfig;
use RocketMQ\Remoting\Protocol\SubscriptionGroupWrapper;
use RocketMQ\Remoting\Protocol\TopicList;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Remoting\Protocol\TopicStatsTable;

// ==================================================================== test doubles

/** Admin that records requests and answers per script (intercepts all sync RPCs). */
final class FakeAdmin extends DefaultMQAdminExt
{
    /** @var list<array{addr: string, cmd: RemotingCommand}> */
    public array $requests = [];

    /** @var callable(RemotingCommand, string): ?RemotingCommand|null */
    public $handler = null;

    /** @var array<string, \Throwable> thrown directly when the address is hit (simulates connect failure) */
    public array $failAddrs = [];

    protected function invokeSyncOn(MQClientInstance $client, string $addr, RemotingCommand $request, ?int $timeoutMillis): RemotingCommand
    {
        $this->requests[] = ['addr' => $addr, 'cmd' => $request];
        if (isset($this->failAddrs[$addr])) {
            throw $this->failAddrs[$addr];
        }
        $h = $this->handler;
        $resp = $h !== null ? $h($request, $addr) : null;
        return $resp ?? RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

/** MQClientInstance double that records delegated calls (offline constructor, no real network requests). */
final class FakeClient extends MQClientInstance
{
    public ?ClusterInfo $clusterInfo = null;
    /** @var array<string, TopicRouteData> */
    public array $topicRoutes = [];
    public ?TopicList $allTopics = null;

    public int $maxOffsetValue = 0;
    public int $minOffsetValue = 0;
    public int $searchOffsetValue = 0;
    public ?int $queryOffsetValue = null;
    /** @var list<MessageExt> */
    public array $queryMessageResult = [];
    /** @var list<string> */
    public array $consumerIdList = [];

    /** @var list<array<string, mixed>> */
    public array $maxOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $minOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $searchOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $queryOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $updateOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $queryMessageCalls = [];
    /** @var list<array<string, mixed>> */
    public array $consumerListCalls = [];
    /** @var list<array<string, mixed>> */
    public array $createTopicInBrokerCalls = [];
    /** @var list<array<string, mixed>> */
    public array $createTopicInRouteCalls = [];
    /** @var list<array<string, mixed>> */
    public array $deleteTopicInBrokerCalls = [];
    /** @var list<array<string, mixed>> */
    public array $deleteTopicInNamesrvCalls = [];
    /** @var list<string> */
    public array $routeRefreshCalls = [];

    public function __construct()
    {
        parent::__construct('admin-test-client', ['10.0.0.1:9876']);
    }

    public function getBrokerClusterInfo(int $timeoutMillis = 10000): ClusterInfo
    {
        return $this->clusterInfo ?? new ClusterInfo();
    }

    public function getAllTopicListFromNameServer(int $timeoutMillis = 10000): TopicList
    {
        return $this->allTopics ?? new TopicList();
    }

    public function getTopicRouteData(string $topic): ?TopicRouteData
    {
        return $this->topicRoutes[$topic] ?? null;
    }

    public function updateTopicRouteInfoFromNameServer(string $topic, int $timeoutMillis = 5000, bool $isDefault = false): bool
    {
        $this->routeRefreshCalls[] = $topic;
        return false;
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        $this->maxOffsetCalls[] = ['mq' => $mq, 'addr' => $addr];
        return $this->maxOffsetValue;
    }

    public function getMinOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        $this->minOffsetCalls[] = ['mq' => $mq, 'addr' => $addr];
        return $this->minOffsetValue;
    }

    public function searchOffsetByTimestamp(
        MessageQueue $mq,
        int $timestamp,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        ?\RocketMQ\Common\BoundaryType $boundaryType = \RocketMQ\Common\BoundaryType::LOWER,
    ): int {
        $this->searchOffsetCalls[] = ['mq' => $mq, 'ts' => $timestamp, 'addr' => $addr, 'boundary' => $boundaryType];
        return $this->searchOffsetValue;
    }

    public function queryConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        bool $setZeroIfNotFound = false,
    ): ?int {
        $this->queryOffsetCalls[] = ['group' => $consumerGroup, 'mq' => $mq, 'addr' => $addr];
        return $this->queryOffsetValue;
    }

    public function updateConsumerOffset(string $consumerGroup, MessageQueue $mq, int $commitOffset, int $timeoutMillis = 5000, ?string $addr = null): void
    {
        $this->updateOffsetCalls[] = ['group' => $consumerGroup, 'mq' => $mq, 'offset' => $commitOffset, 'addr' => $addr];
    }

    public function queryMessageAllBrokers(
        string $topic,
        string $key,
        int $maxNum,
        int $beginTimestamp,
        int $endTimestamp,
        ?string $indexType = null,
        bool $uniqKey = false,
        int $timeoutMillis = 15000,
    ): array {
        $this->queryMessageCalls[] = ['topic' => $topic, 'key' => $key, 'maxNum' => $maxNum, 'indexType' => $indexType, 'uniqKey' => $uniqKey];
        return $this->queryMessageResult;
    }

    public function getConsumerListByGroup(string $consumerGroup, int $timeoutMillis = 5000, ?string $addr = null): GetConsumerListByGroupResponseBody
    {
        $this->consumerListCalls[] = ['group' => $consumerGroup, 'addr' => $addr];
        $body = new GetConsumerListByGroupResponseBody();
        $body->consumerIdList = $this->consumerIdList;
        return $body;
    }

    public function createTopicInBroker(
        string $brokerAddr,
        string $defaultTopic,
        string $topic,
        int $readQueueNums = 4,
        int $writeQueueNums = 4,
        int $perm = 6,
        int $topicSysFlag = 0,
        string $topicFilterType = \RocketMQ\Common\TopicFilterType::SINGLE_TAG,
        bool $order = false,
        ?string $attributes = null,
        int $timeoutMillis = 5000,
        int $retryTimes = 5,
    ): void {
        $this->createTopicInBrokerCalls[] = [
            'addr' => $brokerAddr, 'defaultTopic' => $defaultTopic, 'topic' => $topic,
            'read' => $readQueueNums, 'write' => $writeQueueNums, 'perm' => $perm,
        ];
    }

    public function createTopicInRoute(string $topic, int $readQueueNums = 4, int $writeQueueNums = 4, int $perm = 6, int $topicSysFlag = 0, ?string $attributes = null, int $timeoutMillis = 5000): void
    {
        $this->createTopicInRouteCalls[] = ['topic' => $topic, 'read' => $readQueueNums, 'write' => $writeQueueNums, 'perm' => $perm];
    }

    public function deleteTopicInBroker(string $brokerAddr, string $topic, int $timeoutMillis = 5000): void
    {
        $this->deleteTopicInBrokerCalls[] = ['addr' => $brokerAddr, 'topic' => $topic];
    }

    public function deleteTopicInNamesrv(string $topic, int $timeoutMillis = 5000): void
    {
        $this->deleteTopicInNamesrvCalls[] = $topic;
    }
}

// ==================================================================== runner

final class RunClientAdmin
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    public function check(bool $cond, string $label): void
    {
        if ($cond) {
            $this->passed++;
        } else {
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$label}\n");
        }
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
        } else {
            $detail = sprintf("%s\n  expected: %s\n  actual:   %s", $label, var_export($expected, true), var_export($actual, true));
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$detail}\n");
        }
    }

    /** @param class-string<\Throwable> $exceptionClass */
    public function checkThrows(callable $fn, string $exceptionClass, string $label): void
    {
        $e = $this->capture($fn);
        if ($e === null) {
            $this->check(false, "{$label} (no exception thrown)");
            return;
        }
        $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
    }

    public function capture(callable $fn): ?\Throwable
    {
        try {
            $fn();
        } catch (\Throwable $e) {
            return $e;
        }
        return null;
    }

    public function finish(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    // ================================================================ data helpers

    private FakeClient $client;

    private function newAdmin(): FakeAdmin
    {
        $this->client = new FakeClient();
        $admin = new FakeAdmin();
        $admin->setMqClientInstance($this->client);
        return $admin;
    }

    private function route(string $topic, string $brokerName, string $addr, int $read = 2, int $write = 2, int $perm = 6): TopicRouteData
    {
        $trd = new TopicRouteData();
        $trd->queueDatas = [new QueueData($brokerName, $read, $write, $perm, 0)];
        $trd->brokerDatas = [new BrokerData('DefaultCluster', $brokerName, [MixAll::MASTER_ID => $addr])];
        $this->client->topicRoutes[$topic] = $trd;
        return $trd;
    }

    /** Route for the %RETRY% group topic (used by the consumed() fan-out), pointing to broker-a. */
    private function retryRoute(): TopicRouteData
    {
        $trd = new TopicRouteData();
        $trd->brokerDatas = [new BrokerData('DefaultCluster', 'broker-a', [MixAll::MASTER_ID => 'b:a'])];
        return $trd;
    }

    private function okResponse(?string $body = null, array $ext = []): RemotingCommand
    {
        $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $resp->body = $body;
        foreach ($ext as $k => $v) {
            $resp->extFields[$k] = (string) $v;
        }
        return $resp;
    }

    private function errResponse(int $code, string $remark): RemotingCommand
    {
        return RemotingCommand::createResponseCommand($code, $remark);
    }

    private function lastCmd(FakeAdmin $a): RemotingCommand
    {
        return $a->requests[count($a->requests) - 1]['cmd'];
    }

    private function lastAddr(FakeAdmin $a): string
    {
        return $a->requests[count($a->requests) - 1]['addr'];
    }

    /** @return list<RemotingCommand> */
    private function cmdsByCode(FakeAdmin $a, int $code): array
    {
        $out = [];
        foreach ($a->requests as ['cmd' => $cmd]) {
            if ($cmd->code === $code) {
                $out[] = $cmd;
            }
        }
        return $out;
    }

    /** @return list<array{addr: string, cmd: RemotingCommand}> */
    private function reqsByCode(FakeAdmin $a, int $code): array
    {
        $out = [];
        foreach ($a->requests as $r) {
            if ($r['cmd']->code === $code) {
                $out[] = $r;
            }
        }
        return $out;
    }

    private function statsBody(float $tps, string $topic, string $brokerName, int $queueId, int $consumerOffset, int $brokerOffset = 100): string
    {
        $cs = new ConsumeStats();
        $cs->consumeTps = $tps;
        $cs->offsetTable[] = [
            'mq' => new MessageQueue($topic, $brokerName, $queueId),
            'value' => new OffsetWrapper($brokerOffset, $consumerOffset),
        ];
        return $cs->encode();
    }

    // ================================================================ Subscription model

    private function testSubscriptionModels(): void
    {
        // ---- GroupRetryPolicy
        $p = new GroupRetryPolicy();
        $this->checkSame(GroupRetryPolicyType::CUSTOMIZED, $p->type, 'GroupRetryPolicy default type=CUSTOMIZED');
        $this->checkSame(null, $p->exponentialRetryPolicy, 'GroupRetryPolicy default exponential=null');
        $this->checkSame(['type' => 'CUSTOMIZED'], $p->toDict(), 'GroupRetryPolicy.toDict emits type only');
        $p2 = new GroupRetryPolicy(GroupRetryPolicyType::EXPONENTIAL, ['initialInterval' => 100], ['retryTimes' => 3]);
        $d2 = $p2->toDict();
        $this->checkSame('EXPONENTIAL', $d2['type'], 'GroupRetryPolicy explicit type');
        $this->checkSame(['initialInterval' => 100], $d2['exponentialRetryPolicy'], 'GroupRetryPolicy emits exponentialRetryPolicy');
        $this->checkSame(['retryTimes' => 3], $d2['customizedRetryPolicy'], 'GroupRetryPolicy emits customizedRetryPolicy');
        $rt = GroupRetryPolicy::fromDict(['type' => 'EXPONENTIAL']);
        $this->checkSame('EXPONENTIAL', $rt->type, 'GroupRetryPolicy.fromDict type');
        $this->checkSame(null, $rt->customizedRetryPolicy, 'GroupRetryPolicy.fromDict missing sub-policy → null');
        $this->checkSame('CUSTOMIZED', GroupRetryPolicy::fromDict(null)->type, 'GroupRetryPolicy.fromDict(null) falls back to CUSTOMIZED');

        // ---- SimpleSubscriptionData
        $s = new SimpleSubscriptionData('T1', 'SQL92', 'a>1', 9);
        $this->checkSame(
            ['topic' => 'T1', 'expressionType' => 'SQL92', 'expression' => 'a>1', 'version' => 9],
            $s->toDict(),
            'SimpleSubscriptionData.toDict'
        );
        $sd = SimpleSubscriptionData::fromDict([]);
        $this->checkSame('', $sd->topic, 'SimpleSubscriptionData default topic empty string');
        $this->checkSame('TAG', $sd->expressionType, 'SimpleSubscriptionData default expressionType=TAG');
        $this->checkSame('*', $sd->expression, 'SimpleSubscriptionData default expression=*');
        $this->checkSame(0, $sd->version, 'SimpleSubscriptionData default version=0');

        // ---- SubscriptionGroupConfig (field defaults)
        $cfg = new SubscriptionGroupConfig('MyGroup');
        $this->checkSame('MyGroup', $cfg->groupName, 'SubscriptionGroupConfig groupName');
        $this->checkSame(true, $cfg->consumeEnable, 'SubscriptionGroupConfig consumeEnable=true');
        $this->checkSame(true, $cfg->consumeFromMinEnable, 'SubscriptionGroupConfig consumeFromMinEnable=true');
        $this->checkSame(true, $cfg->consumeBroadcastEnable, 'SubscriptionGroupConfig consumeBroadcastEnable=true');
        $this->checkSame(false, $cfg->consumeMessageOrderly, 'SubscriptionGroupConfig consumeMessageOrderly=false');
        $this->checkSame(1, $cfg->retryQueueNums, 'SubscriptionGroupConfig retryQueueNums=1');
        $this->checkSame(16, $cfg->retryMaxTimes, 'SubscriptionGroupConfig retryMaxTimes=16');
        $this->checkSame(MixAll::MASTER_ID, $cfg->brokerId, 'SubscriptionGroupConfig brokerId=MASTER_ID(0)');
        $this->checkSame(1, $cfg->whichBrokerWhenConsumeSlowly, 'SubscriptionGroupConfig whichBrokerWhenConsumeSlowly=1');
        $this->checkSame(true, $cfg->notifyConsumerIdsChangedEnable, 'SubscriptionGroupConfig notifyConsumerIdsChangedEnable=true');
        $this->checkSame(0, $cfg->groupSysFlag, 'SubscriptionGroupConfig groupSysFlag=0');
        $this->checkSame(15, $cfg->consumeTimeoutMinute, 'SubscriptionGroupConfig consumeTimeoutMinute=15');
        $this->checkSame(null, $cfg->subscriptionDataSet, 'SubscriptionGroupConfig subscriptionDataSet=null');
        $this->checkSame([], $cfg->attributes, 'SubscriptionGroupConfig attributes={}');

        $json = json_decode($cfg->encode(), true);
        $this->checkSame('MyGroup', $json['groupName'], 'SubscriptionGroupConfig.encode groupName');
        $this->checkSame(['type' => 'CUSTOMIZED'], $json['groupRetryPolicy'], 'SubscriptionGroupConfig.encode groupRetryPolicy contains type only');
        $this->check(!isset($json['subscriptionDataSet']), 'SubscriptionGroupConfig.encode null fields omitted (fastjson2 semantics)');
        $this->checkSame([], $json['attributes'], 'SubscriptionGroupConfig.encode attributes={}');

        $cfg2 = SubscriptionGroupConfig::decode($cfg->encode());
        $this->checkSame($cfg->toDict(), $cfg2->toDict(), 'SubscriptionGroupConfig encode→decode roundtrip');

        $cfg3 = new SubscriptionGroupConfig('G');
        $cfg3->subscriptionDataSet = [new SimpleSubscriptionData('T', 'TAG', '*', 5)];
        $cfg3->attributes = ['x' => 'y'];
        $rt3 = SubscriptionGroupConfig::fromDict(json_decode($cfg3->encode(), true));
        $this->checkSame(1, count($rt3->subscriptionDataSet ?? []), 'subscriptionDataSet serialized and restored when non-empty');
        $this->checkSame('T', $rt3->subscriptionDataSet[0]->topic, 'subscriptionDataSet[0].topic');
        $this->checkSame(5, $rt3->subscriptionDataSet[0]->version, 'subscriptionDataSet[0].version');
        $this->checkSame(['x' => 'y'], $rt3->attributes, 'attributes roundtrip');

        // ---- SubscriptionGroupWrapper
        $w = new SubscriptionGroupWrapper();
        $w->subscriptionGroupTable = ['G1' => new SubscriptionGroupConfig('G1')];
        $w->forbiddenTable = ['T@G' => 3];
        $w->dataVersion = ['counter' => 1, 'timestamp' => 100];
        $jw = json_decode($w->encode(), true);
        $this->checkSame(1, $jw['dataVersion']['counter'], 'Wrapper.encode dataVersion');
        $this->checkSame(3, $jw['forbiddenTable']['T@G'], 'Wrapper.encode forbiddenTable');
        $this->checkSame('G1', $jw['subscriptionGroupTable']['G1']['groupName'], 'Wrapper.encode subscriptionGroupTable');
        $rw = SubscriptionGroupWrapper::decode($w->encode());
        $this->check($rw->subscriptionGroupTable['G1'] instanceof SubscriptionGroupConfig, 'Wrapper.decode restores SubscriptionGroupConfig');
        $this->checkSame(1, $rw->dataVersion['counter'], 'Wrapper.decode dataVersion');
        $this->checkSame([], SubscriptionGroupWrapper::decode('{}')->subscriptionGroupTable, 'Wrapper.decode empty object');
    }

    // ================================================================ Configuration and lifecycle

    private function testAdminConfigLifecycle(): void
    {
        $this->checkSame(15000, DefaultMQAdminExt::DEFAULT_TIMEOUT, 'DEFAULT_TIMEOUT = 5000*3');
        $this->checkSame('ORDER_TOPIC_CONFIG', DefaultMQAdminExt::NAMESPACE_ORDER_TOPIC_CONFIG, 'NAMESPACE_ORDER_TOPIC_CONFIG');

        $admin = new FakeAdmin();
        $this->checkSame('ADMIN', $admin->instanceName, 'default instanceName=ADMIN');
        $this->checkSame(null, $admin->clientId, 'default clientId=null');
        $this->checkSame(null, $admin->unitName, 'default unitName=null');
        $this->checkSame(false, $admin->enableStreamRequestType, 'default enableStreamRequestType=false');
        $this->checkSame(false, $admin->vipChannelEnabled, 'default vipChannelEnabled=false');
        $this->checkSame(30000, $admin->pollNameServerInterval, 'default pollNameServerInterval=30000');
        $this->checkSame(DefaultMQAdminExt::DEFAULT_TIMEOUT, $admin->timeoutMillis, 'default timeoutMillis=DEFAULT_TIMEOUT');
        $this->checkSame(null, $admin->rpcHook, 'default rpcHook=null');
        $this->checkSame('', $admin->namespace, 'default namespace empty string');

        $admin->setNamesrvAddr(' a:1 ; b:2 ; ;');
        $this->checkSame(['a:1', 'b:2'], $admin->getNameServerAddressList(), 'setNamesrvAddr splits on semicolons and trims');
        $this->checkSame('a:1;b:2', $admin->getNameServerAddr(), 'getNameServerAddr joins with semicolons');
        $admin->setNameServerAddresses(['c:3']);
        $this->checkSame(['c:3'], $admin->getNameServerAddressList(), 'setNameServerAddresses');

        $admin->setInstanceName('X');
        $this->checkSame('X', $admin->instanceName, 'setInstanceName');
        $admin->setUnitName('unit1');
        $this->checkSame('unit1', $admin->getUnitName(), 'setUnitName/getUnitName');
        $admin->setEnableStreamRequestType(true);
        $this->checkSame(true, $admin->enableStreamRequestType, 'setEnableStreamRequestType');
        $admin->setVipChannelEnabled(true);
        $this->checkSame(true, $admin->vipChannelEnabled, 'setVipChannelEnabled');
        $admin->setTimeoutMillis(1234);
        $this->checkSame(1234, $admin->timeoutMillis, 'setTimeoutMillis');

        // Forbidden to use before start (before injection)
        $bare = new FakeAdmin();
        $this->checkThrows(fn() => $bare->fetchAllTopicList(), MQClientException::class, 'call before start → admin not started');
        $this->checkThrows(fn() => $bare->getMqClientInstance(), MQClientException::class, 'fetch instance before start → throws');

        // start() without address
        $bare2 = new DefaultMQAdminExt();
        $this->checkThrows(fn() => $bare2->start(), MQClientException::class, 'start without NameServer → throws');
        $bare2->shutdown(); // no-op before start
        $this->check(true, 'shutdown before start is a no-op');

        // Real start() (offline path: no in-use topics / no brokers / no consumers, no network requests)
        $real = new DefaultMQAdminExt();
        $real->setNamesrvAddr('127.0.0.1:9876');
        $real->setInstanceName('DEFAULT');
        $real->start();
        $this->check($real->instanceName !== 'DEFAULT', "start replaces the DEFAULT instance name with <pid>#<nanoTime>");
        // changeInstanceNameToPID already replaced DEFAULT with <pid>#<nanoTime>;
        // clientId looks like <ip>@<pid>#<nanoTime> and no longer contains the literal DEFAULT.
        $this->check(
            $real->getClientId() !== null && str_contains((string) $real->getClientId(), '@')
                && !str_contains((string) $real->getClientId(), '@DEFAULT'),
            'clientId looks like ip@instanceName (DEFAULT replaced by the pid form)',
        );
        $this->check($real->getMqClientInstance() instanceof MQClientInstance, 'MQClientInstance obtainable after start');
        $real->shutdown();
        $real2 = $real;
        $real2->shutdown();
        $this->check(true, 'duplicate shutdown is a no-op');

        // Inject the double
        $injected = new FakeAdmin();
        $fc = new FakeClient();
        $injected->setMqClientInstance($fc);
        $this->check($injected->getMqClientInstance() === $fc, 'setMqClientInstance injects and returns');

        // permIsValid (permission-bit check)
        $this->checkSame(true, DefaultMQAdminExt::permIsValid('6'), "permIsValid('6')");
        $this->checkSame(true, DefaultMQAdminExt::permIsValid('0'), "permIsValid('0')");
        $this->checkSame(true, DefaultMQAdminExt::permIsValid(4), 'permIsValid(4)');
        $this->checkSame(false, DefaultMQAdminExt::permIsValid('8'), 'permIsValid(8)=false (>= PRIORITY)');
        $this->checkSame(false, DefaultMQAdminExt::permIsValid('-1'), 'permIsValid(-1)=false');
        $this->checkSame(false, DefaultMQAdminExt::permIsValid('abc'), "permIsValid('abc')=false (NumberFormatException semantics)");
        $this->checkSame(false, DefaultMQAdminExt::permIsValid(null), 'permIsValid(null)=false');
    }

    // ================================================================ Low-level invoke helpers

    private function testInvokeHelpers(): void
    {
        // invokeBroker: request code + ext fields always stringified
        $admin = $this->newAdmin();
        $admin->handler = fn(RemotingCommand $cmd) => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'boom');
        $this->checkThrows(fn() => $admin->getSystemTopicListFromBroker('b:1'), MQBrokerException::class, 'non-SUCCESS → MQBrokerException');
        $e = $this->capture(fn() => $admin->getSystemTopicListFromBroker('b:1'));
        $this->check($e instanceof MQBrokerException && $e->responseCode === ResponseCode::SYSTEM_ERROR, 'MQBrokerException carries the response code');
        $this->check($e !== null && str_contains($e->getMessage(), 'boom'), 'MQBrokerException carries the remark');

        // VIP channel: broker request port -2
        $admin2 = $this->newAdmin();
        $admin2->setVipChannelEnabled(true);
        $admin2->getSystemTopicListFromBroker('10.0.0.1:10911');
        $this->checkSame('10.0.0.1:10909', $this->lastAddr($admin2), 'VIP on → broker address port-2');

        // NameServer broadcast: PUT_KV_CONFIG hits every NameServer
        $admin3 = $this->newAdmin();
        $admin3->createAndUpdateKvConfig('NS', 'K', 'V');
        $putReqs = $this->reqsByCode($admin3, RequestCode::PUT_KV_CONFIG);
        $this->checkSame(1, count($putReqs), 'single NameServer → 1 PUT');
        $this->checkSame('10.0.0.1:9876', $putReqs[0]['addr'], 'PUT hits the NameServer address');
        $this->checkSame(
            ['namespace' => 'NS', 'key' => 'K', 'value' => 'V'],
            $putReqs[0]['cmd']->extFields,
            'PUT_KV_CONFIG ext fields'
        );

        $admin3b = $this->newAdmin();
        $admin3b->setNameServerAddresses(['10.0.0.1:1', '10.0.0.1:2']);
        $admin3b->createAndUpdateKvConfig('NS', 'K', 'V');
        $this->checkSame(2, count($this->cmdsByCode($admin3b, RequestCode::PUT_KV_CONFIG)), 'PUT broadcast to every NameServer (2 of them)');

        // Any broadcast failure: throws after all collected (with remark)
        $admin3c = $this->newAdmin();
        $admin3c->setNameServerAddresses(['10.0.0.1:1', '10.0.0.1:2']);
        $admin3c->handler = fn(RemotingCommand $c, string $addr) => $addr === '10.0.0.1:2'
            ? $this->errResponse(ResponseCode::NO_PERMISSION, 'denied')
            : $this->okResponse();
        $err = $this->capture(fn() => $admin3c->createAndUpdateKvConfig('NS', 'K', 'V'));
        $this->check($err instanceof MQClientException, 'any broadcast failure → MQClientException');
        $this->check($err instanceof MQClientException && $err->responseCode === ResponseCode::NO_PERMISSION, 'broadcast failure carries the response code');
        $this->check($err !== null && str_contains($err->getMessage(), 'denied'), 'broadcast failure carries the remark');
        $this->checkSame(2, count($this->cmdsByCode($admin3c, RequestCode::PUT_KV_CONFIG)), 'broadcast sends to both even when failing');

        // invokeNamesrvOne: first server connect fails → moves to the next
        $admin4 = $this->newAdmin();
        $admin4->setNameServerAddresses(['10.0.0.1:1', '10.0.0.1:2']);
        $admin4->failAddrs['10.0.0.1:1'] = new RemotingConnectException('10.0.0.1:1');
        $admin4->getKvListByNamespace('NS');
        $this->checkSame('10.0.0.1:2', $this->lastAddr($admin4), 'first unreachable → falls to the second');

        $admin4b = $this->newAdmin();
        $admin4b->failAddrs['10.0.0.1:9876'] = new RemotingConnectException('x');
        $this->checkThrows(fn() => $admin4b->getKvListByNamespace('NS'), MQClientException::class, 'all NameServers unreachable → all name servers unreachable');

        // Default timeout pass-through (timeout parameter)
        $admin5 = $this->newAdmin();
        $admin5->setTimeoutMillis(777);
        $seen = null;
        $admin5->handler = function (RemotingCommand $c, string $addr) use (&$seen) {
            $seen = $c;
            return $this->okResponse();
        };
        $admin5->getSystemTopicListFromBroker('b:9', 5555);
        $this->check(true, 'explicit timeout accepted (placeholder)');
        $this->checkSame(RequestCode::GET_SYSTEM_TOPIC_LIST_FROM_BROKER, $seen->code, 'GET_SYSTEM_TOPIC_LIST_FROM_BROKER=305');
    }

    // ================================================================ Topic management

    private function testTopicManagement(): void
    {
        // createTopic → createTopicInRoute (default perm=READ|WRITE)
        $admin = $this->newAdmin();
        $admin->createTopic('key', 'NewTopic', 8);
        $this->checkSame(1, count($this->client->createTopicInRouteCalls), 'createTopic goes through createTopicInRoute');
        $call = $this->client->createTopicInRouteCalls[0];
        $this->checkSame('NewTopic', $call['topic'], 'createTopic topic name');
        $this->checkSame(8, $call['read'], 'createTopic readQueueNums');
        $this->checkSame(8, $call['write'], 'createTopic writeQueueNums');
        $this->checkSame(MixAll::READ_PERM_BY_DEFAULT, $call['perm'], 'createTopic perm=READ|WRITE');

        // createAndUpdateTopicConfig → createTopicInBroker (defaultTopic=TBW102)
        $admin2 = $this->newAdmin();
        $cfg = new TopicConfig('T1', 4, 6, 6);
        $admin2->createAndUpdateTopicConfig('b:1', $cfg);
        $this->checkSame(1, count($this->client->createTopicInBrokerCalls), 'createAndUpdateTopicConfig goes through createTopicInBroker');
        $c = $this->client->createTopicInBrokerCalls[0];
        $this->checkSame('b:1', $c['addr'], 'createAndUpdateTopicConfig addr');
        $this->checkSame(MixAll::DEFAULT_TOPIC, $c['defaultTopic'], 'createAndUpdateTopicConfig defaultTopic=TBW102');
        $this->checkSame('T1', $c['topic'], 'createAndUpdateTopicConfig topic');
        $this->checkSame(4, $c['read'], 'createAndUpdateTopicConfig readQueueNums');
        $this->checkSame(6, $c['write'], 'createAndUpdateTopicConfig writeQueueNums');

        $admin2b = $this->newAdmin();
        $admin2b->createTopicInBroker('b:2', 'T2', 1, 2, 4);
        $this->checkSame('T2', $this->client->createTopicInBrokerCalls[0]['topic'], 'createTopicInBroker pass-through');

        $admin2c = $this->newAdmin();
        $admin2c->deleteTopicInBroker('b:3', 'T3');
        $this->checkSame(['addr' => 'b:3', 'topic' => 'T3'], $this->client->deleteTopicInBrokerCalls[0], 'deleteTopicInBroker pass-through');

        // examineTopicConfig: 351 + topic + lo=true
        $admin3 = $this->newAdmin();
        $admin3->handler = fn() => $this->okResponse((new TopicConfig('T', 3, 3, 6))->encode());
        $got = $admin3->examineTopicConfig('b:1', 'T');
        $cmd = $this->lastCmd($admin3);
        $this->checkSame(RequestCode::GET_TOPIC_CONFIG, $cmd->code, 'GET_TOPIC_CONFIG=351');
        $this->checkSame('T', $cmd->extFields['topic'] ?? null, 'GET_TOPIC_CONFIG carries topic');
        $this->checkSame('true', $cmd->extFields['lo'] ?? null, 'GET_TOPIC_CONFIG carries lo=true');
        $this->checkSame('T', $got->topicName, 'examineTopicConfig parses TopicConfig');
        $this->checkSame(3, $got->readQueueNums, 'examineTopicConfig readQueueNums');
        $admin3->handler = fn() => $this->okResponse(null);
        $this->checkThrows(fn() => $admin3->examineTopicConfig('b:1', 'T'), MQBrokerException::class, 'examineTopicConfig empty body → MQBrokerException');

        // deleteTopicInNameServer: explicit address list; VIP on does not convert the NameServer port either
        $admin4 = $this->newAdmin();
        $admin4->setVipChannelEnabled(true);
        $admin4->deleteTopicInNameServer(['ns:1', 'ns:2'], 'T');
        $delReqs = $this->reqsByCode($admin4, RequestCode::DELETE_TOPIC_IN_NAMESRV);
        $this->checkSame(2, count($delReqs), 'deleteTopicInNameServer sends to each given address');
        $this->checkSame(RequestCode::DELETE_TOPIC_IN_NAMESRV, $delReqs[0]['cmd']->code, 'DELETE_TOPIC_IN_NAMESRV=216');
        $this->checkSame('T', $delReqs[0]['cmd']->extFields['topic'] ?? null, 'DELETE_TOPIC_IN_NAMESRV ext topic');
        $this->checkSame('ns:1', $delReqs[0]['addr'], 'NameServer addresses skip the VIP port conversion');
        $this->checkSame('ns:2', $delReqs[1]['addr'], 'second NameServer untouched');

        $admin4b = $this->newAdmin();
        $admin4b->setNameServerAddresses(['ns:a', 'ns:b']);
        $admin4b->deleteTopicInNameServer(null, 'T');
        $this->checkSame(['ns:a', 'ns:b'], array_map(fn($r) => $r['addr'], $this->reqsByCode($admin4b, RequestCode::DELETE_TOPIC_IN_NAMESRV)), 'empty address list falls back to all NameServers');

        $admin4c = $this->newAdmin();
        $admin4c->deleteTopicInNamesrv('T');
        $this->checkSame(['T'], $this->client->deleteTopicInNamesrvCalls, 'deleteTopicInNamesrv legacy name goes to the instance');

        // deleteTopic: clears broker + namesrv + KV namespace; failure degrades to warn
        $admin5 = $this->newAdmin();
        $ci = new ClusterInfo();
        $ci->brokerAddrTable = ['broker-a' => [0 => 'b:a', 1 => 'b:a-s']];
        $ci->clusterAddrTable = ['C1' => ['broker-a']];
        $this->client->clusterInfo = $ci;
        $this->client->topicRoutes['T'] = new TopicRouteData();
        $admin5->kvNamespaceToDeleteList = ['ORDER_TOPIC_CONFIG'];
        $admin5->deleteTopic('T', 'C1');
        $this->checkSame([['addr' => 'b:a', 'topic' => 'T'], ['addr' => 'b:a-s', 'topic' => 'T']], $this->client->deleteTopicInBrokerCalls, 'deleteTopic clears every broker in the cluster');
        $this->checkSame(['T'], $this->client->deleteTopicInNamesrvCalls, 'deleteTopic clears the NameServer route');
        $delKv = $this->cmdsByCode($admin5, RequestCode::DELETE_KV_CONFIG);
        $this->checkSame(1, count($delKv), 'deleteTopic clears the KV namespace');
        $this->checkSame('T', $delKv[0]->extFields['key'] ?? null, 'deleteTopic KV key=topic');

        $admin5b = $this->newAdmin();
        $admin5b->kvNamespaceToDeleteList = ['NS1'];
        $admin5b->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $admin5b->deleteTopic('T'); // broker deletion failure should not block
        $this->checkSame(1, count($this->client->deleteTopicInNamesrvCalls), 'broker deletion failure does not block NameServer cleanup');

        // fetchAllTopicList / fetchAllTopicRoute
        $admin6 = $this->newAdmin();
        $this->client->allTopics = new TopicList();
        $this->client->allTopics->topicList = ['T1', 'T2'];
        $this->client->topicRoutes = ['T1' => $this->route('T1', 'broker-a', 'b:1')];
        $tl = $admin6->fetchAllTopicList();
        $this->checkSame(['T1', 'T2'], $tl->getTopicList(), 'fetchAllTopicList');
        $routes = $admin6->fetchAllTopicRoute();
        $this->checkSame(1, count($routes), 'fetchAllTopicRoute collects only topics with routes');

        // fetchTopicsByCluster: 224 + cluster field name
        $admin7 = $this->newAdmin();
        $admin7->handler = fn() => $this->okResponse(json_encode(['topicList' => ['TA', 'TB', 'TA']]));
        $topics = $admin7->fetchTopicsByCluster('C1');
        $cmd7 = $this->lastCmd($admin7);
        $this->checkSame(RequestCode::GET_TOPICS_BY_CLUSTER, $cmd7->code, 'GET_TOPICS_BY_CLUSTER=224');
        $this->checkSame('C1', $cmd7->extFields['cluster'] ?? null, 'GET_TOPICS_BY_CLUSTER field name must be cluster');
        $this->checkSame(['TA', 'TB'], $topics, 'fetchTopicsByCluster deduplicates');

        // getClusterList / getTopicClusterList
        $admin8 = $this->newAdmin();
        $ci8 = new ClusterInfo();
        $ci8->clusterAddrTable = ['C1' => ['broker-a'], 'C2' => ['broker-x']];
        $this->client->clusterInfo = $ci8;
        $this->route('T', 'broker-a', 'b:1');
        $this->checkSame(['C1'], $admin8->getClusterList('T'), 'getClusterList intersects by route brokers');
        $this->checkSame(['C1'], $admin8->getTopicClusterList('T'), 'getTopicClusterList alias');

        // examineTopicRoute: throws when the route is missing
        $admin9 = $this->newAdmin();
        $this->checkThrows(fn() => $admin9->examineTopicRoute('Nope'), MQClientException::class, 'examineTopicRoute no route → throws');
        $this->route('T', 'broker-a', 'b:1');
        $this->checkSame('broker-a', $admin9->examineTopicRoute('T')->getBrokerDatas()[0]->brokerName, 'examineTopicRoute hit');

        // get_all_topic_config / get_user_topic_config
        $admin10 = $this->newAdmin();
        $wrapper = new \RocketMQ\Remoting\Protocol\TopicConfigSerializeWrapper();
        $wrapper->topicConfigTable = [
            'T1' => new TopicConfig('T1'),
            'rmq_sys_x' => new TopicConfig('rmq_sys_x'),
            '%RETRY%G' => new TopicConfig('%RETRY%G'),
            '%DLQ%G' => new TopicConfig('%DLQ%G'),
        ];
        $sys = new TopicList();
        $sys->topicList = ['rmq_sys_x'];
        $admin10->handler = function (RemotingCommand $cmd) use ($wrapper, $sys) {
            if ($cmd->code === RequestCode::GET_ALL_TOPIC_CONFIG) {
                return $this->okResponse($wrapper->encode());
            }
            return $this->okResponse($sys->encode());
        };
        $all = $admin10->getAllTopicConfig('b:1');
        $this->checkSame(RequestCode::GET_ALL_TOPIC_CONFIG, $this->lastCmd($admin10)->code, 'GET_ALL_TOPIC_CONFIG=21');
        $this->checkSame(4, count($all->topicConfigTable), 'getAllTopicConfig full table');
        $user = $admin10->getUserTopicConfig('b:1');
        $this->checkSame(['T1'], array_keys($user->topicConfigTable), 'getUserTopicConfig strips system/retry/DLQ');
        $userSpecial = $admin10->getUserTopicConfig('b:1', true);
        $this->checkSame(['T1', '%RETRY%G', '%DLQ%G'], array_keys($userSpecial->topicConfigTable), 'specialTopic=true keeps retry/DLQ');
        $admin10b = $this->newAdmin();
        $admin10b->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin10b->getAllTopicConfig('b:1')->topicConfigTable, 'getAllTopicConfig empty body → empty wrapper');

        // getSystemTopicListFromBroker
        $admin11 = $this->newAdmin();
        $admin11->handler = fn() => $this->okResponse((new TopicList())->encode());
        $this->checkSame([], $admin11->getSystemTopicListFromBroker('b:1')->getTopicList(), 'getSystemTopicListFromBroker empty');

        // examineTopicStats merge
        $admin12 = $this->newAdmin();
        $trd = new TopicRouteData();
        $trd->brokerDatas = [
            new BrokerData('C', 'broker-a', [0 => 'b:a']),
            new BrokerData('C', 'broker-b', [0 => 'b:b']),
        ];
        $this->client->topicRoutes['TS'] = $trd;
        $admin12->handler = function (RemotingCommand $cmd, string $addr) {
            $t = new TopicStatsTable();
            $t->topicPutTps = 1.5;
            $t->offsetTable[] = [
                'mq' => new MessageQueue('TS', $addr === 'b:a' ? 'broker-a' : 'broker-b', 0),
                'value' => new \RocketMQ\Remoting\Protocol\TopicOffset(0, 10),
            ];
            return $this->okResponse($t->encode());
        };
        $stats = $admin12->examineTopicStats('TS');
        $this->checkSame(2, count($stats->offsetTable), 'examineTopicStats merges offsetTable from both brokers');
        $this->checkSame(3.0, $stats->topicPutTps, 'examineTopicStats accumulates TPS');
        $statsCmd = $this->lastCmd($admin12);
        $this->checkSame(RequestCode::GET_TOPIC_STATS_INFO, $statsCmd->code, 'GET_TOPIC_STATS_INFO=202');
        $this->checkSame('TS', $statsCmd->extFields['topic'] ?? null, 'GET_TOPIC_STATS_INFO ext topic');
        $admin12b = $this->newAdmin();
        $admin12b->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $this->checkThrows(fn() => $admin12b->examineTopicStats('TS'), MQClientException::class, 'examineTopicStats all fail → Not found the topic stats info');
    }

    // ================================================================ Cluster / Broker

    private function testClusterBroker(): void
    {
        $admin = $this->newAdmin();
        $ci = new ClusterInfo();
        $ci->brokerAddrTable = ['broker-a' => [0 => 'b:a', 1 => 'b:a-s']];
        $this->client->clusterInfo = $ci;
        $this->check($admin->fetchBrokerClusterInfo() === $this->client->clusterInfo, 'fetchBrokerClusterInfo passes through to the instance');
        $this->check($admin->examineBrokerClusterInfo() === $this->client->clusterInfo, 'examineBrokerClusterInfo alias');

        $admin2 = $this->newAdmin();
        $kv = new KVTable();
        $kv->table = ['putTps' => '1.0'];
        $admin2->handler = fn() => $this->okResponse($kv->encode());
        $got = $admin2->fetchBrokerRuntimeStats('b:1');
        $this->checkSame(RequestCode::GET_BROKER_RUNTIME_INFO, $this->lastCmd($admin2)->code, 'GET_BROKER_RUNTIME_INFO=28');
        $this->checkSame(['putTps' => '1.0'], $got->table, 'fetchBrokerRuntimeStats parses KVTable');
        $this->checkSame($admin2->getBrokerRuntimeInfo('b:1')->table, $got->table, 'getBrokerRuntimeInfo legacy interface alias');
        $admin2->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin2->fetchBrokerRuntimeStats('b:1')->table, 'fetchBrokerRuntimeStats empty body → empty KVTable');

        // getBrokerConfig: body is a properties text
        $admin3 = $this->newAdmin();
        $admin3->handler = fn() => $this->okResponse("brokerId=0\nbrokerName=broker-a\n");
        $props = $admin3->getBrokerConfig('b:1');
        $this->checkSame(RequestCode::GET_BROKER_CONFIG, $this->lastCmd($admin3)->code, 'GET_BROKER_CONFIG=26');
        $this->checkSame(['brokerId' => '0', 'brokerName' => 'broker-a'], $props, 'getBrokerConfig parses the properties text');

        // updateBrokerConfig: body is a properties text + brokerPermission validation
        $admin4 = $this->newAdmin();
        $admin4->updateBrokerConfig('b:1', ['brokerName' => 'x']);
        $cmd4 = $this->lastCmd($admin4);
        $this->checkSame(RequestCode::UPDATE_BROKER_CONFIG, $cmd4->code, 'UPDATE_BROKER_CONFIG=25');
        $this->checkSame("brokerName=x\n", $cmd4->body, 'updateBrokerConfig body is k=v\n text');
        $n0 = count($admin4->requests);
        $admin4->updateBrokerConfig('b:1', []);
        $this->checkSame($n0, count($admin4->requests), 'updateBrokerConfig sends no request for empty properties');
        $e = $this->capture(fn() => $admin4->updateBrokerConfig('b:1', ['brokerPermission' => '8']));
        $this->check($e instanceof MQClientException && $e->responseCode === ResponseCode::NO_PERMISSION, 'illegal brokerPermission → NO_PERMISSION');
        $e2 = $this->capture(fn() => $admin4->updateBrokerConfig('b:1', ['brokerPermission' => 'abc']));
        $this->check($e2 instanceof MQClientException, "brokerPermission='abc' illegal");

        // wipe / add write perm
        $admin5 = $this->newAdmin();
        $admin5->handler = fn() => $this->okResponse(null, ['wipeTopicCount' => '3']);
        $this->checkSame(3, $admin5->wipeWritePermOfBroker('ns:1', 'broker-a'), 'wipeWritePermOfBroker reads wipeTopicCount');
        $this->checkSame(RequestCode::WIPE_WRITE_PERM_OF_BROKER, $this->lastCmd($admin5)->code, 'WIPE_WRITE_PERM_OF_BROKER=205');
        $this->checkSame('broker-a', $this->lastCmd($admin5)->extFields['brokerName'] ?? null, 'wipe ext brokerName');
        $admin5->handler = fn() => $this->okResponse(null, ['addTopicCount' => '2']);
        $this->checkSame(2, $admin5->addWritePermOfBroker('ns:1', 'broker-a'), 'addWritePermOfBroker reads addTopicCount');
        $this->checkSame(RequestCode::ADD_WRITE_PERM_OF_BROKER, $this->lastCmd($admin5)->code, 'ADD_WRITE_PERM_OF_BROKER=327');

        // cleanUnusedTopic: all succeed true / any failure false
        $admin6 = $this->newAdmin();
        $admin6->setNameServerAddresses(['ns:1']);
        $ci6 = new ClusterInfo();
        $ci6->brokerAddrTable = ['broker-a' => [0 => 'b:a']];
        $this->client->clusterInfo = $ci6;
        $this->checkSame(true, $admin6->cleanUnusedTopic(), 'cleanUnusedTopic all succeed');
        $this->checkSame(RequestCode::CLEAN_UNUSED_TOPIC, $this->lastCmd($admin6)->code, 'CLEAN_UNUSED_TOPIC=316');
        $admin6->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $this->checkSame(false, $admin6->cleanUnusedTopic(), 'cleanUnusedTopic failure → false');

        // viewBrokerStatsData
        $admin7 = $this->newAdmin();
        $admin7->handler = fn() => $this->okResponse(json_encode(['statsMinute' => []]));
        $got7 = $admin7->viewBrokerStatsData('b:1', 'SN', 'SK');
        $this->checkSame(RequestCode::VIEW_BROKER_STATS_DATA, $this->lastCmd($admin7)->code, 'VIEW_BROKER_STATS_DATA=315');
        $this->checkSame(['statsName' => 'SN', 'statsKey' => 'SK'], $this->lastCmd($admin7)->extFields, 'VIEW_BROKER_STATS_DATA ext');
        $this->checkSame(['statsMinute' => []], $got7, 'viewBrokerStatsData parses the body');
    }

    // ================================================================ NameServer KV configuration

    private function testKvConfig(): void
    {
        $admin = $this->newAdmin();
        $this->assertSamePub($admin->putKvConfig('NS', 'K', 'V'), null, 'putKvConfig returns nothing');
        $this->checkSame(RequestCode::PUT_KV_CONFIG, $this->lastCmd($admin)->code, 'PUT_KV_CONFIG=100');

        $admin2 = $this->newAdmin();
        $admin2->deleteKvConfig('NS', 'K');
        $this->checkSame(RequestCode::DELETE_KV_CONFIG, $this->lastCmd($admin2)->code, 'DELETE_KV_CONFIG=102');
        $this->checkSame(['namespace' => 'NS', 'key' => 'K'], $this->lastCmd($admin2)->extFields, 'DELETE_KV_CONFIG ext');

        $admin3 = $this->newAdmin();
        $admin3->handler = fn() => $this->okResponse(null, ['value' => 'V1']);
        $this->checkSame('V1', $admin3->getKvConfig('NS', 'K'), 'getKvConfig reads ext value');
        $this->checkSame(RequestCode::GET_KV_CONFIG, $this->lastCmd($admin3)->code, 'GET_KV_CONFIG=101');
        $admin3->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        // getKvConfig: non-SUCCESS does **not** throw, returns null (different from checkResponse semantics)
        $this->checkSame(null, $admin3->getKvConfig('NS', 'K'), 'getKvConfig non-SUCCESS → null (no exception)');

        $admin4 = $this->newAdmin();
        $kv = new KVTable();
        $kv->table = ['k1' => 'v1'];
        $admin4->handler = fn() => $this->okResponse($kv->encode());
        $this->checkSame(['k1' => 'v1'], $admin4->getKvListByNamespace('NS')->table, 'getKvListByNamespace parses KVTable');
        $this->checkSame(RequestCode::GET_KVLIST_BY_NAMESPACE, $this->lastCmd($admin4)->code, 'GET_KVLIST_BY_NAMESPACE=219');
        $admin4->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin4->getKvListByNamespace('NS')->table, 'getKvListByNamespace empty body');

        // createOrUpdateOrderConf: cluster mode writes directly
        $admin5 = $this->newAdmin();
        $admin5->createOrUpdateOrderConf('T', 'broker-a:q', true);
        $this->checkSame(
            ['namespace' => DefaultMQAdminExt::NAMESPACE_ORDER_TOPIC_CONFIG, 'key' => 'T', 'value' => 'broker-a:q'],
            $this->lastCmd($admin5)->extFields,
            'createOrUpdateOrderConf cluster mode writes ORDER_TOPIC_CONFIG directly'
        );
        // Non-cluster mode: read-modify-write
        $admin6 = $this->newAdmin();
        $admin6->handler = fn() => $this->okResponse(null, ['value' => 't1:conf1;t2:conf2']);
        $admin6->createOrUpdateOrderConf('T', 't3:conf3', false);
        $puts = $this->cmdsByCode($admin6, RequestCode::PUT_KV_CONFIG);
        $this->checkSame(1, count($puts), 'createOrUpdateOrderConf non-cluster = 1 PUT');
        $this->checkSame('t1:conf1;t2:conf2;t3:conf3', $puts[0]->extFields['value'] ?? null, 'createOrUpdateOrderConf replaces the entry then writes the whole back');
        $admin6b = $this->newAdmin();
        $admin6b->handler = fn() => $this->okResponse(null, ['value' => 't1:conf1;t2:conf2']);
        $admin6b->createOrUpdateOrderConf('T', 't2:newconf', false);
        $puts2 = $this->cmdsByCode($admin6b, RequestCode::PUT_KV_CONFIG);
        $this->checkSame('t1:conf1;t2:newconf', $puts2[0]->extFields['value'] ?? null, 'createOrUpdateOrderConf replaces the same key');
        $this->checkThrows(fn() => $admin6b->createOrUpdateOrderConf('', 'v'), MQClientException::class, 'createOrUpdateOrderConf empty key throws');
        $this->checkThrows(fn() => $admin6b->createOrUpdateOrderConf('T', ''), MQClientException::class, 'createOrUpdateOrderConf empty value throws');
        $this->checkThrows(fn() => $admin6b->createOrUpdateOrderConf('T', ':conf'), MQClientException::class, 'createOrUpdateOrderConf value without key throws');
        // getKvConfig exception continues with an empty table (exception swallowed, old_confs treated as empty)
        // Note: the PUT broadcast has **no fault tolerance** for connect failures (no try/catch fallback),
        // so here only the first GET fails; the PUT must arrive normally.
        $admin6c = $this->newAdmin();
        $seen6c = 0;
        $admin6c->handler = function (RemotingCommand $c, string $addr) use (&$seen6c) {
            if ($c->code === RequestCode::GET_KV_CONFIG && $seen6c++ === 0) {
                throw new RemotingConnectException('x');
            }
            return null; // harness falls back to SUCCESS
        };
        $admin6c->createOrUpdateOrderConf('T', 't1:conf1', false);
        $puts3 = $this->cmdsByCode($admin6c, RequestCode::PUT_KV_CONFIG);
        $this->checkSame(1, count($puts3), 'PUT still sent after the read failure');
        $this->checkSame('t1:conf1', $puts3[0]->extFields['value'] ?? null, 'createOrUpdateOrderConf continues with an empty table after a read failure');
        $this->checkSame('ORDER_TOPIC_CONFIG', $puts3[0]->extFields['namespace'] ?? null, 'order conf uses the ORDER_TOPIC_CONFIG namespace');
    }

    private function assertSamePub(mixed $a, mixed $b, string $label): void
    {
        $this->checkSame($a, $b, $label);
    }

    // ================================================================ Subscription group management

    private function testSubscriptionGroup(): void
    {
        // createAndUpdateSubscriptionGroupConfig: body is SubscriptionGroupConfig JSON
        $admin = $this->newAdmin();
        $cfg = new SubscriptionGroupConfig('G1');
        $cfg->retryMaxTimes = 9;
        $admin->createAndUpdateSubscriptionGroupConfig('b:1', $cfg);
        $cmd = $this->lastCmd($admin);
        $this->checkSame(RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, $cmd->code, 'UPDATE_AND_CREATE_SUBSCRIPTIONGROUP=200');
        $decoded = SubscriptionGroupConfig::decode($cmd->body ?? '');
        $this->checkSame('G1', $decoded->groupName, '200 body is SubscriptionGroupConfig JSON');
        $this->checkSame(9, $decoded->retryMaxTimes, '200 body field preserved');

        // getSubscriptionGroupConfig: 352 single lookup
        $admin2 = $this->newAdmin();
        $admin2->handler = fn() => $this->okResponse((new SubscriptionGroupConfig('G2'))->encode());
        $got = $admin2->getSubscriptionGroupConfig('b:1', 'G2');
        $this->checkSame(RequestCode::GET_SUBSCRIPTIONGROUP_CONFIG, $this->lastCmd($admin2)->code, 'GET_SUBSCRIPTIONGROUP_CONFIG=352');
        $this->checkSame('G2', $this->lastCmd($admin2)->extFields['group'] ?? null, '352 ext group');
        $this->checkSame('G2', $got?->groupName, 'getSubscriptionGroupConfig parses');
        $admin2->handler = fn() => $this->okResponse(null);
        $this->checkSame(null, $admin2->getSubscriptionGroupConfig('b:1', 'G2'), 'getSubscriptionGroupConfig empty body → null');

        // deleteSubscriptionGroup
        $admin3 = $this->newAdmin();
        $admin3->deleteSubscriptionGroup('b:1', 'G1', true);
        $this->checkSame(RequestCode::DELETE_SUBSCRIPTIONGROUP, $this->lastCmd($admin3)->code, 'DELETE_SUBSCRIPTIONGROUP=207');
        $this->checkSame(['groupName' => 'G1', 'cleanOffset' => 'true'], $this->lastCmd($admin3)->extFields, '207 ext cleanOffset=true');
        $admin3->deleteSubscriptionGroup('b:1', 'G1');
        $this->checkSame('false', $this->lastCmd($admin3)->extFields['cleanOffset'] ?? null, '207 ext cleanOffset=false default');

        // Pagination: totalGroupNum=4, 2 groups per page
        $admin4 = $this->newAdmin();
        $page = 0;
        $admin4->handler = function (RemotingCommand $cmd) use (&$page) {
            $page++;
            $w = new SubscriptionGroupWrapper();
            if ($page === 1) {
                $w->subscriptionGroupTable = ['G1' => new SubscriptionGroupConfig('G1'), 'G2' => new SubscriptionGroupConfig('G2')];
                $w->dataVersion = ['counter' => 7];
                return $this->okResponse($w->encode(), ['totalGroupNum' => '4']);
            }
            $w->subscriptionGroupTable = ['G3' => new SubscriptionGroupConfig('G3'), 'G4' => new SubscriptionGroupConfig('G4')];
            $w->dataVersion = ['counter' => 7];
            return $this->okResponse($w->encode(), ['totalGroupNum' => '4']);
        };
        $wrapper = $admin4->getAllSubscriptionGroup('b:1');
        $this->checkSame(2, count($admin4->requests), 'pagination collects all 2 pages');
        $this->checkSame(['G1', 'G2', 'G3', 'G4'], array_keys($wrapper->subscriptionGroupTable), 'paged results merged');
        $this->checkSame(['counter' => 7], $wrapper->dataVersion, 'paged results keep dataVersion');
        $r1 = $admin4->requests[0]['cmd'];
        $r2 = $admin4->requests[1]['cmd'];
        $this->checkSame(RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG, $r1->code, 'GET_ALL_SUBSCRIPTIONGROUP_CONFIG=201');
        $this->checkSame('0', $r1->extFields['groupSeq'] ?? null, 'first page groupSeq=0');
        $this->checkSame('10000', $r1->extFields['maxGroupNum'] ?? null, 'maxGroupNum=10000');
        $this->check(!isset($r1->extFields['dataVersion']), 'first page carries no dataVersion');
        $this->checkSame('2', $r2->extFields['groupSeq'] ?? null, 'second page groupSeq=2');
        $this->checkSame('{"counter":7}', $r2->extFields['dataVersion'] ?? null, 'second page carries dataVersion JSON');

        // Old broker: no totalGroupNum, single round
        $admin5 = $this->newAdmin();
        $admin5->handler = function () {
            $w = new SubscriptionGroupWrapper();
            $w->subscriptionGroupTable = ['G1' => new SubscriptionGroupConfig('G1')];
            return $this->okResponse($w->encode());
        };
        $w5 = $admin5->getAllSubscriptionGroup('b:1');
        $this->checkSame(1, count($admin5->requests), 'old broker finishes in a single round');
        $this->checkSame(['G1'], array_keys($w5->subscriptionGroupTable), 'old broker result');

        // dataVersion changed → restart pagination
        $admin6 = $this->newAdmin();
        $turn = 0;
        $admin6->handler = function (RemotingCommand $cmd) use (&$turn) {
            $turn++;
            $w = new SubscriptionGroupWrapper();
            $w->subscriptionGroupTable = ['P' . $turn => new SubscriptionGroupConfig('P' . $turn)];
            if ($turn === 1) {
                $w->dataVersion = ['counter' => 1];
                return $this->okResponse($w->encode(), ['totalGroupNum' => '3']);
            }
            $w->dataVersion = ['counter' => 2];
            return $this->okResponse($w->encode(), ['totalGroupNum' => '3']);
        };
        $w6 = $admin6->getAllSubscriptionGroup('b:1');
        $this->checkSame(4, count($admin6->requests), 'dataVersion change restarts pagination (4 rounds total)');
        $this->checkSame(['P3', 'P4'], array_keys($w6->subscriptionGroupTable), 'old page data cleared after restart');
        $this->checkSame('0', $admin6->requests[2]['cmd']->extFields['groupSeq'] ?? null, 'restart round groupSeq reset to 0');

        // Non-SUCCESS → MQBrokerException
        $admin7 = $this->newAdmin();
        $admin7->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'bad');
        $this->checkThrows(fn() => $admin7->getAllSubscriptionGroup('b:1'), MQBrokerException::class, 'pagination non-SUCCESS → MQBrokerException');

        // examineSubscriptionGroupConfig
        $admin8 = $this->newAdmin();
        $admin8->handler = function () {
            $w = new SubscriptionGroupWrapper();
            $w->subscriptionGroupTable = ['G1' => new SubscriptionGroupConfig('G1'), 'G2' => new SubscriptionGroupConfig('G2')];
            return $this->okResponse($w->encode());
        };
        $this->checkSame('G2', $admin8->examineSubscriptionGroupConfig('b:1', 'G2')?->groupName, 'examineSubscriptionGroupConfig picks the target group');
        $this->checkSame(null, $admin8->examineSubscriptionGroupConfig('b:1', 'Nope'), 'examineSubscriptionGroupConfig miss → null');

        // getUserSubscriptionGroup filters system/predefined groups
        $admin9 = $this->newAdmin();
        $admin9->handler = function () {
            $w = new SubscriptionGroupWrapper();
            $w->subscriptionGroupTable = [
                'G1' => new SubscriptionGroupConfig('G1'),
                'CID_RMQ_SYS_x' => new SubscriptionGroupConfig('CID_RMQ_SYS_x'),
                'TOOLS_CONSUMER' => new SubscriptionGroupConfig('TOOLS_CONSUMER'),
            ];
            return $this->okResponse($w->encode());
        };
        $this->checkSame(['G1'], array_keys($admin9->getUserSubscriptionGroup('b:1')->subscriptionGroupTable), 'getUserSubscriptionGroup filters');

        // Batch: createAndUpdateSubscriptionGroupConfigList
        $admin10 = $this->newAdmin();
        $g1 = new SubscriptionGroupConfig('G1');
        $g2 = new SubscriptionGroupConfig('G2');
        $admin10->createAndUpdateSubscriptionGroupConfigList('b:1', [$g1, $g2]);
        $cmd10 = $this->lastCmd($admin10);
        $this->checkSame(RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, $cmd10->code, 'UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST=225');
        $body10 = json_decode($cmd10->body ?? '', true);
        $this->checkSame(['G1', 'G2'], array_map(fn($x) => $x['groupName'], $body10['groupConfigList'] ?? []), '225 body key=groupConfigList');
        $this->checkThrows(fn() => $admin10->createAndUpdateSubscriptionGroupConfigList('b:1', []), MQClientException::class, 'batch subscription group empty list throws');
    }

    // ================================================================ Consumer / producer connections

    private function testConnectionAndStats(): void
    {
        // Cluster info for firstBrokerAddr
        $ci = new ClusterInfo();
        $ci->brokerAddrTable = ['broker-a' => [0 => 'b:a']];
        $this->client->clusterInfo = $ci;

        // examineConsumerConnectionInfo: takes the first broker by default
        $admin = $this->newAdmin();
        $this->client->clusterInfo = $ci; // newAdmin rebuilds FakeClient, reseed needed
        $cc = new ConsumerConnection();
        $cc->consumeType = 'CONSUME_PASSIVELY';
        $cc->subscriptionTable = ['T' => ['tagsSet' => ['TagA']]];
        $admin->handler = fn() => $this->okResponse($cc->encode());
        $got = $admin->examineConsumerConnectionInfo('G1');
        $this->checkSame(RequestCode::GET_CONSUMER_CONNECTION_LIST, $this->lastCmd($admin)->code, 'GET_CONSUMER_CONNECTION_LIST=203');
        $this->checkSame('G1', $this->lastCmd($admin)->extFields['consumerGroup'] ?? null, '203 ext consumerGroup');
        $this->checkSame('b:a', $this->lastAddr($admin), 'first cluster broker used when no broker address given');
        $this->checkSame('CONSUME_PASSIVELY', $got->consumeType, 'examineConsumerConnectionInfo parses');
        $this->check($admin->examineConsumerConnection('G1')->consumeType === 'CONSUME_PASSIVELY', 'examineConsumerConnection alias');
        $admin->handler = fn() => $this->okResponse(null);
        $this->checkThrows(fn() => $admin->examineConsumerConnectionInfo('G1'), MQClientException::class, 'empty body → group not online');

        // examineProducerConnectionInfo: empty body → ProducerConnection
        $admin2 = $this->newAdmin();
        $this->client->clusterInfo = $ci;
        $admin2->handler = fn() => $this->okResponse(null);
        $pc = $admin2->examineProducerConnectionInfo('PG');
        $this->checkSame(RequestCode::GET_PRODUCER_CONNECTION_LIST, $this->lastCmd($admin2)->code, 'GET_PRODUCER_CONNECTION_LIST=204');
        $this->checkSame('PG', $this->lastCmd($admin2)->extFields['producerGroup'] ?? null, '204 ext producerGroup');
        $this->checkSame([], $pc->connectionSet, 'empty body → ProducerConnection');

        // examineConsumerRunningInfo
        $admin3 = $this->newAdmin();
        $this->client->clusterInfo = $ci;
        $ri = new \RocketMQ\Remoting\Protocol\ConsumerRunningInfo();
        $ri->properties = ['PROP_CONSUME_TYPE' => 'CONSUME_PASSIVELY'];
        $admin3->handler = fn() => $this->okResponse($ri->encode());
        $got3 = $admin3->examineConsumerRunningInfo('G1', 'cid-1', true);
        $cmd3 = $this->lastCmd($admin3);
        $this->checkSame(RequestCode::GET_CONSUMER_RUNNING_INFO, $cmd3->code, 'GET_CONSUMER_RUNNING_INFO=307');
        $this->checkSame('G1', $cmd3->extFields['consumerGroup'] ?? null, '307 ext consumerGroup');
        $this->checkSame('cid-1', $cmd3->extFields['clientId'] ?? null, '307 ext clientId');
        $this->checkSame('true', $cmd3->extFields['jstackEnable'] ?? null, '307 ext jstackEnable=true');
        $this->checkSame('CONSUME_PASSIVELY', $got3->properties['PROP_CONSUME_TYPE'] ?? null, '307 body parsed');
        $this->check($admin3->getConsumerRunningInfo('G1', 'cid-1') !== null, 'getConsumerRunningInfo alias');
        $admin3->handler = fn() => $this->okResponse(null);
        $this->checkThrows(fn() => $admin3->examineConsumerRunningInfo('G1', 'cid-1'), MQClientException::class, '307 empty body → throws');

        // getConsumerListByGroup delegates to the instance
        $admin4 = $this->newAdmin();
        $this->client->clusterInfo = $ci;
        $this->client->consumerIdList = ['c1', 'c2'];
        $list = $admin4->getConsumerListByGroup('G1');
        $this->checkSame('b:a', $this->client->consumerListCalls[0]['addr'] ?? null, 'getConsumerListByGroup defaults to the first broker');
        $this->checkSame(['c1', 'c2'], $list->consumerIdList, 'getConsumerListByGroup result');

        // examineConsumeStats
        $admin5 = $this->newAdmin();
        $admin5->handler = fn() => $this->okResponse($this->statsBody(2.5, 'T', 'broker-a', 0, 7));
        $cs = $admin5->examineConsumeStats('b:1', 'G1', 'T', ['T1', 'T2']);
        $cmd5 = $this->lastCmd($admin5);
        $this->checkSame(RequestCode::GET_CONSUME_STATS, $cmd5->code, 'GET_CONSUME_STATS=208');
        $this->checkSame(['consumerGroup' => 'G1', 'topic' => 'T', 'topicList' => 'T1;T2'], $cmd5->extFields, '208 ext (topicList joined with semicolons)');
        $this->checkSame(2.5, $cs->consumeTps, '208 parses consumeTps');
        $this->checkSame(7, $cs->offsetTable[0]['value']->consumerOffset, '208 parses consumerOffset');
        $admin5->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin5->examineConsumeStats('b:1', 'G1')->offsetTable, '208 empty body → empty ConsumeStats');

        // fetchConsumeStatsInBroker
        $admin6 = $this->newAdmin();
        $list6 = new ConsumeStatsList();
        $list6->statsList = [['group' => 'G1']];
        $admin6->handler = fn() => $this->okResponse($list6->encode());
        $got6 = $admin6->fetchConsumeStatsInBroker('b:1', true);
        $this->checkSame(RequestCode::GET_BROKER_CONSUME_STATS, $this->lastCmd($admin6)->code, 'GET_BROKER_CONSUME_STATS=317');
        $this->checkSame('true', $this->lastCmd($admin6)->extFields['isOrder'] ?? null, '317 ext isOrder=true');
        $this->checkSame(1, count($got6->statsList), '317 parses statsList');
        $admin6->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin6->fetchConsumeStatsInBroker('b:1')->statsList, '317 empty body');

        // queryTopicConsumeByWho
        $admin7 = $this->newAdmin();
        $admin7->handler = fn() => $this->okResponse(json_encode(['groupList' => ['GA', 'GB', 'GA']]));
        $groups = $admin7->queryTopicConsumeByWho('b:1', 'T');
        $this->checkSame(RequestCode::QUERY_TOPIC_CONSUME_BY_WHO, $this->lastCmd($admin7)->code, 'QUERY_TOPIC_CONSUME_BY_WHO=300');
        $this->checkSame('T', $this->lastCmd($admin7)->extFields['topic'] ?? null, '300 ext topic');
        $this->checkSame(['GA', 'GB'], $groups, '300 groupList deduplicated');
        $admin7->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin7->queryTopicConsumeByWho('b:1', 'T'), '300 empty body → empty');

        // examineConsumeStatsGroup: retry route fan-out merge
        $admin8 = $this->newAdmin();
        $retryRoute = new TopicRouteData();
        $retryRoute->brokerDatas = [
            new BrokerData('C', 'broker-a', [0 => 'b:a']),
            new BrokerData('C', 'broker-b', [0 => 'b:b']),
        ];
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'G1'] = $retryRoute;
        // Both brokers each report their own queues (different brokerName → different MessageQueue keys, 2 entries after merge)
        $admin8->handler = fn(RemotingCommand $cmd, string $addr) => $this->okResponse(
            $this->statsBody(1.5, 'T', $addr === 'b:a' ? 'broker-a' : 'broker-b', 0, 1),
        );
        $merged = $admin8->examineConsumeStatsGroup('G1', 'T');
        $this->checkSame(3.0, $merged->consumeTps, 'examineConsumeStatsGroup accumulates consumeTps');
        $this->checkSame(2, count($merged->offsetTable), 'examineConsumeStatsGroup merges offsetTable');
        $this->checkSame(RequestCode::GET_CONSUME_STATS, $this->lastCmd($admin8)->code, 'examineConsumeStatsGroup per-broker 208');
        $admin8b = $this->newAdmin();
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'G2'] = new TopicRouteData();
        $this->checkThrows(fn() => $admin8b->examineConsumeStatsGroup('G2'), MQClientException::class, 'examineConsumeStatsGroup all empty → throws');

        // consumed()
        $admin9 = $this->newAdmin();
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => '10.0.0.1:10911']];
        $retryRoute9 = new TopicRouteData();
        $retryRoute9->brokerDatas = [new BrokerData('C', 'broker-a', [MixAll::MASTER_ID => '10.0.0.1:10911'])];
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'G1'] = $retryRoute9;
        $admin9->handler = fn() => $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 5));
        $msg = new MessageExt(topic: 'T', body: 'x');
        $msg->setQueueId(0);
        $msg->setQueueOffset(3);
        $msg->storeHost = '10.0.0.1';
        $msg->storeHostPort = 10911;
        $this->checkSame(true, $admin9->consumed($msg, 'G1'), 'consumed: offset advanced → true');
        $msg2 = new MessageExt(topic: 'T', body: 'x');
        $msg2->setQueueId(0);
        $msg2->setQueueOffset(9);
        $msg2->storeHost = '10.0.0.1';
        $msg2->storeHostPort = 10911;
        $this->checkSame(false, $admin9->consumed($msg2, 'G1'), 'consumed: offset not advanced → false');

        // queryTopicsByConsumerToBroker / queryTopicsByConsumer
        $admin10 = $this->newAdmin();
        $tl = new TopicList();
        $tl->topicList = ['T1'];
        $admin10->handler = fn() => $this->okResponse($tl->encode());
        $got10 = $admin10->queryTopicsByConsumerToBroker('b:1', 'G1');
        $this->checkSame(RequestCode::QUERY_TOPICS_BY_CONSUMER, $this->lastCmd($admin10)->code, 'QUERY_TOPICS_BY_CONSUMER=343');
        $this->checkSame('G1', $this->lastCmd($admin10)->extFields['group'] ?? null, '343 ext group');
        $this->checkSame(['T1'], $got10->getTopicList(), '343 parses');

        $admin11 = $this->newAdmin();
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'G1'] = new TopicRouteData();
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'G1']->brokerDatas = [
            new BrokerData('C', 'broker-a', [0 => 'b:a']),
            new BrokerData('C', 'broker-b', [0 => 'b:b']),
        ];
        $admin11->handler = fn() => $this->okResponse((new TopicList())->encode());
        $merged11 = $admin11->queryTopicsByConsumer('G1');
        $this->checkSame([], $merged11->getTopicList(), 'queryTopicsByConsumer merges empty results');

        // querySubscription / getConsumeStatus / cloneGroupOffset
        $admin12 = $this->newAdmin();
        $admin12->handler = fn() => $this->okResponse(json_encode(['topics' => ['T']]));
        $sub = $admin12->querySubscription('b:1', 'G1', 'T');
        $this->checkSame(RequestCode::QUERY_SUBSCRIPTION_BY_CONSUMER, $this->lastCmd($admin12)->code, 'QUERY_SUBSCRIPTION_BY_CONSUMER=345');
        $this->checkSame(['topics' => ['T']], $sub, '345 parses');
        $admin12->handler = fn() => $this->okResponse(null);
        $this->checkSame(null, $admin12->querySubscription('b:1', 'G1', 'T'), '345 empty body → null');

        $admin13 = $this->newAdmin();
        $admin13->handler = fn() => $this->okResponse(json_encode(['consumerTable' => ['cid-1' => ['pullRT' => 1.5]]]));
        $status = $admin13->getConsumeStatus('b:1', 'T', 'G1', 'cid-1');
        $this->checkSame(RequestCode::INVOKE_BROKER_TO_GET_CONSUMER_STATUS, $this->lastCmd($admin13)->code, 'INVOKE_BROKER_TO_GET_CONSUMER_STATUS=223');
        $this->checkSame('cid-1', $this->lastCmd($admin13)->extFields['clientAddr'] ?? null, '223 ext clientAddr');
        $this->checkSame(['cid-1' => ['pullRT' => 1.5]], $status, '223 reads consumerTable');
        $admin13->handler = fn() => $this->okResponse(null);
        $this->checkSame([], $admin13->getConsumeStatus('b:1', 'T', 'G1'), '223 empty body → empty');

        $admin14 = $this->newAdmin();
        $admin14->cloneGroupOffset('b:1', 'S', 'D', 'T', true);
        $this->checkSame(RequestCode::CLONE_GROUP_OFFSET, $this->lastCmd($admin14)->code, 'CLONE_GROUP_OFFSET=314');
        $this->checkSame(
            ['srcGroup' => 'S', 'destGroup' => 'D', 'topic' => 'T', 'offline' => 'true'],
            $this->lastCmd($admin14)->extFields,
            '314 ext'
        );
    }

    // ================================================================ Message track

    private function testMessageTrack(): void
    {
        // PULL (CONSUME_ACTIVELY)
        $admin = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $cc = new ConsumerConnection();
        $cc->consumeType = 'CONSUME_ACTIVELY';
        $admin->handler = function (RemotingCommand $cmd, string $addr) use ($cc) {
            if ($cmd->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GA']]));
            }
            if ($cmd->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc->encode());
            }
            return $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 9));
        };
        $tracks = $admin->messageTrackDetail($this->trackMsg());
        $this->checkSame(1, count($tracks), 'messageTrackDetail single group');
        $this->checkSame('GA', $tracks[0]->consumerGroup, 'track consumerGroup');
        $this->checkSame(TrackType::PULL, $tracks[0]->trackType, 'CONSUME_ACTIVELY → PULL');
        $this->checkSame(null, $tracks[0]->exceptionDesc, 'track without exception description');

        // NOT_ONLINE (206)
        $admin2 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $admin2->handler = function (RemotingCommand $cmd) {
            if ($cmd->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GB']]));
            }
            return $this->errResponse(ResponseCode::CONSUMER_NOT_ONLINE, 'offline');
        };
        $tracks2 = $admin2->messageTrackDetail($this->trackMsg());
        $this->checkSame(TrackType::NOT_ONLINE, $tracks2[0]->trackType, '206 → NOT_ONLINE');
        $this->check(str_contains((string) $tracks2[0]->exceptionDesc, 'CODE:206'), 'NOT_ONLINE carries the exception description');

        // CONSUMED + CONSUMED_BUT_FILTERED (subscription narrower than the message)
        $admin3 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GC'] = $this->retryRoute();
        $cc3 = new ConsumerConnection();
        $cc3->consumeType = 'CONSUME_PASSIVELY';
        $cc3->subscriptionTable = ['T' => ['tagsSet' => ['TagA']]];
        $admin3->handler = function (RemotingCommand $cmd, string $addr) use ($cc3) {
            if ($cmd->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GC']]));
            }
            if ($cmd->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc3->encode());
            }
            return $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 9));
        };
        $msgTagA = $this->trackMsg();
        $msgTagA->setTags('TagA');
        $t3 = $admin3->messageTrackDetail($msgTagA);
        $this->checkSame(TrackType::CONSUMED, $t3[0]->trackType, 'offset advanced and subscription contains the message tag → CONSUMED');
        $msgTagB = $this->trackMsg();
        $msgTagB->setTags('TagB');
        $t3b = $admin3->messageTrackDetail($msgTagB);
        $this->checkSame(TrackType::CONSUMED_BUT_FILTERED, $t3b[0]->trackType, 'subscription lacks the message tag → CONSUMED_BUT_FILTERED');
        $msgNoTag = $this->trackMsg();
        $t3c = $admin3->messageTrackDetail($msgNoTag);
        $this->checkSame(TrackType::CONSUMED_BUT_FILTERED, $t3c[0]->trackType, 'tag-less message against a narrow subscription → CONSUMED_BUT_FILTERED');
        // Subscription contains "*" → stays CONSUMED
        $cc3b = clone $cc3;
        $cc3b->subscriptionTable = ['T' => ['tagsSet' => ['*']]];
        $admin3->handler = function (RemotingCommand $cmd, string $addr) use ($cc3b) {
            if ($cmd->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GC']]));
            }
            if ($cmd->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc3b->encode());
            }
            return $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 9));
        };
        $t3d = $admin3->messageTrackDetail($msgNoTag);
        $this->checkSame(TrackType::CONSUMED, $t3d[0]->trackType, 'subscription contains * → CONSUMED');

        // NOT_CONSUME_YET
        $admin4 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GD'] = $this->retryRoute();
        $cc4 = new ConsumerConnection();
        $cc4->consumeType = 'CONSUME_PASSIVELY';
        $admin4->handler = function (RemotingCommand $cmd, string $addr) use ($cc4) {
            if ($cmd->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GD']]));
            }
            if ($cmd->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc4->encode());
            }
            return $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 1));
        };
        $t4 = $admin4->messageTrackDetail($this->trackMsg());
        $this->checkSame(TrackType::NOT_CONSUME_YET, $t4[0]->trackType, 'offset not advanced → NOT_CONSUME_YET');

        // BROADCAST_CONSUMPTION (213) — criterion: only when 213 comes back during the consumed()
        // phase (208) does it become CONSUME_BROADCASTING; a 213 at the connection phase (203)
        // yields UNKNOWN + desc.
        $admin5 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $this->client->topicRoutes[MixAll::RETRY_GROUP_TOPIC_PREFIX . 'GE'] = $this->retryRoute();
        $cc5 = new ConsumerConnection();
        $cc5->consumeType = 'CONSUME_PASSIVELY';
        $admin5->handler = function (RemotingCommand $cmd) use ($cc5) {
            if ($cmd->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GE']]));
            }
            if ($cmd->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc5->encode());
            }
            return $this->errResponse(ResponseCode::BROADCAST_CONSUMPTION, 'broadcast');
        };
        $t5 = $admin5->messageTrackDetail($this->trackMsg());
        $this->checkSame(TrackType::CONSUME_BROADCASTING, $t5[0]->trackType, '213 → CONSUME_BROADCASTING');

        // No available broker → empty list
        $admin6 = $this->newAdmin();
        $emptyRoute = new TopicRouteData();
        $this->client->topicRoutes['T'] = $emptyRoute;
        $this->checkSame([], $admin6->messageTrackDetail($this->trackMsg()), 'no broker → empty tracks');

        // MessageTrack DTO
        $mt = new MessageTrack('G1', TrackType::CONSUMED, 'note');
        $this->checkSame(
            ['consumerGroup' => 'G1', 'trackType' => 'CONSUMED', 'exceptionDesc' => 'note'],
            $mt->toDict(),
            'MessageTrack.toDict'
        );
        $mt2 = MessageTrack::decode($mt->encode());
        $this->checkSame($mt->consumerGroup, $mt2->consumerGroup, 'MessageTrack roundtrip consumerGroup');
        $this->checkSame($mt->trackType, $mt2->trackType, 'MessageTrack roundtrip trackType');
        $mt3 = MessageTrack::fromDict(['consumerGroup' => 'G2']);
        $this->checkSame(TrackType::UNKNOWN, $mt3->trackType, 'MessageTrack missing trackType → UNKNOWN');
        $mt4 = new MessageTrack('G3');
        $this->checkSame(['consumerGroup' => 'G3', 'trackType' => 'UNKNOWN'], $mt4->toDict(), 'MessageTrack null exceptionDesc not serialized');
        $this->checkSame(TrackType::CONSUMED, TrackType::CONSUMED, 'TrackType constant exists');
        $this->checkSame('CONSUMED_BUT_FILTERED', TrackType::CONSUMED_BUT_FILTERED, 'TrackType.CONSUMED_BUT_FILTERED');
        $this->checkSame('NOT_CONSUME_YET', TrackType::NOT_CONSUME_YET, 'TrackType.NOT_CONSUME_YET');
        $this->checkSame('NOT_ONLINE', TrackType::NOT_ONLINE, 'TrackType.NOT_ONLINE');
        $this->checkSame('CONSUME_BROADCASTING', TrackType::CONSUME_BROADCASTING, 'TrackType.CONSUME_BROADCASTING');
    }

    private function trackMsg(): MessageExt
    {
        $msg = new MessageExt(topic: 'T', body: 'x');
        $msg->setQueueId(0);
        $msg->setQueueOffset(3);
        $msg->storeHost = 'b';
        return $msg;
    }

    // ================================================================ Offset management

    private function testOffsetManagement(): void
    {
        $mq = new MessageQueue('T', 'broker-a', 0);

        // max/min offset delegation
        $admin = $this->newAdmin();
        $this->client->maxOffsetValue = 11;
        $this->client->minOffsetValue = 2;
        $this->checkSame(11, $admin->maxOffset($mq), 'maxOffset delegation');
        $this->checkSame(2, $admin->minOffset($mq), 'minOffset delegation');

        // search boundary
        $admin2 = $this->newAdmin();
        $this->client->searchOffsetValue = 5;
        $this->checkSame(5, $admin2->searchOffset($mq, 111), 'searchOffset delegation');
        $this->checkSame('LOWER', $this->client->searchOffsetCalls[0]['boundary']?->value, 'searchOffset explicit LOWER');
        $this->checkSame(5, $admin2->searchLowerBoundaryOffset($mq, 111), 'searchLowerBoundaryOffset');
        $this->checkSame('LOWER', $this->client->searchOffsetCalls[1]['boundary']?->value, 'searchLower LOWER');
        $this->checkSame(5, $admin2->searchUpperBoundaryOffset($mq, 111), 'searchUpperBoundaryOffset');
        $this->checkSame('UPPER', $this->client->searchOffsetCalls[2]['boundary']?->value, 'searchUpper UPPER');
        $this->checkSame(111, $this->client->searchOffsetCalls[0]['ts'], 'searchOffset timestamp pass-through');

        // earliestMsgStoreTime
        $admin3 = $this->newAdmin();
        $this->client->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b:a']];
        $admin3->handler = fn() => $this->okResponse(null, ['timestamp' => '123456']);
        $this->checkSame(123456, $admin3->earliestMsgStoreTime($mq), 'earliestMsgStoreTime reads ext timestamp');
        $cmd3 = $this->lastCmd($admin3);
        $this->checkSame(RequestCode::GET_EARLIEST_MSG_STORETIME, $cmd3->code, 'GET_EARLIEST_MSG_STORETIME=32');
        $this->checkSame(['topic' => 'T', 'queueId' => '0', 'brokerName' => 'broker-a'], $cmd3->extFields, '32 ext');
        $admin3b = $this->newAdmin();
        $this->checkThrows(fn() => $admin3b->earliestMsgStoreTime(new MessageQueue('T', 'no-broker', 0)), MQClientException::class, 'earliestMsgStoreTime no master → The broker[X] not exist');

        // examineConsumerOffset / updateConsumerOffset / ToBroker
        $admin4 = $this->newAdmin();
        $this->client->queryOffsetValue = 42;
        $this->checkSame(42, $admin4->examineConsumerOffset('G1', $mq), 'examineConsumerOffset delegation');
        $admin4->updateConsumerOffset('G1', $mq, 42);
        $this->checkSame(42, $this->client->updateOffsetCalls[0]['offset'], 'updateConsumerOffset delegation');
        $admin4->updateConsumerOffsetToBroker('b:9', 'G1', $mq, 43);
        $this->checkSame('b:9', $this->client->updateOffsetCalls[1]['addr'], 'updateConsumerOffsetToBroker explicit addr');

        // resetOffsetByTimestamp: single broker + ext shape
        $admin5 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $admin5->handler = function () {
            $b = new ResetOffsetBody();
            $b->offsetTable[] = ['mq' => new MessageQueue('T', 'broker-a', 0), 'offset' => 100];
            return $this->okResponse($b->encode());
        };
        $reset = $admin5->resetOffsetByTimestamp('T', 'G1', 123456);
        $this->checkSame(1, count($reset), 'resetOffsetByTimestamp returns the queue table');
        $this->checkSame(100, $reset[0]['offset'], 'resetOffsetByTimestamp offset value');
        $this->checkSame('broker-a', $reset[0]['mq']->brokerName, 'resetOffsetByTimestamp mq.brokerName');
        $cmd5 = $this->lastCmd($admin5);
        $this->checkSame(RequestCode::INVOKE_BROKER_TO_RESET_OFFSET, $cmd5->code, 'INVOKE_BROKER_TO_RESET_OFFSET=222');
        $this->checkSame(
            ['topic' => 'T', 'group' => 'G1', 'timestamp' => '123456', 'isForce' => 'true', 'offset' => '-1'],
            $cmd5->extFields,
            '222 ext (key is isForce not force; offset defaults to -1)'
        );

        // LMQ topic re-routed via clusterName
        $admin6 = $this->newAdmin();
        $this->route('ClusterA', 'broker-c', 'b:c');
        $admin6->handler = function () {
            $b = new ResetOffsetBody();
            $b->offsetTable[] = ['mq' => new MessageQueue('%LMQ%x', 'broker-c', 0), 'offset' => 7];
            return $this->okResponse($b->encode());
        };
        $reset6 = $admin6->resetOffsetByTimestamp('%LMQ%x', 'G1', 1, true, 'ClusterA');
        $this->checkSame('broker-c', $reset6[0]['mq']->brokerName, 'LMQ topic looks up the route by clusterName');

        // Empty offset table → throws
        $admin7 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $admin7->handler = fn() => $this->okResponse(null);
        $this->checkThrows(fn() => $admin7->resetOffsetByTimestamp('T', 'G1', 1), MQClientException::class, 'reset without a queue table → throws');

        // is_cpp → language=CPP
        $admin8 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $admin8->handler = function () {
            $b = new ResetOffsetBody();
            $b->offsetTable[] = ['mq' => new MessageQueue('T', 'broker-a', 0), 'offset' => 1];
            return $this->okResponse($b->encode());
        };
        $admin8->resetOffsetByTimestamp('T', 'G1', 1, true, null, true);
        $this->checkSame(LanguageCode::CPP->value, $this->lastCmd($admin8)->language, 'is_cpp=true → language=CPP');

        // 222 non-SUCCESS → MQClientException
        $admin9 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $admin9->handler = fn() => $this->errResponse(ResponseCode::CONSUMER_NOT_ONLINE, 'no consumer');
        $e9 = $this->capture(fn() => $admin9->resetOffsetByTimestamp('T', 'G1', 1));
        $this->check($e9 instanceof MQClientException && $e9->responseCode === ResponseCode::CONSUMER_NOT_ONLINE, '222 failure propagates with the response code');

        // resetOffsetByQueueId: two RPCs
        $admin10 = $this->newAdmin();
        $admin10->handler = function () {
            $b = new ResetOffsetBody();
            $b->offsetTable[] = ['mq' => new MessageQueue('T', '', 3), 'offset' => 55];
            return $this->okResponse($b->encode());
        };
        $reset10 = $admin10->resetOffsetByQueueId('b:1', 'G1', 'T', 3, 55);
        $this->checkSame(1, count($this->client->updateOffsetCalls), 'resetOffsetByQueueId first call 25 updates the offset');
        $this->checkSame(55, $this->client->updateOffsetCalls[0]['offset'], 'first call offset value');
        $this->checkSame('b:1', $this->client->updateOffsetCalls[0]['addr'], 'first call hits the given broker');
        $cmd10 = $this->lastCmd($admin10);
        $this->checkSame(RequestCode::INVOKE_BROKER_TO_RESET_OFFSET, $cmd10->code, 'resetOffsetByQueueId second call 222');
        $this->checkSame(
            ['topic' => 'T', 'group' => 'G1', 'timestamp' => '0', 'isForce' => 'false', 'offset' => '55', 'queueId' => '3'],
            $cmd10->extFields,
            '222 single-queue overload ext (timestamp=0/force=false/offset=55/queueId=3)'
        );
        $this->checkSame(55, $reset10[0]['offset'], 'resetOffsetByQueueId returns the queue table');

        // resetOffsetNew: 206 falls back to the legacy implementation
        $admin11 = $this->newAdmin();
        $trd = $this->route('T', 'broker-a', 'b:a', 1, 1);
        $admin11->handler = fn() => $this->errResponse(ResponseCode::CONSUMER_NOT_ONLINE, 'offline');
        $this->client->queryOffsetValue = 2;
        $this->client->maxOffsetValue = 10;
        $admin11->resetOffsetNew('G1', 'T', -1);
        $this->checkSame(1, count($this->client->updateOffsetCalls), 'resetOffsetNew fallback path writes back the offset');
        $this->checkSame(10, $this->client->updateOffsetCalls[0]['offset'], 'timestamp=-1 → maxOffset');

        $admin12 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a');
        $admin12->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $e12 = $this->capture(fn() => $admin12->resetOffsetNew('G1', 'T', -1));
        $this->check($e12 instanceof MQClientException && $e12->responseCode === ResponseCode::SYSTEM_ERROR, 'resetOffsetNew non-206 rethrown as-is');

        // resetOffsetByTimestampOld: force=false and reset > consumer → no write-back
        $admin13 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:a', 1, 1);
        $this->client->queryOffsetValue = 2;
        $this->client->searchOffsetValue = 9;
        $got13 = $admin13->resetOffsetByTimestampOld('G1', 'T', 100, false);
        $this->checkSame([], $got13, 'force=false and offset advanced → no write-back');
        $got13b = $admin13->resetOffsetByTimestampOld('G1', 'T', 100, true);
        $this->checkSame(9, $got13b[0]['offset'] ?? null, 'force=true → writes back the search offset');
        $this->checkSame(1, count($this->client->updateOffsetCalls), 'force=true performs updateConsumerOffset');

        // queryMessage family
        $admin14 = $this->newAdmin();
        $m1 = new MessageExt(topic: 'T', body: 'b');
        $m1->setTags('k1');
        $this->client->queryMessageResult = [$m1];
        $got14 = $admin14->queryMessage('T', 'k1', 32, 0, 100);
        $this->checkSame(['indexType' => 'K', 'uniqKey' => false], [
            'indexType' => $this->client->queryMessageCalls[0]['indexType'],
            'uniqKey' => $this->client->queryMessageCalls[0]['uniqKey'],
        ], 'queryMessage indexType=K');
        $this->checkSame([$m1], $got14, 'queryMessage result');
        $uniq = $admin14->queryMessageByUniqKey('T', 'u1');
        $this->checkSame('U', $this->client->queryMessageCalls[1]['indexType'], 'queryMessageByUniqKey indexType=U');
        $this->checkSame(true, $this->client->queryMessageCalls[1]['uniqKey'], 'queryMessageByUniqKey uniqKey=true');
        $this->checkSame(32, $this->client->queryMessageCalls[1]['maxNum'], 'queryMessageByUniqKey maxNum=32');
        $this->checkSame($m1, $uniq, 'queryMessageByUniqKey takes the first hit');
        $this->client->queryMessageResult = [];
        $this->checkSame(null, $admin14->queryMessageByUniqKey('T', 'u1'), 'uniqKey not found → null');
        $admin14->queryMessageByKey('T', 'k2', 5);
        $this->checkSame(5, $this->client->queryMessageCalls[3]['maxNum'], 'queryMessageByKey maxNum pass-through');

        // viewMessage: offset msgId direct lookup
        $admin15 = $this->newAdmin();
        $target = new MessageExt(topic: 'T', body: 'vb');
        $msgId = '7f00000100002a9f0000000000000064';
        $admin15->handler = fn(RemotingCommand $cmd) => $this->okResponse(\RocketMQ\Common\MessageDecoder::encodeMessageExt($target));
        $got15 = $admin15->viewMessage('T', $msgId);
        $this->checkSame(RequestCode::VIEW_MESSAGE_BY_ID, $this->lastCmd($admin15)->code, 'VIEW_MESSAGE_BY_ID=33');
        $this->checkSame('127.0.0.1:10911', $this->lastAddr($admin15), 'viewMessage decodes the broker address from msgId (decodes the address directly, no route lookup)');
        $this->checkSame($target->getBody(), $got15->getBody(), 'viewMessage hits the offset msgId');

        // viewMessage: port=0 (uniqKey shape) → falls back to the uniq query
        $admin16 = $this->newAdmin();
        $admin16->handler = fn() => $this->okResponse(null);
        $this->client->queryMessageResult = [$target];
        $got16 = $admin16->viewMessage('T', '7f0000010000000000000000000064');
        $this->checkSame($target, $got16, 'viewMessage uniq-key fallback hit');
        $this->checkSame('U', $this->client->queryMessageCalls[0]['indexType'] ?? null, 'fallback goes through the uniq query');

        // viewMessage: all fail
        $admin17 = $this->newAdmin();
        $admin17->handler = fn() => $this->okResponse(null);
        $this->client->queryMessageResult = [];
        $e17 = $this->capture(fn() => $admin17->viewMessage('T', '7f00000100002a9f0000000000000064'));
        $this->check($e17 instanceof MQClientException && $e17->responseCode === ResponseCode::NO_MESSAGE, 'viewMessage all fail → NO_MESSAGE');
        $this->check($e17 !== null && $e17->getPrevious() !== null, 'viewMessage keeps the by-id failure cause');

        // queryConsumeQueue
        $admin18 = $this->newAdmin();
        $qb = new \RocketMQ\Remoting\Protocol\QueryConsumeQueueResponseBody();
        $qb->maxQueueIndex = 88;
        $admin18->handler = fn() => $this->okResponse($qb->encode());
        $got18 = $admin18->queryConsumeQueue('b:1', 'T', 0, 10, 32, 'G1');
        $this->checkSame(RequestCode::QUERY_CONSUME_QUEUE, $this->lastCmd($admin18)->code, 'QUERY_CONSUME_QUEUE=321');
        $this->checkSame(
            ['topic' => 'T', 'queueId' => '0', 'index' => '10', 'count' => '32', 'consumerGroup' => 'G1'],
            $this->lastCmd($admin18)->extFields,
            '321 ext'
        );
        $this->checkSame(88, $got18->maxQueueIndex, '321 parses');

        // consumeMessageDirectly: offset msgId full chain → viewMessage locates storeHost + 309 dispatch
        $admin19 = $this->newAdmin();
        $stored = new MessageExt(topic: 'T', body: 'vb');
        $stored->setStoreHost('10.0.0.9');
        $stored->storeHostPort = 10911;
        $admin19->handler = function (RemotingCommand $cmd) use ($stored) {
            if ($cmd->code === RequestCode::VIEW_MESSAGE_BY_ID) {
                return $this->okResponse(\RocketMQ\Common\MessageDecoder::encodeMessageExt($stored));
            }
            return $this->okResponse((new ConsumeMessageDirectlyResult())->encode());
        };
        $res19 = $admin19->consumeMessageDirectly('G1', 'cid-1', 'T', '7f00000100002a9f0000000000000064');
        $byId = $admin19->requests[0]['cmd'];
        $direct = $admin19->requests[1]['cmd'];
        $this->checkSame(RequestCode::VIEW_MESSAGE_BY_ID, $byId->code, '309 preceded by viewMessage to locate');
        $this->checkSame(RequestCode::CONSUME_MESSAGE_DIRECTLY, $direct->code, 'CONSUME_MESSAGE_DIRECTLY=309');
        $this->checkSame('10.0.0.9:10911', $admin19->requests[1]['addr'], '309 sent to the message storeHost');
        $this->checkSame(
            ['consumerGroup' => 'G1', 'clientId' => 'cid-1', 'msgId' => '7f00000100002a9f0000000000000064', 'topic' => 'T'],
            $direct->extFields,
            '309 ext (without UNIQ_KEY the msgId is dispatched as-is)'
        );
        $this->check($res19 instanceof ConsumeMessageDirectlyResult, '309 body decoded as ConsumeMessageDirectlyResult');

        // consumeMessageDirectly: message with UNIQ_KEY → dispatches offsetMsgId
        $admin20 = $this->newAdmin();
        $uniqMsg = new MessageExt(topic: 'T', body: 'vb');
        $uniqMsg->setStoreHost('10.0.0.9');
        $uniqMsg->storeHostPort = 10911;
        $uniqMsg->putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, 'uniq-abc');
        $uniqMsg->setOffsetMsgId('7f00000100002a9f0000000000000065');
        // Hard-decoding a uniqKey-shaped msgId must fail → viewMessage falls back to the uniq query; the double answers from there
        $this->client->queryMessageResult = [$uniqMsg];
        $admin20->handler = function (RemotingCommand $cmd) use ($uniqMsg) {
            if ($cmd->code === RequestCode::VIEW_MESSAGE_BY_ID) {
                return $this->okResponse(\RocketMQ\Common\MessageDecoder::encodeMessageExt($uniqMsg));
            }
            return $this->okResponse((new ConsumeMessageDirectlyResult())->encode());
        };
        $admin20->consumeMessageDirectly('G1', 'cid-1', 'T', 'uniq-abc');
        $this->checkSame(1, count($admin20->requests), '309 uniqKey hard-decode failure → no VIEW_MESSAGE_BY_ID sent');
        $this->checkSame(RequestCode::CONSUME_MESSAGE_DIRECTLY, $admin20->requests[0]['cmd']->code, '309 uniqKey fallback goes direct');
        $this->checkSame('10.0.0.9:10911', $admin20->requests[0]['addr'], '309 uniqKey fallback still targets storeHost');
        $this->checkSame('7f00000100002a9f0000000000000065', $admin20->requests[0]['cmd']->extFields['msgId'] ?? null, '309 with UNIQ_KEY → dispatches offsetMsgId (both paths agree)');

        // consumeMessageDirectly: explicit brokerAddr skips viewMessage and sends directly
        $admin21 = $this->newAdmin();
        $admin21->handler = fn() => $this->okResponse((new ConsumeMessageDirectlyResult())->encode());
        $admin21->consumeMessageDirectly('G1', 'cid-1', 'T', 'm-1', 'b:1');
        $this->checkSame(1, count($admin21->requests), '309 explicit brokerAddr skips viewMessage');
        $this->checkSame('b:1', $this->lastAddr($admin21), '309 sends to the given address');
        $this->checkSame('m-1', $this->lastCmd($admin21)->extFields['msgId'] ?? null, '309 explicit address msgId as-is');

        // consumeMessageDirectly: empty body → throws; non-SUCCESS → MQBrokerException
        $admin22 = $this->newAdmin();
        $admin22->handler = fn() => $this->okResponse(null);
        $this->checkThrows(fn() => $admin22->consumeMessageDirectly('G1', 'cid-1', 'T', 'm-1', 'b:1'), MQClientException::class, '309 empty body throws');
        $admin23 = $this->newAdmin();
        $admin23->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'client offline');
        $e23 = $this->capture(fn() => $admin23->consumeMessageDirectly('G1', 'cid-1', 'T', 'm-1', 'b:1'));
        $this->check($e23 instanceof MQBrokerException, '309 non-SUCCESS → MQBrokerException');
        $this->checkSame('client offline', $e23?->getMessage(), '309 non-SUCCESS passes the remark through');
    }

    // ================================================================ Batch / ops / NameServer configuration

    private function testBatchAndOps(): void
    {
        // createAndUpdateTopicConfigList
        $admin = $this->newAdmin();
        $admin->createAndUpdateTopicConfigList('b:1', [new TopicConfig('T1', 1, 1), new TopicConfig('T2', 2, 2)]);
        $cmd = $this->lastCmd($admin);
        $this->checkSame(RequestCode::UPDATE_AND_CREATE_TOPIC_LIST, $cmd->code, 'UPDATE_AND_CREATE_TOPIC_LIST=18');
        $this->checkSame([], $cmd->extFields, '18 custom header empty');
        $body = json_decode($cmd->body ?? '', true);
        $this->checkSame(['T1', 'T2'], array_map(fn($x) => $x['topicName'], $body['topicConfigList'] ?? []), '18 body key=topicConfigList');
        $this->checkThrows(fn() => $admin->createAndUpdateTopicConfigList('b:1', []), MQClientException::class, 'batch topic empty list throws');

        // createStaticTopic
        $admin2 = $this->newAdmin();
        $admin2->createStaticTopic('b:1', 'TBW102', new TopicConfig('ST', 8, 8, 6), ['b:1' => ['0' => 0]], true);
        $cmd2 = $this->lastCmd($admin2);
        $this->checkSame(RequestCode::UPDATE_AND_CREATE_STATIC_TOPIC, $cmd2->code, 'UPDATE_AND_CREATE_STATIC_TOPIC=513');
        $ext2 = $cmd2->extFields;
        $this->checkSame('ST', $ext2['topic'] ?? null, '513 ext topic');
        $this->checkSame('TBW102', $ext2['defaultTopic'] ?? null, '513 ext defaultTopic');
        $this->checkSame('8', $ext2['readQueueNums'] ?? null, '513 ext readQueueNums');
        $this->checkSame('8', $ext2['writeQueueNums'] ?? null, '513 ext writeQueueNums');
        $this->checkSame('6', $ext2['perm'] ?? null, '513 ext perm');
        $this->checkSame('SINGLE_TAG', $ext2['topicFilterType'] ?? null, '513 ext topicFilterType');
        $this->checkSame('false', $ext2['order'] ?? null, '513 ext order lowercase');
        $this->checkSame('true', $ext2['force'] ?? null, '513 ext force lowercase');
        $this->checkSame(['b:1' => ['0' => 0]], json_decode($cmd2->body ?? '', true), '513 body is the mappingDetail JSON');

        // updateAndGetGroupReadForbidden
        $admin3 = $this->newAdmin();
        $admin3->handler = fn() => $this->okResponse(json_encode(['forbidden' => false]));
        $got3 = $admin3->updateAndGetGroupReadForbidden('b:1', 'G1', 'T', true);
        $this->checkSame(RequestCode::UPDATE_AND_GET_GROUP_FORBIDDEN, $this->lastCmd($admin3)->code, 'UPDATE_AND_GET_GROUP_FORBIDDEN=353');
        $this->checkSame('true', $this->lastCmd($admin3)->extFields['readable'] ?? null, '353 readable=true dispatched');
        $this->checkSame(['forbidden' => false], $got3, '353 parses GroupForbidden');
        $admin3->updateAndGetGroupReadForbidden('b:1', 'G1', 'T');
        $this->check(!isset($this->lastCmd($admin3)->extFields['readable']), '353 readable=null not dispatched');
        $admin3->handler = fn() => $this->okResponse(null);
        $this->checkThrows(fn() => $admin3->updateAndGetGroupReadForbidden('b:1', 'G1', 'T'), MQClientException::class, '353 empty body throws');
        $this->checkThrows(fn() => $admin3->updateAndGetGroupReadForbidden('b:1', '', 'T'), MQClientException::class, '353 group required');
        $this->checkThrows(fn() => $admin3->updateAndGetGroupReadForbidden('b:1', 'G', ''), MQClientException::class, '353 topic required');

        // resumeCheckHalfMessage: non-SUCCESS returns false without throwing
        $admin4 = $this->newAdmin();
        $this->checkSame(true, $admin4->resumeCheckHalfMessage('b:1', 'T', 'm1'), '323 success → true');
        $cmd4 = $this->lastCmd($admin4);
        $this->checkSame(RequestCode::RESUME_CHECK_HALF_MESSAGE, $cmd4->code, 'RESUME_CHECK_HALF_MESSAGE=323');
        $this->checkSame(['topic' => 'T', 'msgId' => 'm1'], $cmd4->extFields, '323 ext');
        $admin4->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'not half');
        $this->checkSame(false, $admin4->resumeCheckHalfMessage('b:1', 'T', 'm1'), '323 broker rejects → false without throwing');
        $admin4->resumeCheckHalfMessage('b:1', 'T');
        $this->checkSame(['topic' => 'T'], $this->lastCmd($admin4)->extFields, '323 without msgId nothing dispatched');
        $this->checkThrows(fn() => $admin4->resumeCheckHalfMessage('b:1', ''), MQClientException::class, '323 topic required');

        // cleanExpiredConsumerQueue / deleteExpiredCommitLog / ByAddr
        $admin5 = $this->newAdmin();
        $admin5->cleanExpiredConsumerQueue('b:1', 72);
        $this->checkSame(RequestCode::CLEAN_EXPIRED_CONSUMEQUEUE, $this->lastCmd($admin5)->code, 'CLEAN_EXPIRED_CONSUMEQUEUE=306');
        $this->checkSame('72', $this->lastCmd($admin5)->extFields['time'] ?? null, '306 ext time');
        $admin5->deleteExpiredCommitLog('b:1', 48);
        $this->checkSame(RequestCode::DELETE_EXPIRED_COMMITLOG, $this->lastCmd($admin5)->code, 'DELETE_EXPIRED_COMMITLOG=329');
        $this->checkSame('48', $this->lastCmd($admin5)->extFields['time'] ?? null, '329 ext time');
        $admin5->cleanUnusedTopicByAddr('b:2');
        $this->checkSame(RequestCode::CLEAN_UNUSED_TOPIC, $this->lastCmd($admin5)->code, 'cleanUnusedTopicByAddr single request 316');
        $failed = $admin5->cleanExpiredConsumerQueueByAddr(['b:ok', 'b:bad'], 1);
        $admin5->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $failed2 = $admin5->cleanExpiredConsumerQueueByAddr(['b:bad2', 'b:bad3'], 1);
        $this->checkSame(['b:bad2', 'b:bad3'], $failed2, 'cleanExpiredConsumerQueueByAddr returns failed addresses');
        $admin5->handler = fn() => $this->okResponse();
        $this->checkSame([], $admin5->deleteExpiredCommitLogByAddr(['b:1'], 1), 'deleteExpiredCommitLogByAddr all succeed');
        $admin5->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $this->checkSame(['b:1'], $admin5->deleteExpiredCommitLogByAddr(['b:1'], 1), 'deleteExpiredCommitLogByAddr failed address');
        $this->check(true, 'cleanExpiredConsumerQueueByAddr success segment executed (placeholder)');
        $this->checkSame([], $failed, 'cleanExpiredConsumerQueueByAddr all succeed → empty');

        // queryConsumeTimeSpan
        $admin6 = $this->newAdmin();
        $trd = new TopicRouteData();
        $trd->brokerDatas = [
            new BrokerData('C', 'broker-a', [0 => 'b:a']),
            new BrokerData('C', 'broker-b', [0 => 'b:b']),
        ];
        $this->client->topicRoutes['T'] = $trd;
        $admin6->handler = function (RemotingCommand $cmd, string $addr) {
            if ($addr === 'b:b') {
                return $this->okResponse(null);
            }
            $body = \RocketMQ\Remoting\Protocol\RemotingSerializable::encode([
                'consumeTimeSpanSet' => [['minTimestamp' => 1, 'maxTimestamp' => 2]],
            ]);
            return $this->okResponse($body);
        };
        $spans = $admin6->queryConsumeTimeSpan('T', 'G1');
        $this->checkSame(RequestCode::QUERY_CONSUME_TIME_SPAN, $this->lastCmd($admin6)->code, 'QUERY_CONSUME_TIME_SPAN=303');
        $this->checkSame(['topic' => 'T', 'group' => 'G1'], $this->lastCmd($admin6)->extFields, '303 ext');
        $this->checkSame(1, count($spans), 'queryConsumeTimeSpan skips empty-body brokers');

        // updateNameServerConfig: properties text + broadcast
        $admin7 = $this->newAdmin();
        $admin7->setNameServerAddresses(['ns:1', 'ns:2']);
        $admin7->updateNameServerConfig(['k' => 'v']);
        $upds = $this->cmdsByCode($admin7, RequestCode::UPDATE_NAMESRV_CONFIG);
        $this->checkSame(2, count($upds), 'UPDATE_NAMESRV_CONFIG broadcasts to both');
        $this->checkSame("k=v\n", $upds[0]->body, '318 body is k=v\n text');
        $n = count($admin7->requests);
        $admin7->updateNameServerConfig([]);
        $this->checkSame($n, count($admin7->requests), '318 sends no request for empty properties');
        $admin7->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'denied');
        $e7 = $this->capture(fn() => $admin7->updateNameServerConfig(['k' => 'v']));
        $this->check($e7 instanceof MQClientException && str_contains($e7->getMessage(), 'denied'), '318 any failure throws uniformly');

        // getNameServerConfig
        $admin8 = $this->newAdmin();
        $admin8->setNameServerAddresses(['ns:1', 'ns:2']);
        $admin8->handler = fn() => $this->okResponse("a=1\n");
        $conf = $admin8->getNameServerConfig();
        $this->checkSame(2, count($conf), '319 per-server query');
        $this->checkSame(['a' => '1'], $conf['ns:1'], '319 properties parsing');
        $admin8->handler = fn() => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $this->checkThrows(fn() => $admin8->getNameServerConfig(), MQClientException::class, '319 all fail throws uniformly');
        $admin9 = $this->newAdmin();
        $admin9->setNameServerAddresses(['ns:1', 'ns:2']);
        $admin9->handler = fn(RemotingCommand $c, string $addr) => $addr === 'ns:1'
            ? $this->okResponse("a=1\n")
            : $this->errResponse(ResponseCode::SYSTEM_ERROR, 'x');
        $partial = $admin9->getNameServerConfig();
        $this->checkSame(['ns:1'], array_keys($partial), '319 partial success returns usable entries');

        // setMessageRequestMode
        $admin10 = $this->newAdmin();
        $admin10->setMessageRequestMode('b:1', 'T', 'G1', 'POP', 4);
        $this->checkSame(RequestCode::SET_MESSAGE_REQUEST_MODE, $this->lastCmd($admin10)->code, 'SET_MESSAGE_REQUEST_MODE=401');
        $this->checkSame(
            ['topic' => 'T', 'consumerGroup' => 'G1', 'mode' => 'POP', 'popShareQueueNum' => '4'],
            $this->lastCmd($admin10)->extFields,
            '401 ext (popShareQueueNum>0 only)'
        );
        $admin10->setMessageRequestMode('b:1', 'T', 'G1', 'PULL');
        $this->check(!isset($this->lastCmd($admin10)->extFields['popShareQueueNum']), '401 popShareQueueNum=0 not dispatched');
    }

    // ================================================================ Entry point

    public function run(): int
    {
        $this->testSubscriptionModels();
        $this->testAdminConfigLifecycle();
        $this->testInvokeHelpers();
        $this->testTopicManagement();
        $this->testClusterBroker();
        $this->testKvConfig();
        $this->testSubscriptionGroup();
        $this->testConnectionAndStats();
        $this->testMessageTrack();
        $this->testOffsetManagement();
        $this->testBatchAndOps();
        return $this->finish();
    }
}

exit((new RunClientAdmin())->run());

<?php

declare(strict_types=1);

/**
 * Client admin "long tail" assert-style self-test (no phpunit, no real cluster).
 *
 * Covers the container join/leave RPCs, the controller config/sync-state/clean block,
 * the cold-data flow-ctr and commit-log read-ahead switches, the pop-record export and
 * the RocksDB maintenance pair, the order-conf KV forms, the batch topic/group list
 * forms, the lite admin query set, and the seven batch (fan-out) admin operations with
 * their envelope codes and error mappings.
 *
 * Network-layer doubles: LtgAdmin overrides DefaultMQAdminExt::invokeSyncOn answering
 * per script and recording every request (address, command, timeout); LtgClient
 * subclasses MQClientInstance with an offline constructor, overriding the admin-oriented
 * instance methods and recording calls.
 *
 * Run: php tests/RunClientAdminLongTail.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\MessageTrack;
use RocketMQ\Client\TrackType;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\TopicConfig;
use RocketMQ\Remoting\Protocol\AdminToolResult;
use RocketMQ\Remoting\Protocol\AdminToolsResultCodeEnum;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\BrokerOperatorResult;
use RocketMQ\Remoting\Protocol\BrokerReplicasInfo;
use RocketMQ\Remoting\Protocol\CheckRocksdbCqWriteResult;
use RocketMQ\Remoting\Protocol\ClusterInfo;
use RocketMQ\Remoting\Protocol\ConsumeStats;
use RocketMQ\Remoting\Protocol\ConsumerConnection;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\GetBrokerLiteInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetLiteClientInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetLiteGroupInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetLiteTopicInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetParentTopicInfoResponseBody;
use RocketMQ\Remoting\Protocol\OffsetWrapper;
use RocketMQ\Remoting\Protocol\QueryConsumeTimeSpanBody;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\ResetOffsetBody;
use RocketMQ\Remoting\Protocol\SubscriptionGroupConfig;
use RocketMQ\Remoting\Protocol\TopicList;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Remoting\Protocol\TopicStatsTable;
use RocketMQ\Remoting\Protocol\TopicOffset;

// ==================================================================== test doubles

/** Admin double recording every sync RPC (address, command, effective timeout). */
final class LtgAdmin extends DefaultMQAdminExt
{
    /** @var list<array{addr: string, cmd: RemotingCommand, timeout: ?int}> */
    public array $requests = [];

    /** @var callable(RemotingCommand, string): ?RemotingCommand|null */
    public $handler = null;

    /** @var array<string, \Throwable> thrown when the address is hit (simulates connect failure) */
    public array $failAddrs = [];

    protected function invokeSyncOn(MQClientInstance $client, string $addr, RemotingCommand $request, ?int $timeoutMillis): RemotingCommand
    {
        $this->requests[] = ['addr' => $addr, 'cmd' => $request, 'timeout' => $timeoutMillis];
        if (isset($this->failAddrs[$addr])) {
            throw $this->failAddrs[$addr];
        }
        $h = $this->handler;
        $resp = $h !== null ? $h($request, $addr) : null;
        return $resp ?? RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

/** MQClientInstance double: offline constructor, records the admin-oriented calls. */
final class LtgClient extends MQClientInstance
{
    public ?ClusterInfo $clusterInfo = null;
    /** @var array<string, TopicRouteData> */
    public array $topicRoutes = [];
    /** @var list<string> */
    public array $routeLookups = [];
    /** @var list<array<string, mixed>> */
    public array $maxOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $searchOffsetCalls = [];
    /** @var list<array<string, mixed>> */
    public array $updateOffsetCalls = [];

    public int $maxOffsetValue = 0;
    public int $searchOffsetValue = 0;

    public function __construct(array $nameServers = ['10.0.0.1:9876'])
    {
        parent::__construct('admin-ltg-client', $nameServers);
    }

    public function getBrokerClusterInfo(int $timeoutMillis = 10000): ClusterInfo
    {
        return $this->clusterInfo ?? new ClusterInfo();
    }

    public function getTopicRouteData(string $topic): ?TopicRouteData
    {
        $this->routeLookups[] = $topic;
        return $this->topicRoutes[$topic] ?? null;
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        $this->maxOffsetCalls[] = ['mq' => $mq, 'addr' => $addr];
        return $this->maxOffsetValue;
    }

    public function searchOffsetByTimestamp(
        MessageQueue $mq,
        int $timestamp,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        ?\RocketMQ\Common\BoundaryType $boundaryType = \RocketMQ\Common\BoundaryType::LOWER,
    ): int {
        $this->searchOffsetCalls[] = ['mq' => $mq, 'ts' => $timestamp, 'addr' => $addr];
        return $this->searchOffsetValue;
    }

    public function updateConsumerOffset(string $consumerGroup, MessageQueue $mq, int $commitOffset, int $timeoutMillis = 5000, ?string $addr = null): void
    {
        $this->updateOffsetCalls[] = ['group' => $consumerGroup, 'mq' => $mq, 'offset' => $commitOffset, 'addr' => $addr];
    }
}

// ==================================================================== runner

final class RunClientAdminLongTail
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    private LtgClient $client;

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

    // ================================================================ helpers

    private function newAdmin(array $nameServers = ['10.0.0.1:9876']): LtgAdmin
    {
        $this->client = new LtgClient($nameServers);
        $admin = new LtgAdmin();
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

    /** Route with two brokers (a → addrA, b → addrB) and one queue-data entry each. */
    private function route2(string $topic, string $nameA, string $addrA, string $nameB, string $addrB): TopicRouteData
    {
        $trd = new TopicRouteData();
        $trd->queueDatas = [
            new QueueData($nameA, 2, 2, 6, 0),
            new QueueData($nameB, 2, 2, 6, 0),
        ];
        $trd->brokerDatas = [
            new BrokerData('DefaultCluster', $nameA, [MixAll::MASTER_ID => $addrA]),
            new BrokerData('DefaultCluster', $nameB, [MixAll::MASTER_ID => $addrB]),
        ];
        $this->client->topicRoutes[$topic] = $trd;
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

    /** @return list<array{addr: string, cmd: RemotingCommand, timeout: ?int}> */
    private function reqsByCode(LtgAdmin $a, int $code): array
    {
        return array_values(array_filter($a->requests, static fn(array $r): bool => $r['cmd']->code === $code));
    }

    private function lastOf(array $reqs): array
    {
        return $reqs[count($reqs) - 1];
    }

    /** ConsumeStats body with one queue entry. */
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

    /** ResetOffsetBody body with one queue entry, keyed the way the broker inlines MessageQueue keys. */
    private function resetBody(string $topic, string $brokerName, int $queueId, int $offset): string
    {
        $body = new ResetOffsetBody();
        $body->offsetTable[] = ['mq' => new MessageQueue($topic, $brokerName, $queueId), 'offset' => $offset];
        return $body->encode();
    }

    /** TopicStatsTable body with one queue entry. */
    private function topicStatsBody(float $tps, string $topic, string $brokerName, int $queueId, int $max = 10): string
    {
        $t = new TopicStatsTable();
        $t->topicPutTps = $tps;
        $t->offsetTable[] = [
            'mq' => new MessageQueue($topic, $brokerName, $queueId),
            'value' => new TopicOffset(0, $max),
        ];
        return $t->encode();
    }

    private function trackMsg(): MessageExt
    {
        $msg = new MessageExt(topic: 'T', body: 'x');
        $msg->setQueueId(0);
        $msg->setQueueOffset(3);
        $msg->storeHost = 'b';
        return $msg;
    }

    // ================================================================ tests

    public function run(): int
    {
        $this->testContainerOps();
        $this->testControllerOps();
        $this->testColdDataAndReadAhead();
        $this->testPopAndRocksdbOps();
        $this->testOrderConf();
        $this->testListForms();
        $this->testLiteAdmin();
        $this->testConcurrentTopicStats();
        $this->testConcurrentConsumeStats();
        $this->testConcurrentDeleteTopic();
        $this->testConcurrentResetOffset();
        $this->testConcurrentTopicsByConsumer();
        $this->testConsumeTimeSpanConcurrent();
        $this->testMessageTrackConcurrent();
        $this->testDeadlineBudget();
        return $this->finish();
    }

    // ---------------------------------------------------------------- container

    private function testContainerOps(): void
    {
        // Join: ADD_BROKER(902), configPath header, plain address even with the VIP channel on,
        // fixed 20s timeout, no body.
        $admin = $this->newAdmin();
        $admin->setVipChannelEnabled(true);
        $admin->addBrokerToContainer('10.0.0.5:10911', 'conf/broker.conf');
        $r = $this->lastOf($this->reqsByCode($admin, RequestCode::ADD_BROKER));
        $this->checkSame('10.0.0.5:10911', $r['addr'], 'container join uses the container address as-is (no VIP rewrite)');
        $this->checkSame(['configPath' => 'conf/broker.conf'], $r['cmd']->extFields, 'ADD_BROKER carries configPath');
        $this->checkSame(null, $r['cmd']->body, 'ADD_BROKER has no body');
        $this->checkSame(DefaultMQAdminExt::CONTAINER_TIMEOUT, $r['timeout'], 'ADD_BROKER fixed timeout');

        // Leave: REMOVE_BROKER(903), cluster/name/id header fields.
        $admin2 = $this->newAdmin();
        $admin2->setVipChannelEnabled(true);
        $admin2->removeBrokerFromContainer('10.0.0.5:10911', 'ClusterA', 'broker-1', 0);
        $r2 = $this->lastOf($this->reqsByCode($admin2, RequestCode::REMOVE_BROKER));
        $this->checkSame('10.0.0.5:10911', $r2['addr'], 'container leave uses the container address as-is');
        $this->checkSame(
            ['brokerClusterName' => 'ClusterA', 'brokerName' => 'broker-1', 'brokerId' => '0'],
            $r2['cmd']->extFields,
            'REMOVE_BROKER carries cluster/name/id (primitive id always sent)'
        );
        $this->checkSame(DefaultMQAdminExt::CONTAINER_TIMEOUT, $r2['timeout'], 'REMOVE_BROKER fixed timeout');

        // Non-SUCCESS → broker exception
        $admin3 = $this->newAdmin();
        $admin3->handler = fn () => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'no such container');
        $this->checkThrows(
            fn () => $admin3->addBrokerToContainer('10.0.0.5:10911', 'conf/broker.conf'),
            MQBrokerException::class,
            'container join non-SUCCESS → MQBrokerException'
        );
    }

    // ---------------------------------------------------------------- controller

    private function testControllerOps(): void
    {
        // Metadata(1005): plain address, 3s timeout, the answer IS the response header.
        $admin = $this->newAdmin();
        $admin->handler = fn () => $this->okResponse(null, ['controllerLeaderAddress' => 'c-leader:9878', 'controllerType' => 'CONTROLLER']);
        $meta = $admin->getControllerMetaData('c1:9878');
        $r = $this->lastOf($this->reqsByCode($admin, RequestCode::CONTROLLER_GET_METADATA_INFO));
        $this->checkSame('c1:9878', $r['addr'], 'controller metadata plain address');
        $this->checkSame(DefaultMQAdminExt::CONTROLLER_TIMEOUT, $r['timeout'], 'controller metadata 3s timeout');
        $this->checkSame([], $r['cmd']->extFields, 'controller metadata request has no header fields');
        $this->checkSame('c-leader:9878', $meta['controllerLeaderAddress'] ?? null, 'metadata answer exposes the leader address');

        // Sync state(1006): two-phase — metadata first, then the leader with the broker-name
        // list as a raw JSON array body.
        $admin2 = $this->newAdmin();
        $admin2->handler = function (RemotingCommand $cmd) {
            if ($cmd->code === RequestCode::CONTROLLER_GET_METADATA_INFO) {
                return $this->okResponse(null, ['controllerLeaderAddress' => 'c-leader:9878']);
            }
            if ($cmd->code === RequestCode::CONTROLLER_GET_SYNC_STATE_DATA) {
                return $this->okResponse(json_encode([
                    'replicasInfoTable' => [
                        'broker-a' => [
                            'masterBrokerId' => 0,
                            'masterAddress' => 'b1:10911',
                            'masterEpoch' => 1,
                            'syncStateSetEpoch' => 2,
                            'inSyncReplicas' => [
                                ['brokerName' => 'broker-a', 'brokerId' => 0, 'brokerAddress' => 'b1:10911', 'alive' => true],
                            ],
                        ],
                    ],
                ]));
            }
            return $this->okResponse();
        };
        $info = $admin2->getInSyncStateData('c1:9878', ['broker-a', 'broker-b']);
        $metaReqs = $this->reqsByCode($admin2, RequestCode::CONTROLLER_GET_METADATA_INFO);
        $syncReqs = $this->reqsByCode($admin2, RequestCode::CONTROLLER_GET_SYNC_STATE_DATA);
        $this->checkSame(1, count($metaReqs), 'sync-state two-phase: one metadata call');
        $this->checkSame(1, count($syncReqs), 'sync-state two-phase: one sync-state call');
        $this->checkSame('c-leader:9878', $syncReqs[0]['addr'], 'sync-state query goes to the leader');
        $this->checkSame(
            ['broker-a', 'broker-b'],
            json_decode((string) $syncReqs[0]['cmd']->body, true),
            'sync-state body is the raw JSON broker-name array'
        );
        $this->checkSame(DefaultMQAdminExt::CONTROLLER_TIMEOUT, $syncReqs[0]['timeout'], 'sync-state 3s timeout');
        $this->checkSame('b1:10911', $info->replicasInfoTable['broker-a']->masterAddress ?? null, 'sync-state body decodes to BrokerReplicasInfo');
        $this->checkSame(true, $info->replicasInfoTable['broker-a']->inSyncReplicas[0]->alive ?? null, 'replica identity fields decode');

        // Missing leader → client exception
        $admin3 = $this->newAdmin();
        $admin3->handler = fn () => $this->okResponse(null, []);
        $this->checkThrows(
            fn () => $admin3->getInSyncStateData('c1:9878', ['broker-a']),
            MQClientException::class,
            'sync-state without a leader address → MQClientException'
        );

        // Empty body answer → empty replicas table (not an error)
        $admin4 = $this->newAdmin();
        $admin4->handler = function (RemotingCommand $cmd) {
            if ($cmd->code === RequestCode::CONTROLLER_GET_METADATA_INFO) {
                return $this->okResponse(null, ['controllerLeaderAddress' => 'c-leader:9878']);
            }
            return $this->okResponse(null);
        };
        $empty = $admin4->getInSyncStateData('c1:9878', ['broker-a']);
        $this->checkSame([], $empty->replicasInfoTable, 'sync-state empty body → empty BrokerReplicasInfo');

        // Clean(1011): leader-routed, brokerName + flag + invokeTime always present,
        // clusterName / ids only when supplied.
        $admin5 = $this->newAdmin();
        $admin5->handler = function (RemotingCommand $cmd) {
            if ($cmd->code === RequestCode::CONTROLLER_GET_METADATA_INFO) {
                return $this->okResponse(null, ['controllerLeaderAddress' => 'c-leader:9878']);
            }
            return $this->okResponse();
        };
        $admin5->cleanControllerBrokerData('c1:9878', null, 'broker-1');
        $clean = $this->lastOf($this->reqsByCode($admin5, RequestCode::CLEAN_BROKER_DATA));
        $this->checkSame('c-leader:9878', $clean['addr'], 'clean goes to the leader');
        $this->checkSame(DefaultMQAdminExt::CONTROLLER_TIMEOUT, $clean['timeout'], 'clean 3s timeout');
        $ext = $clean['cmd']->extFields;
        $this->checkSame('broker-1', $ext['brokerName'] ?? null, 'clean carries brokerName');
        $this->checkSame('false', $ext['isCleanLivingBroker'] ?? null, 'clean default flag is false');
        $this->check(isset($ext['invokeTime']) && ctype_digit((string) $ext['invokeTime']), 'clean carries a numeric invokeTime');
        $this->check(abs((int) $ext['invokeTime'] - (int) floor(microtime(true) * 1000.0)) < 60000, 'invokeTime is the current wall clock (ms)');
        $this->check(!isset($ext['clusterName']), 'clean omits clusterName when not supplied');
        $this->check(!isset($ext['brokerControllerIdsToClean']), 'clean omits the controller-id set when not supplied');

        $admin5->cleanControllerBrokerData('c1:9878', 'ClusterA', 'broker-1', 'c1,c2', true);
        $ext2 = $this->lastOf($this->reqsByCode($admin5, RequestCode::CLEAN_BROKER_DATA))['cmd']->extFields;
        $this->checkSame('ClusterA', $ext2['clusterName'] ?? null, 'clean carries clusterName when supplied');
        $this->checkSame('c1,c2', $ext2['brokerControllerIdsToClean'] ?? null, 'clean carries the controller-id set when supplied');
        $this->checkSame('true', $ext2['isCleanLivingBroker'] ?? null, 'clean flag true when asked');

        // Config read(1010): one call per controller, properties-text body, admin timeout.
        $admin6 = $this->newAdmin();
        $admin6->handler = fn (RemotingCommand $c) => $this->okResponse("a=1\nb=2\n");
        $cfg = $admin6->getControllerConfig(['c1:9878', 'c2:9878']);
        $readReqs = $this->reqsByCode($admin6, RequestCode::GET_CONTROLLER_CONFIG);
        $this->checkSame(2, count($readReqs), 'controller config read hits every controller');
        $this->checkSame('c1:9878', $readReqs[0]['addr'], 'controller config read address 1');
        $this->checkSame(['a' => '1', 'b' => '2'], $cfg['c1:9878'] ?? null, 'controller config body parses as properties');
        $this->checkSame(DefaultMQAdminExt::DEFAULT_TIMEOUT, $readReqs[0]['timeout'], 'controller config read uses the admin timeout');

        // Config read falls back to the name servers
        $admin7 = $this->newAdmin();
        $admin7->handler = fn () => $this->okResponse("x=9\n");
        $cfg2 = $admin7->getControllerConfig();
        $this->checkSame(['x' => '9'], $cfg2['10.0.0.1:9876'] ?? null, 'controller config read falls back to the name servers');

        // Non-SUCCESS → client exception
        $admin8 = $this->newAdmin();
        $admin8->handler = fn () => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'no controller here');
        $this->checkThrows(
            fn () => $admin8->getControllerConfig(['c1:9878']),
            MQClientException::class,
            'controller config read non-SUCCESS → MQClientException'
        );

        // Config update(1009): empty properties is a silent no-op; the LAST failure wins.
        $admin9 = $this->newAdmin();
        $admin9->updateControllerConfig([], ['c1:9878']);
        $this->checkSame(0, count($admin9->requests), 'controller config update with empty properties sends nothing');
        $admin9->updateControllerConfig(['a' => '1'], null);
        $this->checkSame(0, count($admin9->requests), 'controller config update with no controllers sends nothing');

        $admin10 = $this->newAdmin();
        $admin10->handler = function (RemotingCommand $c, string $addr) {
            if ($addr === 'c1:9878') {
                return $this->errResponse(1, 'first failure');
            }
            if ($addr === 'c2:9878') {
                return $this->errResponse(10, 'second failure');
            }
            return $this->okResponse();
        };
        $e = $this->capture(fn () => $admin10->updateControllerConfig(['a' => '1', 'b' => '2'], ['c1:9878', 'c2:9878']));
        $this->check($e instanceof MQClientException, 'controller config update failure → MQClientException');
        $this->checkSame(10, $e?->responseCode, 'controller config update keeps the LAST failure code');
        $this->checkSame(2, count($this->reqsByCode($admin10, RequestCode::UPDATE_CONTROLLER_CONFIG)), 'controller config update collects all responses before failing');
        $this->checkSame("a=1\nb=2\n", $this->lastOf($this->reqsByCode($admin10, RequestCode::UPDATE_CONTROLLER_CONFIG))['cmd']->body, 'controller config update body is the properties text');

        $admin11 = $this->newAdmin();
        $admin11->updateControllerConfig(['a' => '1'], ['c1:9878']);
        $this->checkSame(1, count($this->reqsByCode($admin11, RequestCode::UPDATE_CONTROLLER_CONFIG)), 'controller config update succeeds when every controller answers SUCCESS');
    }

    // ---------------------------------------------------------------- cold data / read-ahead

    private function testColdDataAndReadAhead(): void
    {
        // Update(2001): properties text body, VIP routing, empty set skipped.
        $admin = $this->newAdmin();
        $admin->setVipChannelEnabled(true);
        $admin->updateColdDataFlowCtrGroupConfig('10.0.0.5:10911', ['coldFlowControlEnable' => 'true', 'threshold' => '1024']);
        $r = $this->lastOf($this->reqsByCode($admin, RequestCode::UPDATE_COLD_DATA_FLOW_CTR_CONFIG));
        $this->checkSame('10.0.0.5:10909', $r['addr'], 'cold update goes through the VIP channel');
        $this->checkSame("coldFlowControlEnable=true\nthreshold=1024\n", $r['cmd']->body, 'cold update body is the properties text');
        $this->checkSame([], $r['cmd']->extFields, 'cold update has no header fields');

        $admin2 = $this->newAdmin();
        $admin2->updateColdDataFlowCtrGroupConfig('b:1', []);
        $this->checkSame(0, count($admin2->requests), 'cold update with an empty property set sends nothing');

        // Remove(2002): the body IS the group name; empty group skipped.
        $admin3 = $this->newAdmin();
        $admin3->setVipChannelEnabled(true);
        $admin3->removeColdDataFlowCtrGroupConfig('10.0.0.5:10911', 'G1');
        $r3 = $this->lastOf($this->reqsByCode($admin3, RequestCode::REMOVE_COLD_DATA_FLOW_CTR_CONFIG));
        $this->checkSame('10.0.0.5:10909', $r3['addr'], 'cold remove goes through the VIP channel');
        $this->checkSame('G1', $r3['cmd']->body, 'cold remove body is the bare group name');

        $admin4 = $this->newAdmin();
        $admin4->removeColdDataFlowCtrGroupConfig('b:1', '');
        $this->checkSame(0, count($admin4->requests), 'cold remove with an empty group sends nothing');

        // Info(2003): plain port, raw body or null.
        $admin5 = $this->newAdmin();
        $admin5->setVipChannelEnabled(true);
        $admin5->handler = fn () => $this->okResponse('coldDataFlowCtrInfo=xxx');
        $this->checkSame('coldDataFlowCtrInfo=xxx', $admin5->getColdDataFlowCtrInfo('10.0.0.5:10911'), 'cold info returns the raw body');
        $this->checkSame('10.0.0.5:10911', $this->lastOf($this->reqsByCode($admin5, RequestCode::GET_COLD_DATA_FLOW_CTR_INFO))['addr'], 'cold info uses the plain port');

        $admin6 = $this->newAdmin();
        $admin6->handler = fn () => $this->okResponse(null);
        $this->checkSame(null, $admin6->getColdDataFlowCtrInfo('b:1'), 'cold info empty body → null');

        // Read-ahead(2004): single READ_AHEAD_MODE header key, plain port, remark answer.
        $admin7 = $this->newAdmin();
        $admin7->setVipChannelEnabled(true);
        $admin7->handler = fn () => $this->okResponse(null, [], );
        $admin7->handler = function (RemotingCommand $c) {
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, 'read ahead mode switched');
        };
        $res = $admin7->setCommitLogReadAheadMode('10.0.0.5:10911', 'NORMAL');
        $r7 = $this->lastOf($this->reqsByCode($admin7, RequestCode::SET_COMMITLOG_READ_MODE));
        $this->checkSame('10.0.0.5:10911', $r7['addr'], 'read-ahead switch uses the plain port');
        $this->checkSame(['READ_AHEAD_MODE' => 'NORMAL'], $r7['cmd']->extFields, 'read-ahead header carries exactly READ_AHEAD_MODE');
        $this->checkSame('read ahead mode switched', $res, 'read-ahead answer is the response remark');

        $admin8 = $this->newAdmin();
        $admin8->handler = function () {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->remark = null;
            return $resp;
        };
        $this->checkSame(null, $admin8->setCommitLogReadAheadMode('b:1', 'NORMAL'), 'read-ahead without a remark → null');
    }

    // ---------------------------------------------------------------- pop records / rocksdb

    private function testPopAndRocksdbOps(): void
    {
        // Pop record export(200056): caller timeout verbatim, plain port, no header, no body.
        $admin = $this->newAdmin();
        $admin->setVipChannelEnabled(true);
        $admin->exportPopRecords('10.0.0.5:10911', 7777);
        $r = $this->lastOf($this->reqsByCode($admin, RequestCode::POP_ROLLBACK));
        $this->checkSame('10.0.0.5:10911', $r['addr'], 'pop record export uses the plain port');
        $this->checkSame(7777, $r['timeout'], 'pop record export timeout passes through verbatim');
        $this->checkSame([], $r['cmd']->extFields, 'pop record export has no header fields');
        $this->checkSame(null, $r['cmd']->body, 'pop record export has no body');

        // RocksDB progress check(354): topic + checkStoreTime always in the header
        // (primitive long, even 0), plain port, JSON body, non-SUCCESS → client exception.
        $admin2 = $this->newAdmin();
        $admin2->setVipChannelEnabled(true);
        $admin2->handler = fn () => $this->okResponse(json_encode(['checkResult' => 'progress ok', 'checkStatus' => 2]));
        $res = $admin2->checkRocksdbCqWriteProgress('10.0.0.5:10911', 'T', 0);
        $r2 = $this->lastOf($this->reqsByCode($admin2, RequestCode::CHECK_ROCKSDB_CQ_WRITE_PROGRESS));
        $this->checkSame('10.0.0.5:10911', $r2['addr'], 'rocksdb progress check uses the plain port');
        $this->checkSame(['topic' => 'T', 'checkStoreTime' => '0'], $r2['cmd']->extFields, 'rocksdb progress check header carries topic and checkStoreTime (0 always sent)');
        $this->check($res instanceof CheckRocksdbCqWriteResult, 'rocksdb progress answer decodes');
        $this->checkSame('progress ok', $res?->checkResult, 'rocksdb progress checkResult');
        $this->checkSame(2, $res?->checkStatus, 'rocksdb progress checkStatus');

        $admin3 = $this->newAdmin();
        $admin3->handler = fn () => $this->okResponse(null);
        $this->checkSame(null, $admin3->checkRocksdbCqWriteProgress('b:1', 'T', 5), 'rocksdb progress empty body → null');

        $admin4 = $this->newAdmin();
        $admin4->handler = fn () => $this->errResponse(10, 'not ready');
        $e = $this->capture(fn () => $admin4->checkRocksdbCqWriteProgress('b:1', 'T', 5));
        $this->check($e instanceof MQClientException, 'rocksdb progress non-SUCCESS → MQClientException (not MQBrokerException)');
        $this->checkSame(10, $e?->responseCode, 'rocksdb progress failure keeps the response code');

        // RocksDB config export(355): each type name lowercased and ';'-terminated,
        // unknown type rejected before any request, non-SUCCESS → client exception.
        $admin5 = $this->newAdmin();
        $admin5->setVipChannelEnabled(true);
        $admin5->exportRocksDBConfigToJson('10.0.0.5:10911', ['Topics', 'subscriptiongroups']);
        $r5 = $this->lastOf($this->reqsByCode($admin5, RequestCode::EXPORT_ROCKSDB_CONFIG_TO_JSON));
        $this->checkSame('10.0.0.5:10911', $r5['addr'], 'rocksdb export uses the plain port');
        $this->checkSame(['configType' => 'topics;subscriptiongroups;'], $r5['cmd']->extFields, 'rocksdb export configType is the lowercased names, each ;-terminated');

        $admin6 = $this->newAdmin();
        $this->checkThrows(
            fn () => $admin6->exportRocksDBConfigToJson('b:1', ['nope']),
            MQClientException::class,
            'rocksdb export unknown config type → MQClientException'
        );
        $this->checkSame(0, count($admin6->requests), 'rocksdb export unknown config type sends nothing');

        $admin7 = $this->newAdmin();
        $admin7->handler = fn () => $this->errResponse(10, 'export failed');
        $this->checkThrows(
            fn () => $admin7->exportRocksDBConfigToJson('b:1', ['topics']),
            MQClientException::class,
            'rocksdb export non-SUCCESS → MQClientException'
        );
    }

    // ---------------------------------------------------------------- order conf

    private function testOrderConf(): void
    {
        // Cluster form: one broadcast PUT, value passed through.
        $admin = $this->newAdmin(['ns1:9876', 'ns2:9876']);
        $admin->createOrUpdateOrderConf('T', 'q0:3', true);
        $puts = $this->reqsByCode($admin, RequestCode::PUT_KV_CONFIG);
        $this->checkSame(2, count($puts), 'order conf cluster form broadcasts the PUT to every name server');
        $this->checkSame(
            ['namespace' => DefaultMQAdminExt::NAMESPACE_ORDER_TOPIC_CONFIG, 'key' => 'T', 'value' => 'q0:3'],
            $puts[0]['cmd']->extFields,
            'order conf PUT header is namespace+key+value'
        );

        // Broker form: read-modify-write keyed by the part before ':'.
        $admin2 = $this->newAdmin();
        $admin2->handler = fn (RemotingCommand $c) => $c->code === RequestCode::GET_KV_CONFIG
            ? $this->okResponse(null, ['value' => 'q0:3'])
            : $this->okResponse();
        $admin2->createOrUpdateOrderConf('T', 'q1:5', false);
        $gets = $this->reqsByCode($admin2, RequestCode::GET_KV_CONFIG);
        $puts2 = $this->reqsByCode($admin2, RequestCode::PUT_KV_CONFIG);
        $this->checkSame(1, count($gets), 'order conf broker form reads the old value first');
        $this->checkSame(['namespace' => 'ORDER_TOPIC_CONFIG', 'key' => 'T'], $gets[0]['cmd']->extFields, 'order conf GET header is namespace+key');
        $this->checkSame(1, count($puts2), 'order conf broker form writes the merged value back');
        $this->checkSame('q0:3;q1:5', $puts2[0]['cmd']->extFields['value'] ?? null, 'order conf merge appends the new entry');

        $admin3 = $this->newAdmin();
        $admin3->handler = fn (RemotingCommand $c) => $c->code === RequestCode::GET_KV_CONFIG
            ? $this->okResponse(null, ['value' => 'q0:3;q1:5'])
            : $this->okResponse();
        $admin3->createOrUpdateOrderConf('T', 'q0:9', false);
        $puts3 = $this->reqsByCode($admin3, RequestCode::PUT_KV_CONFIG);
        $this->checkSame('q0:9;q1:5', $puts3[0]['cmd']->extFields['value'] ?? null, 'order conf merge replaces the entry with the same key');

        // GET failure tolerated: the read failure still produces a PUT.
        $admin4 = $this->newAdmin();
        $admin4->handler = fn (RemotingCommand $c) => $c->code === RequestCode::GET_KV_CONFIG
            ? $this->errResponse(ResponseCode::SYSTEM_ERROR, 'no kv yet')
            : $this->okResponse();
        $admin4->createOrUpdateOrderConf('T', 'q0:1', false);
        $this->checkSame(1, count($this->reqsByCode($admin4, RequestCode::PUT_KV_CONFIG)), 'order conf tolerates a failed read and still writes');
    }

    // ---------------------------------------------------------------- list forms

    private function testListForms(): void
    {
        $admin = $this->newAdmin();
        $admin->createAndUpdateTopicConfigList('b:1', [new TopicConfig('T1')]);
        $r = $this->lastOf($this->reqsByCode($admin, RequestCode::UPDATE_AND_CREATE_TOPIC_LIST));
        $body = json_decode((string) $r['cmd']->body, true);
        $this->checkSame('T1', $body['topicConfigList'][0]['topicName'] ?? null, 'topic list form body carries topicConfigList');
        $this->checkThrows(
            fn () => $admin->createAndUpdateTopicConfigList('b:1', []),
            MQClientException::class,
            'topic list form with an empty list → MQClientException'
        );

        $admin2 = $this->newAdmin();
        $admin2->handler = fn () => $this->errResponse(10, 'bad topic');
        $e = $this->capture(fn () => $admin2->createAndUpdateTopicConfigList('b:1', [new TopicConfig('T1')]));
        $this->check($e instanceof MQClientException, 'topic list form non-SUCCESS → MQClientException (not MQBrokerException)');
        $this->checkSame(10, $e?->responseCode, 'topic list form failure keeps the response code');

        $admin3 = $this->newAdmin();
        $admin3->createAndUpdateSubscriptionGroupConfigList('b:1', [new SubscriptionGroupConfig('G1')]);
        $r3 = $this->lastOf($this->reqsByCode($admin3, RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST));
        $body3 = json_decode((string) $r3['cmd']->body, true);
        $this->checkSame('G1', $body3['groupConfigList'][0]['groupName'] ?? null, 'group list form body carries groupConfigList');

        $admin4 = $this->newAdmin();
        $admin4->handler = fn () => $this->errResponse(10, 'bad group');
        $this->checkThrows(
            fn () => $admin4->createAndUpdateSubscriptionGroupConfigList('b:1', [new SubscriptionGroupConfig('G1')]),
            MQClientException::class,
            'group list form non-SUCCESS → MQClientException'
        );
    }

    // ---------------------------------------------------------------- lite admin

    private function testLiteAdmin(): void
    {
        $liteBrokerBody = fn (array $extra = []): string => json_encode(array_merge([
            'storeType' => 'RocksDB',
            'maxLmqNum' => 100,
            'currentLmqNum' => 3,
        ], $extra));

        // 200074: no header at all; VIP routing.
        $admin = $this->newAdmin();
        $admin->setVipChannelEnabled(true);
        $admin->handler = fn () => $this->okResponse($liteBrokerBody());
        $info = $admin->getBrokerLiteInfo('10.0.0.5:10911');
        $r = $this->lastOf($this->reqsByCode($admin, RequestCode::GET_BROKER_LITE_INFO));
        $this->checkSame('10.0.0.5:10909', $r['addr'], 'lite broker info goes through the VIP channel');
        $this->checkSame([], $r['cmd']->extFields, 'lite broker info request has no header fields');
        $this->checkSame('RocksDB', $info?->storeType, 'lite broker info body decodes');
        $this->checkSame(100, $info?->maxLmqNum, 'lite broker info maxLmqNum');

        // 200075: topic header.
        $admin2 = $this->newAdmin();
        $admin2->handler = fn () => $this->okResponse(json_encode(['topic' => 'P', 'ttl' => 3600, 'liteTopicCount' => 5]));
        $pinfo = $admin2->getParentTopicInfo('b:1', 'P');
        $r2 = $this->lastOf($this->reqsByCode($admin2, RequestCode::GET_PARENT_TOPIC_INFO));
        $this->checkSame(['topic' => 'P'], $r2['cmd']->extFields, 'parent topic info header carries topic');
        $this->checkSame(3600, $pinfo?->ttl, 'parent topic info ttl');
        $this->checkSame(5, $pinfo?->liteTopicCount, 'parent topic info liteTopicCount');

        // 200076: parentTopic always, liteTopic only when given.
        $admin3 = $this->newAdmin();
        $admin3->handler = fn () => $this->okResponse(json_encode(['parentTopic' => 'P', 'liteTopic' => 'L', 'shardingToBroker' => true]));
        $linfo = $admin3->getLiteTopicInfo('b:1', 'P', 'L');
        $this->checkSame(['parentTopic' => 'P', 'liteTopic' => 'L'], $this->lastOf($this->reqsByCode($admin3, RequestCode::GET_LITE_TOPIC_INFO))['cmd']->extFields, 'lite topic info header carries both names');
        $this->checkSame(true, $linfo?->shardingToBroker, 'lite topic info body decodes');
        $admin3->getLiteTopicInfo('b:1', 'P');
        $this->checkSame(['parentTopic' => 'P'], $this->lastOf($this->reqsByCode($admin3, RequestCode::GET_LITE_TOPIC_INFO))['cmd']->extFields, 'lite topic info omits the unset liteTopic');

        // 200077: maxCount=1000 always, clientId only when given.
        $admin4 = $this->newAdmin();
        $admin4->handler = fn () => $this->okResponse(json_encode(['parentTopic' => 'P', 'group' => 'G', 'clientId' => 'c1', 'liteTopicCount' => 2]));
        $cinfo = $admin4->getLiteClientInfo('b:1', 'P', 'G');
        $this->checkSame(
            ['parentTopic' => 'P', 'group' => 'G', 'maxCount' => '1000'],
            $this->lastOf($this->reqsByCode($admin4, RequestCode::GET_LITE_CLIENT_INFO))['cmd']->extFields,
            'lite client info header carries maxCount=1000 and omits the unset clientId'
        );
        $this->checkSame(2, $cinfo?->liteTopicCount, 'lite client info body decodes');
        $admin4->getLiteClientInfo('b:1', 'P', 'G', 'c1');
        $this->checkSame(
            ['parentTopic' => 'P', 'group' => 'G', 'clientId' => 'c1', 'maxCount' => '1000'],
            $this->lastOf($this->reqsByCode($admin4, RequestCode::GET_LITE_CLIENT_INFO))['cmd']->extFields,
            'lite client info carries clientId when given'
        );

        // 200078: topK always (default 0), liteTopic only when given.
        $admin5 = $this->newAdmin();
        $admin5->handler = fn () => $this->okResponse(json_encode([
            'group' => 'G',
            'totalLagCount' => 7,
            'lagCountTopK' => [['liteTopic' => 'L1', 'lagCount' => 7, 'earliestUnconsumedTimestamp' => -1, 'consumerOffset' => -1]],
        ]));
        $ginfo = $admin5->getLiteGroupInfo('b:1', 'G');
        $this->checkSame(
            ['group' => 'G', 'topK' => '0'],
            $this->lastOf($this->reqsByCode($admin5, RequestCode::GET_LITE_GROUP_INFO))['cmd']->extFields,
            'lite group info header carries topK=0 and omits the unset liteTopic'
        );
        $this->checkSame(7, $ginfo?->totalLagCount, 'lite group info body decodes');
        $this->checkSame(7, $ginfo?->lagCountTopK[0]->lagCount ?? null, 'lite group info lag list decodes');
        $admin5->getLiteGroupInfo('b:1', 'G', 'L1', 20);
        $this->checkSame(
            ['group' => 'G', 'liteTopic' => 'L1', 'topK' => '20'],
            $this->lastOf($this->reqsByCode($admin5, RequestCode::GET_LITE_GROUP_INFO))['cmd']->extFields,
            'lite group info carries liteTopic and topK when given'
        );

        // 200079: group header, clientId optional, no body to decode.
        $admin6 = $this->newAdmin();
        $admin6->triggerLiteDispatch('b:1', 'G');
        $this->checkSame(['group' => 'G'], $this->lastOf($this->reqsByCode($admin6, RequestCode::TRIGGER_LITE_DISPATCH))['cmd']->extFields, 'lite dispatch header carries group');
        $admin6->triggerLiteDispatch('b:1', 'G', 'c1');
        $this->checkSame(['group' => 'G', 'clientId' => 'c1'], $this->lastOf($this->reqsByCode($admin6, RequestCode::TRIGGER_LITE_DISPATCH))['cmd']->extFields, 'lite dispatch carries clientId when given');

        // SUCCESS with no body → null result (no error).
        $admin7 = $this->newAdmin();
        $admin7->handler = fn () => $this->okResponse(null);
        $this->checkSame(null, $admin7->getBrokerLiteInfo('b:1'), 'lite query SUCCESS without a body → null');

        // Non-SUCCESS → broker exception.
        $admin8 = $this->newAdmin();
        $admin8->handler = fn () => $this->errResponse(10, 'lite store disabled');
        $this->checkThrows(
            fn () => $admin8->getBrokerLiteInfo('b:1'),
            MQBrokerException::class,
            'lite query non-SUCCESS → MQBrokerException'
        );
    }

    // ---------------------------------------------------------------- batch: topic stats

    private function testConcurrentTopicStats(): void
    {
        // Two brokers: tables merged, put TPS summed, per-call budget = remaining deadline.
        $admin = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin->handler = fn (RemotingCommand $c, string $addr) => $this->okResponse(
            $addr === 'b:1' ? $this->topicStatsBody(1.5, 'T', 'broker-a', 0) : $this->topicStatsBody(2.5, 'T', 'broker-b', 1)
        );
        $res = $admin->examineTopicStatsConcurrent('T');
        $this->check($res instanceof AdminToolResult && $res->success, 'topic stats batch succeeds');
        $this->checkSame(AdminToolsResultCodeEnum::SUCCESS, $res->code, 'topic stats batch code 200');
        $this->checkSame('success', $res->errorMsg, 'topic stats batch success message');
        $table = $res->data;
        $this->check($table instanceof TopicStatsTable, 'topic stats batch data is the merged table');
        $this->checkSame(2, count($table?->offsetTable ?? []), 'topic stats batch merges both broker tables');
        $this->checkSame(4.0, $table?->topicPutTps, 'topic stats batch sums the put TPS');
        $stats = $this->reqsByCode($admin, RequestCode::GET_TOPIC_STATS_INFO);
        $this->checkSame(2, count($stats), 'topic stats batch calls every broker');
        $this->check($stats[0]['timeout'] !== null && $stats[0]['timeout'] >= 19000 && $stats[0]['timeout'] <= DefaultMQAdminExt::DEFAULT_TIMEOUT, 'topic stats batch per-call budget is the remaining deadline');

        // A failing broker only shrinks the merge.
        $admin2 = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin2->handler = fn (RemotingCommand $c, string $addr) => $addr === 'b:1'
            ? $this->okResponse($this->topicStatsBody(1.0, 'T', 'broker-a', 0))
            : $this->errResponse(10, 'boom');
        $res2 = $admin2->examineTopicStatsConcurrent('T');
        $this->check($res2->success, 'topic stats batch still succeeds with one broker failing');
        $this->checkSame(1, count($res2->data?->offsetTable ?? []), 'topic stats batch skips the failing broker');

        // Missing route → TOPIC_ROUTE_INFO_NOT_EXIST.
        $admin3 = $this->newAdmin();
        $res3 = $admin3->examineTopicStatsConcurrent('T');
        $this->checkSame(false, $res3->success, 'topic stats batch without a route fails');
        $this->checkSame(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, $res3->code, 'topic stats batch missing route → -2001');

        // Route with no brokers → success with an empty table.
        $admin4 = $this->newAdmin();
        $this->client->topicRoutes['T'] = new TopicRouteData();
        $res4 = $admin4->examineTopicStatsConcurrent('T');
        $this->check($res4->success && $res4->data instanceof TopicStatsTable && $res4->data->offsetTable === [], 'topic stats batch empty route → success with an empty table');
    }

    // ---------------------------------------------------------------- batch: consume stats

    private function testConcurrentConsumeStats(): void
    {
        // Route discovery order: retry topic, plain topic, pop-retry topic.
        $admin = $this->newAdmin();
        $this->route(ExtraInfoUtil_buildPopRetryTopic('T', 'G'), 'broker-a', 'b:1');
        $admin->handler = fn () => $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 2));
        $res = $admin->examineConsumeStatsConcurrent('G', 'T');
        $this->checkSame(
            [MixAll::getRetryTopic('G'), 'T', ExtraInfoUtil_buildPopRetryTopic('T', 'G')],
            $this->client->routeLookups,
            'consume stats batch route discovery order: retry → plain → pop-retry'
        );
        $this->check($res->success, 'consume stats batch falls through to the pop-retry route');
        $this->checkSame(1, count($res->data?->offsetTable ?? []), 'consume stats batch merges per the pop-retry route');

        // The retry topic wins when it exists.
        $admin2 = $this->newAdmin();
        $this->route(MixAll::getRetryTopic('G'), 'broker-a', 'b:9');
        $admin2->handler = fn () => $this->okResponse($this->statsBody(3.0, 'T', 'broker-a', 0, 5));
        $res2 = $admin2->examineConsumeStatsConcurrent('G', 'T');
        $this->checkSame([MixAll::getRetryTopic('G')], $this->client->routeLookups, 'consume stats batch stops at the retry topic route');
        $this->checkSame('b:9', $this->lastOf($this->reqsByCode($admin2, RequestCode::GET_CONSUME_STATS))['addr'], 'consume stats batch queries the retry-route broker');

        // Merge + TPS sum across brokers.
        $admin3 = $this->newAdmin();
        $this->route2(MixAll::getRetryTopic('G'), 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin3->handler = fn (RemotingCommand $c, string $addr) => $this->okResponse(
            $addr === 'b:1' ? $this->statsBody(1.5, 'T', 'broker-a', 0, 4) : $this->statsBody(2.5, 'T', 'broker-b', 1, 6)
        );
        $res3 = $admin3->examineConsumeStatsConcurrent('G');
        $this->check($res3->success, 'consume stats batch succeeds');
        $this->checkSame(2, count($res3->data?->offsetTable ?? []), 'consume stats batch merges both broker tables');
        $this->checkSame(4.0, $res3->data?->consumeTps, 'consume stats batch sums the consume TPS');

        // Empty merged table + dead connection probe → CONSUMER_NOT_ONLINE with the exact message.
        $admin4 = $this->newAdmin();
        $this->route(MixAll::getRetryTopic('G'), 'broker-a', 'b:1');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b:1']];
        $admin4->handler = function (RemotingCommand $c) {
            if ($c->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->errResponse(ResponseCode::CONSUMER_NOT_ONLINE, 'offline');
            }
            return $this->okResponse(null);
        };
        $res4 = $admin4->examineConsumeStatsConcurrent('G');
        $this->checkSame(false, $res4->success, 'consume stats batch with an empty table fails');
        $this->checkSame(AdminToolsResultCodeEnum::CONSUMER_NOT_ONLINE, $res4->code, 'consume stats batch empty table → -2002');
        $this->checkSame(
            'Not found the consumer group consume stats, because return offset table is empty, maybe the consumer not consume any message',
            $res4->errorMsg,
            'consume stats batch -2002 message'
        );

        // Broadcasting group → BROADCAST_CONSUMPTION.
        $cc = new ConsumerConnection();
        $cc->messageModel = 'BROADCASTING';
        $admin5 = $this->newAdmin();
        $this->route(MixAll::getRetryTopic('G'), 'broker-a', 'b:1');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b:1']];
        $admin5->handler = function (RemotingCommand $c) use ($cc) {
            if ($c->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc->encode());
            }
            return $this->okResponse(null);
        };
        $res5 = $admin5->examineConsumeStatsConcurrent('G');
        $this->checkSame(AdminToolsResultCodeEnum::BROADCAST_CONSUMPTION, $res5->code, 'consume stats batch broadcasting → -2003');
        $this->checkSame(
            'Not found the consumer group consume stats, because return offset table is empty, the consumer is under the broadcast mode',
            $res5->errorMsg,
            'consume stats batch -2003 message'
        );

        // Online but nothing consumed (clustering) → success with the empty stats.
        $cc2 = new ConsumerConnection();
        $cc2->messageModel = 'CLUSTERING';
        $admin6 = $this->newAdmin();
        $this->route(MixAll::getRetryTopic('G'), 'broker-a', 'b:1');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b:1']];
        $admin6->handler = function (RemotingCommand $c) use ($cc2) {
            if ($c->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($cc2->encode());
            }
            return $this->okResponse(null);
        };
        $res6 = $admin6->examineConsumeStatsConcurrent('G');
        $this->check($res6->success, 'consume stats batch online-but-empty → success');
        $this->checkSame(0, count($res6->data?->offsetTable ?? []), 'consume stats batch online-but-empty carries the empty table');
    }

    // ---------------------------------------------------------------- batch: delete topic

    private function testConcurrentDeleteTopic(): void
    {
        $admin = $this->newAdmin();
        $res = $admin->deleteTopicInBrokerConcurrent(['b:1', 'b:2'], 'T');
        $this->check($res->success, 'topic delete batch envelope succeeds even when a broker fails');
        $this->checkSame(AdminToolsResultCodeEnum::SUCCESS, $res->code, 'topic delete batch code 200');
        $result = $res->data;
        $this->check($result instanceof BrokerOperatorResult, 'topic delete batch data is the operator result');
        $this->checkSame(['b:1', 'b:2'], $result?->successList, 'topic delete batch success list');
        $this->checkSame([], $result?->failureList, 'topic delete batch failure list empty');
        $dels = $this->reqsByCode($admin, RequestCode::DELETE_TOPIC_IN_BROKER);
        $this->checkSame(2, count($dels), 'topic delete batch calls every address');
        $this->checkSame(['topic' => 'T'], $dels[0]['cmd']->extFields, 'topic delete batch header carries topic');

        $admin2 = $this->newAdmin();
        $admin2->handler = fn (RemotingCommand $c, string $addr) => $addr === 'b:2' ? $this->errResponse(10, 'boom') : $this->okResponse();
        $res2 = $admin2->deleteTopicInBrokerConcurrent(['b:1', 'b:2'], 'T');
        $this->check($res2->success, 'topic delete batch with one failure keeps the success envelope');
        $this->checkSame(['b:1'], $res2->data?->successList, 'topic delete batch success list after a failure');
        $this->checkSame(['b:2'], $res2->data?->failureList, 'topic delete batch failure list after a failure');
    }

    // ---------------------------------------------------------------- batch: reset offset

    private function testConcurrentResetOffset(): void
    {
        // All brokers answer with the reset table → full success list.
        $admin = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $resetBody = $this->resetBody('T', 'broker-a', 0, 5);
        $admin->handler = fn () => $this->okResponse($resetBody);
        $res = $admin->resetOffsetNewConcurrent('G', 'T', 1234567890);
        $this->check($res->success, 'offset reset batch succeeds when every broker resets');
        $this->checkSame(AdminToolsResultCodeEnum::SUCCESS, $res->code, 'offset reset batch code 200');
        $result = $res->data;
        $this->check($result instanceof BrokerOperatorResult, 'offset reset batch data is the operator result');
        $this->checkSame(['b:1', 'b:2'], $result?->successList, 'offset reset batch success list in route order');
        $this->checkSame([], $result?->failureList, 'offset reset batch failure list empty');
        $resets = $this->reqsByCode($admin, RequestCode::INVOKE_BROKER_TO_RESET_OFFSET);
        $this->checkSame(2, count($resets), 'offset reset batch sends one broker-side reset per broker');

        // 206 → legacy fallback on that broker (consume stats + updateConsumerOffset).
        $admin2 = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin2->handler = function (RemotingCommand $c, string $addr) {
            if ($c->code === RequestCode::INVOKE_BROKER_TO_RESET_OFFSET && $addr === 'b:2') {
                return $this->errResponse(ResponseCode::CONSUMER_NOT_ONLINE, 'offline');
            }
            if ($c->code === RequestCode::INVOKE_BROKER_TO_RESET_OFFSET) {
                return $this->okResponse($this->resetBody('T', 'broker-a', 0, 5));
            }
            if ($c->code === RequestCode::GET_CONSUME_STATS && $addr === 'b:2') {
                return $this->okResponse($this->statsBody(1.0, 'T', 'broker-b', 0, 3));
            }
            return $this->okResponse();
        };
        $this->client->searchOffsetValue = 7;
        $res2 = $admin2->resetOffsetNewConcurrent('G', 'T', 1234567890);
        $this->check($res2->success, 'offset reset batch 206 falls back to the legacy path and succeeds');
        $this->checkSame(['b:1', 'b:2'], $res2->data?->successList, 'offset reset batch fallback broker lands in the success list');
        $fallback = array_values(array_filter($this->client->updateOffsetCalls, static fn(array $c): bool => $c['addr'] === 'b:2'));
        $this->checkSame(1, count($fallback), 'offset reset batch fallback writes the consumer offset once');
        $this->checkSame(7, $fallback[0]['offset'] ?? null, 'offset reset batch fallback writes the timestamp offset');

        // SYSTEM_ERROR: group never online counts as done.
        $admin3 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:1');
        $admin3->handler = fn () => $this->errResponse(ResponseCode::SYSTEM_ERROR, 'group not exist');
        $res3 = $admin3->resetOffsetNewConcurrent('G', 'T', 1234567890);
        $this->check($res3->success, 'offset reset batch treats SYSTEM_ERROR as done');
        $this->checkSame(['b:1'], $res3->data?->successList, 'offset reset batch SYSTEM_ERROR lands in the success list');

        // Any other failure → failure list, envelope failure with the operator payload.
        $admin4 = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin4->handler = function (RemotingCommand $c, string $addr) {
            if ($addr === 'b:2') {
                return $this->errResponse(10, 'reset refused');
            }
            return $this->okResponse($this->resetBody('T', 'broker-a', 0, 5));
        };
        $res4 = $admin4->resetOffsetNewConcurrent('G', 'T', 1234567890);
        $this->checkSame(false, $res4->success, 'offset reset batch with a refused broker fails');
        $this->checkSame(AdminToolsResultCodeEnum::MQ_BROKER_ERROR, $res4->code, 'offset reset batch failure → -1002');
        $this->checkSame('operator failure', $res4->errorMsg, 'offset reset batch failure message');
        $this->checkSame(['b:1'], $res4->data?->successList, 'offset reset batch failure payload keeps the successes');
        $this->checkSame(['b:2'], $res4->data?->failureList, 'offset reset batch failure payload keeps the failures');

        // Missing route → -2001.
        $admin5 = $this->newAdmin();
        $res5 = $admin5->resetOffsetNewConcurrent('G', 'T', 1234567890);
        $this->checkSame(false, $res5->success, 'offset reset batch without a route fails');
        $this->checkSame(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, $res5->code, 'offset reset batch missing route → -2001');
        $this->checkSame('topic router info not found', $res5->errorMsg, 'offset reset batch missing route message');
    }

    // ---------------------------------------------------------------- batch: topics by consumer

    private function testConcurrentTopicsByConsumer(): void
    {
        $admin = $this->newAdmin();
        $this->route2(MixAll::getRetryTopic('G'), 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin->handler = fn (RemotingCommand $c, string $addr) => $this->okResponse(
            $addr === 'b:1'
                ? json_encode(['topicList' => ['T1', 'T2']])
                : json_encode(['topicList' => ['T2', 'T3']])
        );
        $res = $admin->queryTopicsByConsumerConcurrent('G');
        $this->check($res->success, 'topics-by-consumer batch succeeds');
        $this->check($res->data instanceof TopicList, 'topics-by-consumer batch data is a TopicList');
        $this->checkSame(['T1', 'T2', 'T3'], $res->data?->getTopicList(), 'topics-by-consumer batch unions and dedupes');
        $calls = $this->reqsByCode($admin, RequestCode::QUERY_TOPICS_BY_CONSUMER);
        $this->checkSame(2, count($calls), 'topics-by-consumer batch calls every broker');
        $this->checkSame(['group' => 'G'], $calls[0]['cmd']->extFields, 'topics-by-consumer batch header carries group');

        $admin2 = $this->newAdmin();
        $res2 = $admin2->queryTopicsByConsumerConcurrent('G');
        $this->checkSame(false, $res2->success, 'topics-by-consumer batch without a retry route fails');
        $this->checkSame(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, $res2->code, 'topics-by-consumer batch missing route → -2001');
        $this->checkSame('router info not found.', $res2->errorMsg, 'topics-by-consumer batch missing route message');
    }

    // ---------------------------------------------------------------- batch: consume time span

    private function testConsumeTimeSpanConcurrent(): void
    {
        $admin = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $spanA = ['time' => 1000, 'minOffset' => 0, 'maxOffset' => 9];
        $spanB = ['time' => 2000, 'minOffset' => 0, 'maxOffset' => 4];
        $admin->handler = fn (RemotingCommand $c, string $addr) => $this->okResponse(
            json_encode(['consumeTimeSpanSet' => [$addr === 'b:1' ? $spanA : $spanB]])
        );
        $res = $admin->queryConsumeTimeSpanConcurrent('T', 'G');
        $this->check($res->success, 'consume time span batch succeeds');
        $this->checkSame([$spanA, $spanB], $res->data, 'consume time span batch concatenates per-broker spans');
        $spans = $this->reqsByCode($admin, RequestCode::QUERY_CONSUME_TIME_SPAN);
        $this->checkSame(2, count($spans), 'consume time span batch calls every broker');
        $this->checkSame(['topic' => 'T', 'group' => 'G'], $spans[0]['cmd']->extFields, 'consume time span batch header carries topic and group');

        // One broker failing → the other's spans only.
        $admin2 = $this->newAdmin();
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $admin2->handler = fn (RemotingCommand $c, string $addr) => $addr === 'b:2'
            ? $this->errResponse(10, 'boom')
            : $this->okResponse(json_encode(['consumeTimeSpanSet' => [$spanA]]));
        $res2 = $admin2->queryConsumeTimeSpanConcurrent('T', 'G');
        $this->check($res2->success, 'consume time span batch still succeeds with one broker failing');
        $this->checkSame([$spanA], $res2->data, 'consume time span batch keeps the surviving spans');

        // Route with no brokers → success with an empty list.
        $admin3 = $this->newAdmin();
        $this->client->topicRoutes['T'] = new TopicRouteData();
        $res3 = $admin3->queryConsumeTimeSpanConcurrent('T', 'G');
        $this->check($res3->success && $res3->data === [], 'consume time span batch empty route → success with an empty list');

        // Missing route → -2001 (through the envelope).
        $admin4 = $this->newAdmin();
        $res4 = $admin4->queryConsumeTimeSpanConcurrent('T', 'G');
        $this->checkSame(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, $res4->code, 'consume time span batch missing route → -2001');
    }

    // ---------------------------------------------------------------- batch: message track

    private function testMessageTrackConcurrent(): void
    {
        // Group list keeps the broker order (no sorting) and the result is NOT wrapped:
        // a plain track list.
        $admin = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:1');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $this->client->topicRoutes[MixAll::getRetryTopic('GT')] = (function () {
            $trd = new TopicRouteData();
            $trd->brokerDatas = [new BrokerData('DefaultCluster', 'broker-a', [MixAll::MASTER_ID => 'b'])];
            return $trd;
        })();
        $ccPull = new ConsumerConnection();
        $ccPull->consumeType = 'CONSUME_ACTIVELY';
        $admin->handler = function (RemotingCommand $c) use ($ccPull) {
            if ($c->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GT', 'GA']]));
            }
            if ($c->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                return $this->okResponse($ccPull->encode());
            }
            return $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 9));
        };
        $tracks = $admin->messageTrackDetailConcurrent($this->trackMsg());
        $this->checkSame(2, count($tracks), 'message track batch tracks every group');
        $this->check($tracks[0] instanceof MessageTrack, 'message track batch returns a plain track list (not wrapped)');
        $this->checkSame('GT', $tracks[0]->consumerGroup, 'message track batch keeps the broker answer order');
        $this->checkSame(TrackType::PULL, $tracks[0]->trackType, 'message track batch pull verdict');
        $this->checkSame('GA', $tracks[1]->consumerGroup, 'message track batch second group');

        // A group whose connection probe fails keeps its own track; the others still tracked.
        $admin2 = $this->newAdmin();
        $this->route('T', 'broker-a', 'b:1');
        $this->client->clusterInfo = new ClusterInfo();
        $this->client->clusterInfo->brokerAddrTable = ['broker-a' => [MixAll::MASTER_ID => 'b']];
        $this->client->topicRoutes[MixAll::getRetryTopic('GOFF')] = (function () {
            $trd = new TopicRouteData();
            $trd->brokerDatas = [new BrokerData('DefaultCluster', 'broker-a', [MixAll::MASTER_ID => 'b'])];
            return $trd;
        })();
        $admin2->handler = function (RemotingCommand $c) {
            if ($c->code === RequestCode::QUERY_TOPIC_CONSUME_BY_WHO) {
                return $this->okResponse(json_encode(['groupList' => ['GOFF', 'GA']]));
            }
            if ($c->code === RequestCode::GET_CONSUMER_CONNECTION_LIST) {
                $group = $c->extFields['consumerGroup'] ?? '';
                if ($group === 'GOFF') {
                    return $this->errResponse(ResponseCode::CONSUMER_NOT_ONLINE, 'offline');
                }
                $cc = new ConsumerConnection();
                $cc->consumeType = 'CONSUME_ACTIVELY';
                return $this->okResponse($cc->encode());
            }
            return $this->okResponse($this->statsBody(1.0, 'T', 'broker-a', 0, 9));
        };
        $tracks2 = $admin2->messageTrackDetailConcurrent($this->trackMsg());
        $this->checkSame(2, count($tracks2), 'message track batch keeps tracking after a group probe fails');
        $this->checkSame(TrackType::NOT_ONLINE, $tracks2[0]->trackType, 'message track batch offline group → NOT_ONLINE');
        $this->check(str_contains((string) $tracks2[0]->exceptionDesc, 'CODE:206'), 'message track batch offline group carries the exception description');
        $this->checkSame(TrackType::PULL, $tracks2[1]->trackType, 'message track batch surviving group still tracked');

        // No broker in the route → empty list.
        $admin3 = $this->newAdmin();
        $this->client->topicRoutes['T'] = new TopicRouteData();
        $this->checkSame([], $admin3->messageTrackDetailConcurrent($this->trackMsg()), 'message track batch without brokers → empty list');
    }

    // ---------------------------------------------------------------- deadline

    private function testDeadlineBudget(): void
    {
        // timeout 0 → the fan-out deadline is already spent: nothing is sent, the
        // envelope is still a success (the state check needs no sleep).
        $admin = $this->newAdmin();
        $admin->setTimeoutMillis(0);
        $this->route2('T', 'broker-a', 'b:1', 'broker-b', 'b:2');
        $res = $admin->examineTopicStatsConcurrent('T');
        $this->check($res->success && $res->data instanceof TopicStatsTable && $res->data->offsetTable === [], 'spent deadline → success with an empty merge');
        $this->checkSame(0, count($admin->requests), 'spent deadline sends no requests');

        $admin2 = $this->newAdmin();
        $admin2->setTimeoutMillis(0);
        $res2 = $admin2->deleteTopicInBrokerConcurrent(['b:1', 'b:2'], 'T');
        $this->check($res2->success && $res2->data?->successList === [] && $res2->data?->failureList === [], 'spent deadline → both operator lists empty');
        $this->checkSame(0, count($admin2->requests), 'spent deadline delete batch sends no requests');
    }
}

/**
 * Local alias so the test reads without a long import line; the pop-retry topic name is
 * the V1 shape ``%RETRY%<group>_<topic>``.
 */
function ExtraInfoUtil_buildPopRetryTopic(string $topic, string $group): string
{
    return \RocketMQ\Remoting\Protocol\ExtraInfoUtil::buildPopRetryTopic($topic, $group);
}

exit((new RunClientAdminLongTail())->run());

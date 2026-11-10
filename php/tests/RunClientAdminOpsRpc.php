<?php

declare(strict_types=1);

/**
 * Client admin ops-RPC group wire self-test (no phpunit, no real cluster).
 *
 * Pins the wire shape of the #47 ops RPC group (Java MQClientAPIImpl:2773-3816):
 * queryCorrectionOffset(308), the three unit-topic reads (311/312/313),
 * getAllProducerInfo(328), getBrokerHAStatus(907), resetMasterFlushOffset(908),
 * electMaster(1002), getBrokerEpochCache(1007) and switchTimerEngine(5001):
 * dial address (including the VIP two-state on the broker-VIP families), header
 * extFields, the JSON body decoding of every response DTO (numeric-key maps,
 * nested HA structures, the defaulted epoch endOffset), and the fixed 3000 ms
 * budget of the controller-routed families.
 *
 * Network-layer doubles: OpsRpcAdmin overrides DefaultMQAdminExt::invokeSyncOn
 * answering per script and recording every request (address, command, timeout);
 * OpsRpcClient subclasses MQClientInstance with an offline constructor.
 *
 * Run: php tests/RunClientAdminOpsRpc.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Remoting\Protocol\BrokerMemberGroup;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\TopicList;

// ==================================================================== test doubles

/** Admin double recording every sync RPC (address, command, effective timeout). */
final class OpsRpcAdmin extends DefaultMQAdminExt
{
    /** @var list<array{addr: string, cmd: RemotingCommand, timeout: ?int}> */
    public array $requests = [];

    /** @var callable(RemotingCommand, string): ?RemotingCommand|null */
    public $handler = null;

    /** Number of times each code has been hit, so handlers can branch per call. */
    public array $codeHits = [];

    protected function invokeSyncOn(MQClientInstance $client, string $addr, RemotingCommand $request, ?int $timeoutMillis): RemotingCommand
    {
        $code = $request->code;
        $this->requests[] = ['addr' => $addr, 'cmd' => $request, 'timeout' => $timeoutMillis];
        $n = $this->codeHits[$code] ?? 0;
        $this->codeHits[$code] = $n + 1;
        $h = $this->handler;
        $resp = $h !== null ? $h($request, $addr, $n) : null;
        return $resp ?? RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

/** MQClientInstance double: offline constructor. */
final class OpsRpcClient extends MQClientInstance
{
    public function __construct(array $nameServers = ['127.0.0.1:9876'])
    {
        parent::__construct('admin-ops-client', $nameServers);
    }
}

// ==================================================================== runner

final class RunClientAdminOpsRpc
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
        try {
            $fn();
        } catch (\Throwable $e) {
            $this->check($e instanceof $exceptionClass, sprintf('%s (got %s: %s)', $label, get_class($e), $e->getMessage()));
            return;
        }
        $this->check(false, "{$label} (no exception thrown)");
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

    private OpsRpcAdmin $admin;

    private function newAdmin(): OpsRpcAdmin
    {
        $client = new OpsRpcClient();
        $this->admin = new OpsRpcAdmin();
        $this->admin->setMqClientInstance($client);
        return $this->admin;
    }

    /** Per-code scripted responses; each entry is consumed in order (null = SUCCESS, no body). */
    private function respondWith(OpsRpcAdmin $admin, int $code, callable $make): void
    {
        $outer = $admin->handler;
        $admin->handler = function (RemotingCommand $req, string $addr, int $n) use ($code, $make, $outer): ?RemotingCommand {
            if ($req->code === $code) {
                return $make($req, $n);
            }
            return $outer !== null ? $outer($req, $addr, $n) : null;
        };
    }

    /** Scripted response builder: SUCCESS with a JSON body and optional extFields. */
    private function okBody(array|string $body, array $extFields = []): \Closure
    {
        return static function (RemotingCommand $req) use ($body, $extFields): RemotingCommand {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->opaque = $req->opaque;
            $json = is_string($body) ? $body : json_encode($body, JSON_THROW_ON_ERROR);
            $resp->body = $json;
            $resp->extFields = $extFields;
            return $resp;
        };
    }

    private function okMeta(string $leaderAddr): \Closure
    {
        return static function (RemotingCommand $req) use ($leaderAddr): RemotingCommand {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->opaque = $req->opaque;
            $resp->extFields = ['controllerLeaderAddress' => $leaderAddr];
            return $resp;
        };
    }

    private function fail(int $code = ResponseCode::SYSTEM_ERROR, string $remark = 'boom'): \Closure
    {
        return static function (RemotingCommand $req) use ($code, $remark): RemotingCommand {
            $resp = RemotingCommand::createResponseCommand($code, $remark);
            $resp->opaque = $req->opaque;
            return $resp;
        };
    }

    /** @return list<array{addr: string, cmd: RemotingCommand, timeout: ?int}> */
    private function reqsOf(OpsRpcAdmin $a, int $code): array
    {
        return array_values(array_filter($a->requests, static fn(array $r): bool => $r['cmd']->code === $code));
    }

    private function oneReq(OpsRpcAdmin $a, int $code): array
    {
        $reqs = $this->reqsOf($a, $code);
        $this->check(count($reqs) === 1, sprintf('exactly one request with code %d (got %d)', $code, count($reqs)));
        return $reqs[0] ?? ['addr' => '', 'cmd' => null, 'timeout' => null];
    }

    // ================================================================ tests

    public function run(): int
    {
        $this->testQueryCorrectionOffset();
        $this->testUnitTopicListFamily();
        $this->testGetAllProducerInfo();
        $this->testGetBrokerHAStatus();
        $this->testResetMasterFlushOffset();
        $this->testElectMaster();
        $this->testGetBrokerEpochCache();
        $this->testSwitchTimerEngine();
        return $this->finish();
    }

    // ---------------------------------------------------------------- 308

    private function testQueryCorrectionOffset(): void
    {
        // The three header fields, the numeric-key correctionOffsets map.
        $admin = $this->newAdmin();
        $this->respondWith($admin, RequestCode::QUERY_CORRECTION_OFFSET,
            $this->okBody(['correctionOffsets' => ['0' => 110, '3' => 407]]));
        $out = $admin->queryCorrectionOffset('127.0.0.1:10911', 'OpsTopic', 'GID_Ops',
            ['GID_other', 'GID_x']);
        $r = $this->oneReq($admin, RequestCode::QUERY_CORRECTION_OFFSET);
        $this->checkSame('127.0.0.1:10911', $r['addr'], '308 dials the broker address');
        $this->checkSame('OpsTopic', $r['cmd']->extFields['topic'] ?? null, '308 carries topic');
        $this->checkSame('GID_Ops', $r['cmd']->extFields['compareGroup'] ?? null, '308 compareGroup = group');
        $got = explode(',', $r['cmd']->extFields['filterGroups'] ?? '');
        sort($got);
        $this->checkSame(['GID_other', 'GID_x'], $got, '308 filterGroups is comma-joined');
        $this->checkSame(null, $r['cmd']->body, '308 has no body');
        $this->checkSame([0 => 110, 3 => 407], $out, '308 decodes the numeric-key correctionOffsets map');

        // VIP two-state: enabled → port - 2.
        $adminVip = $this->newAdmin();
        $adminVip->setVipChannelEnabled(true);
        $this->respondWith($adminVip, RequestCode::QUERY_CORRECTION_OFFSET,
            $this->okBody(['correctionOffsets' => []]));
        $adminVip->queryCorrectionOffset('127.0.0.1:10911', 'OpsTopic', 'GID_Ops');
        $this->checkSame('127.0.0.1:10909', $this->oneReq($adminVip, RequestCode::QUERY_CORRECTION_OFFSET)['addr'],
            '308 with the VIP switch on dials port - 2');

        // No filter groups → the field is omitted entirely.
        $admin2 = $this->newAdmin();
        $this->respondWith($admin2, RequestCode::QUERY_CORRECTION_OFFSET,
            $this->okBody(['correctionOffsets' => []]));
        $admin2->queryCorrectionOffset('127.0.0.1:10911', 'OpsTopic', 'GID_Ops');
        $r2 = $this->oneReq($admin2, RequestCode::QUERY_CORRECTION_OFFSET);
        $this->check(!isset($r2['cmd']->extFields['filterGroups']), '308 without filter groups omits the field');
        $this->checkSame('OpsTopic', $r2['cmd']->extFields['topic'] ?? null, '308 still carries topic/compareGroup');
    }

    // ---------------------------------------------------------------- 311/312/313

    private function testUnitTopicListFamily(): void
    {
        $family = [
            [RequestCode::GET_UNIT_TOPIC_LIST, fn (DefaultMQAdminExt $a) => $a->getUnitTopicList(true)],
            [RequestCode::GET_HAS_UNIT_SUB_TOPIC_LIST, fn (DefaultMQAdminExt $a) => $a->getHasUnitSubTopicList(true)],
            [RequestCode::GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, fn (DefaultMQAdminExt $a) => $a->getHasUnitSubUnUnitTopicList(true)],
        ];
        foreach ($family as [$code, $invoke]) {
            $admin = $this->newAdmin();
            $this->respondWith($admin, $code,
                $this->okBody(['topicList' => ['TopicA', '%RETRY%G']]));
            /** @var TopicList $out */
            $out = $invoke($admin);
            $r = $this->oneReq($admin, $code);
            $this->checkSame('127.0.0.1:9876', $r['addr'], "{$code} hits ONE name server");
            $this->checkSame([], $r['cmd']->extFields, "{$code} null header: no ext fields at all");
            $this->checkSame(null, $r['cmd']->body, "{$code} has no body");
            $this->checkSame(['TopicA', '%RETRY%G'], $out->topicList, "{$code} decodes the TopicList body");
            $this->checkSame(null, $out->brokerAddr, "{$code} carries no brokerAddr");
        }

        // containRetry=false strips %RETRY% topics client-side; the wire answer untouched.
        $admin2 = $this->newAdmin();
        $this->respondWith($admin2, RequestCode::GET_UNIT_TOPIC_LIST,
            $this->okBody(['topicList' => ['TopicA', '%RETRY%G', '%RETRY%G_lite']]));
        $filtered = $admin2->getUnitTopicList(false);
        $this->checkSame(['TopicA'], $filtered->topicList, '311 containRetry=false strips %RETRY% locally');
        $this->checkSame([], $this->oneReq($admin2, RequestCode::GET_UNIT_TOPIC_LIST)['cmd']->extFields,
            '311 the raw answer is untouched on the wire (filter after decode)');

        // Non-SUCCESS → client exception.
        $admin3 = $this->newAdmin();
        $this->respondWith($admin3, RequestCode::GET_HAS_UNIT_SUB_TOPIC_LIST, $this->fail());
        $this->checkThrows(fn () => $admin3->getHasUnitSubTopicList(), MQClientException::class,
            '312 non-SUCCESS raises the client exception');
    }

    // ---------------------------------------------------------------- 328

    private function testGetAllProducerInfo(): void
    {
        $admin = $this->newAdmin();
        $this->respondWith($admin, RequestCode::GET_ALL_PRODUCER_INFO, $this->okBody([
            'data' => ['G' => [['clientId' => 'cid-1', 'remoteIP' => '10.0.0.1', 'language' => 'JAVA',
                'version' => 500, 'lastUpdateTimestamp' => 1700000000000]]],
        ]));
        $table = $admin->getAllProducerInfo('127.0.0.1:10911');
        $r = $this->oneReq($admin, RequestCode::GET_ALL_PRODUCER_INFO);
        $this->checkSame('127.0.0.1:10911', $r['addr'], '328 dials the broker');
        $this->checkSame([], $r['cmd']->extFields, '328 GetAllProducerInfoRequestHeader is empty');
        $this->checkSame('cid-1', $table->data['G'][0]->clientId ?? null, '328 decodes clientId');
        $this->checkSame('10.0.0.1', $table->data['G'][0]->remoteIP ?? null, '328 decodes remoteIP');
        $this->checkSame(500, $table->data['G'][0]->version ?? null, '328 decodes version');
        $this->checkSame(1700000000000, $table->data['G'][0]->lastUpdateTimestamp ?? null, '328 decodes lastUpdateTimestamp');

        $adminVip = $this->newAdmin();
        $adminVip->setVipChannelEnabled(true);
        $this->respondWith($adminVip, RequestCode::GET_ALL_PRODUCER_INFO,
            $this->okBody(['data' => []]));
        $adminVip->getAllProducerInfo('127.0.0.1:10911');
        $this->checkSame('127.0.0.1:10909', $this->oneReq($adminVip, RequestCode::GET_ALL_PRODUCER_INFO)['addr'],
            '328 with the VIP switch on dials port - 2');
    }

    // ---------------------------------------------------------------- 907

    private function testGetBrokerHAStatus(): void
    {
        $admin = $this->newAdmin();
        $this->respondWith($admin, RequestCode::GET_BROKER_HA_STATUS, $this->okBody([
            'master' => true,
            'masterCommitLogMaxOffset' => 9001,
            'inSyncSlaveNums' => 2,
            'haConnectionInfo' => [['addr' => '10.0.0.2:10912', 'slaveAckOffset' => 8900,
                'diff' => 101, 'inSync' => true, 'transferredByteInSecond' => 2048,
                'transferFromWhere' => 8700]],
            'haClientRuntimeInfo' => ['masterAddr' => '10.0.0.1:10912',
                'transferredByteInSecond' => 12, 'maxOffset' => 9001,
                'lastReadTimestamp' => 111, 'lastWriteTimestamp' => 222,
                'masterFlushOffset' => 8800, 'activated' => true],
        ]));
        $info = $admin->getBrokerHAStatus('127.0.0.1:10911');
        $r = $this->oneReq($admin, RequestCode::GET_BROKER_HA_STATUS);
        $this->checkSame('127.0.0.1:10911', $r['addr'], '907 dials the PLAIN broker address, no -2 rewrite');
        $this->checkSame([], $r['cmd']->extFields, '907 null header');
        $this->checkSame(true, $info->master, '907 decodes master');
        $this->checkSame(9001, $info->masterCommitLogMaxOffset, '907 decodes masterCommitLogMaxOffset');
        $this->checkSame(2, $info->inSyncSlaveNums, '907 decodes inSyncSlaveNums');
        $conn = $info->haConnectionInfo[0] ?? null;
        $this->check($conn !== null, '907 has one HA connection entry');
        $this->checkSame('10.0.0.2:10912', $conn?->addr, '907 decodes HA connection addr');
        $this->checkSame(8900, $conn?->slaveAckOffset, '907 decodes slaveAckOffset');
        $this->checkSame(101, $conn?->diff, '907 decodes diff');
        $this->checkSame(true, $conn?->inSync, '907 decodes inSync');
        $this->checkSame(2048, $conn?->transferredByteInSecond, '907 decodes transferredByteInSecond');
        $this->checkSame(8700, $conn?->transferFromWhere, '907 decodes transferFromWhere');
        $hc = $info->haClientRuntimeInfo;
        $this->checkSame('10.0.0.1:10912', $hc->masterAddr, '907 decodes the HA client masterAddr');
        $this->checkSame(9001, $hc->maxOffset, '907 decodes the HA client maxOffset');
        $this->checkSame(8800, $hc->masterFlushOffset, '907 decodes the HA client masterFlushOffset');
        $this->checkSame(true, $hc->isActivated, '907 accepts both activated spellings');

        $admin2 = $this->newAdmin();
        $this->respondWith($admin2, RequestCode::GET_BROKER_HA_STATUS, $this->fail());
        $this->checkThrows(fn () => $admin2->getBrokerHAStatus('127.0.0.1:10911'), MQBrokerException::class,
            '907 non-SUCCESS raises the broker exception');
    }

    // ---------------------------------------------------------------- 908

    private function testResetMasterFlushOffset(): void
    {
        $admin = $this->newAdmin();
        $admin->resetMasterFlushOffset('127.0.0.1:10911', 12345);
        $r = $this->oneReq($admin, RequestCode::RESET_MASTER_FLUSH_OFFSET);
        $this->checkSame('127.0.0.1:10911', $r['addr'], '908 dials the plain broker address');
        $this->checkSame(['masterFlushOffset' => '12345'], $r['cmd']->extFields,
            '908 sends the primitive long and nothing else');
        $this->checkSame(null, $r['cmd']->body, '908 has no body');
        $this->checkSame(DefaultMQAdminExt::CONTROLLER_TIMEOUT, $r['timeout'],
            '908 is pinned to the 3000 ms budget');
    }

    // ---------------------------------------------------------------- 1002

    private function testElectMaster(): void
    {
        $memberBody = json_encode([
            'cluster' => 'DefaultCluster', 'brokerName' => 'broker-a',
            'brokerAddrs' => ['0' => '10.0.0.1:10911', '1' => '10.0.0.2:10911'],
        ], JSON_THROW_ON_ERROR);
        $admin = $this->newAdmin();
        $admin->handler = function (RemotingCommand $req) use ($memberBody): ?RemotingCommand {
            if ($req->code === RequestCode::CONTROLLER_GET_METADATA_INFO) {
                return ($this->okMeta('127.0.0.1:12012'))($req);
            }
            if ($req->code === RequestCode::CONTROLLER_ELECT_MASTER) {
                $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
                $resp->opaque = $req->opaque;
                $resp->body = $memberBody;
                $resp->extFields = ['masterBrokerId' => '1', 'masterAddress' => '10.0.0.2:10911',
                    'masterEpoch' => '3', 'syncStateSetEpoch' => '7'];
                return $resp;
            }
            return null;
        };
        [$header, $group] = $admin->electMaster('127.0.0.1:12011', 'DefaultCluster', 'broker-a', 1);

        // step 1: 1005 metadata read names the leader
        $meta = $this->oneReq($admin, RequestCode::CONTROLLER_GET_METADATA_INFO);
        $this->checkSame('127.0.0.1:12011', $meta['addr'], '1002 step 1 reads metadata from the controller');
        // step 2: 1002 goes to the leader with the ofAdminTrigger shape
        $elect = $this->oneReq($admin, RequestCode::CONTROLLER_ELECT_MASTER);
        $this->checkSame('127.0.0.1:12012', $elect['addr'], '1002 step 2 dials the LEADER');
        $this->checkSame('DefaultCluster', $elect['cmd']->extFields['clusterName'] ?? null, '1002 carries clusterName');
        $this->checkSame('broker-a', $elect['cmd']->extFields['brokerName'] ?? null, '1002 carries brokerName');
        $this->checkSame('1', $elect['cmd']->extFields['brokerId'] ?? null, '1002 carries the designated brokerId');
        $this->checkSame('true', $elect['cmd']->extFields['designateElect'] ?? null, '1002 designateElect is forced true');
        $this->check((int)($elect['cmd']->extFields['invokeTime'] ?? 0) > 0, '1002 carries a live invokeTime');
        $this->checkSame(DefaultMQAdminExt::CONTROLLER_TIMEOUT, $elect['timeout'], '1002 elect leg is pinned to 3000 ms');
        $this->checkSame(1, $header->masterBrokerId, '1002 parses masterBrokerId');
        $this->checkSame('10.0.0.2:10911', $header->masterAddress, '1002 parses masterAddress');
        $this->checkSame(3, $header->masterEpoch, '1002 parses masterEpoch');
        $this->checkSame(7, $header->syncStateSetEpoch, '1002 parses syncStateSetEpoch');
        $this->checkSame('DefaultCluster', $group->cluster, '1002 decodes the member group cluster');
        $this->checkSame('broker-a', $group->brokerName, '1002 decodes the member group brokerName');
        $this->checkSame([0 => '10.0.0.1:10911', 1 => '10.0.0.2:10911'], $group->brokerAddrs,
            '1002 decodes the numeric-key brokerAddrs map');

        // No designated id → -1.
        $admin2 = $this->newAdmin();
        $this->respondWith($admin2, RequestCode::CONTROLLER_GET_METADATA_INFO, $this->okMeta('127.0.0.1:12012'));
        $this->respondWith($admin2, RequestCode::CONTROLLER_ELECT_MASTER, $this->okBody('{}'));
        $admin2->electMaster('127.0.0.1:12011', 'DefaultCluster', 'broker-a');
        $elect2 = $this->oneReq($admin2, RequestCode::CONTROLLER_ELECT_MASTER);
        $this->checkSame('-1', $elect2['cmd']->extFields['brokerId'] ?? null, '1002 without a designated id sends brokerId = -1');

        // Non-SUCCESS on the elect leg → broker exception.
        $admin3 = $this->newAdmin();
        $this->respondWith($admin3, RequestCode::CONTROLLER_GET_METADATA_INFO, $this->okMeta('127.0.0.1:12012'));
        $this->respondWith($admin3, RequestCode::CONTROLLER_ELECT_MASTER, $this->fail());
        $this->checkThrows(
            fn () => $admin3->electMaster('127.0.0.1:12011', 'DefaultCluster', 'broker-a'),
            MQBrokerException::class,
            '1002 non-SUCCESS raises the broker exception',
        );
    }

    // ---------------------------------------------------------------- 1007

    private function testGetBrokerEpochCache(): void
    {
        $admin = $this->newAdmin();
        $this->respondWith($admin, RequestCode::GET_BROKER_EPOCH_CACHE, $this->okBody([
            'clusterName' => 'DefaultCluster', 'brokerName' => 'broker-a', 'brokerId' => 1,
            'epochList' => [['epoch' => 1, 'startOffset' => 0, 'endOffset' => 500],
                ['epoch' => 2, 'startOffset' => 500]],
            'maxOffset' => 900,
        ]));
        $cache = $admin->getBrokerEpochCache('127.0.0.1:10911');
        $r = $this->oneReq($admin, RequestCode::GET_BROKER_EPOCH_CACHE);
        $this->checkSame('127.0.0.1:10911', $r['addr'], '1007 dials the plain broker address');
        $this->checkSame([], $r['cmd']->extFields, '1007 null header');
        $this->checkSame(DefaultMQAdminExt::CONTROLLER_TIMEOUT, $r['timeout'], '1007 is pinned to the 3000 ms budget');
        $this->checkSame('DefaultCluster', $cache->clusterName, '1007 decodes clusterName');
        $this->checkSame('broker-a', $cache->brokerName, '1007 decodes brokerName');
        $this->checkSame(1, $cache->brokerId, '1007 decodes brokerId');
        $this->checkSame(900, $cache->maxOffset, '1007 decodes maxOffset');
        $this->checkSame(2, count($cache->epochList), '1007 has two epoch entries');
        $this->checkSame(1, $cache->epochList[0]->epoch, '1007 decodes epoch #1');
        $this->checkSame(500, $cache->epochList[0]->endOffset, '1007 decodes an explicit endOffset');
        $this->checkSame(2, $cache->epochList[1]->epoch, '1007 decodes epoch #2');
        $this->checkSame(9223372036854775807, $cache->epochList[1]->endOffset,
            '1007 a missing endOffset defaults to Long.MAX_VALUE');
    }

    // ---------------------------------------------------------------- 5001

    private function testSwitchTimerEngine(): void
    {
        $admin = $this->newAdmin();
        $admin->switchTimerEngine('127.0.0.1:10911', 'rocksdb', 4500);
        $r = $this->oneReq($admin, RequestCode::SWITCH_TIMER_ENGINE);
        $this->checkSame('127.0.0.1:10911', $r['addr'], '5001 dials the plain broker address');
        $this->checkSame(['E_T' => 'rocksdb'], $r['cmd']->extFields, '5001 sends the single E_T ext field');
        $this->checkSame(null, $r['cmd']->body, '5001 has no body');
        $this->checkSame(4500, $r['timeout'], '5001 forwards the caller timeout verbatim');
    }
}

exit((new RunClientAdminOpsRpc())->run());

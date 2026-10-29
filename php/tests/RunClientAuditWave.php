<?php

declare(strict_types=1);

/**
 * Audit-wave regression cases (offline, no cluster).
 *
 * Run: php tests/RunClientAuditWave.php (or via tests/run_all.php)
 *
 * Covers the behaviours that are easy to regress because they depend on state the wire
 * cannot show directly:
 *   A1/A2  orderly queue-lock gate: broadcasting is never gated, clustering needs a fresh
 *          broker grant (older than 30s counts as unlocked);
 *   B5     inner-batch unwrapping on the pull path, with the envelope kept on a bad body;
 *   C1     a consumer-offset query that fails (not "not found") skips the queue for the
 *          round instead of starting at the tail;
 *   C3     local persist is a merge: queues not flushed this round keep their stored value;
 *   C4     pull/lite-pull broadcasting reads and writes the local file, never the broker;
 *   D1     stats windows seed a zero point and cap at 7 (minute/hour) and 25 (day);
 *   F1     lite start accepts only the create-just state and checks the suspend timeout.
 *
 * State that would normally be built by start()/rebalance is installed through
 * reflection; a fake client records which broker calls the code under test makes.
 */

require_once __DIR__ . '/../bootstrap.php';
require_once __DIR__ . '/../src/Client/ConsumerSupport.php';

putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\ConsumerStatsManager;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\StatsItem;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageSysFlag;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\MessageModel;
use RocketMQ\Remoting\Protocol\ResponseCode;

use function RocketMQ\Client\build_local_offsets_json;
use function RocketMQ\Client\local_offset_file_path;
use function RocketMQ\Client\parse_local_offsets_json;

/** Records broker traffic and answers with configurable, deterministic values. */
final class AuditFakeInstance extends MQClientInstance
{
    public int $pullCalls = 0;

    /** @var list<int> */
    public array $pullOffsets = [];

    public int $queryCalls = 0;
    public int $updateCalls = 0;
    public int $maxCalls = 0;

    public ?int $queryResult = null;
    public ?\Throwable $queryError = null;
    public int $maxOffsetValue = 100;

    public function __construct(string $clientId = 'audit-fake')
    {
        parent::__construct($clientId, []);
    }

    public function pullMessage(
        string $consumerGroup,
        MessageQueue $mq,
        int $queueOffset,
        int $maxMsgNums,
        int $sysFlag,
        int $commitOffset,
        ?string $subscription,
        int $subVersion,
        ?string $expressionType,
        int $timeoutMillis = 30000,
        int $maxMsgBytes = -1,
        int $suspendTimeoutMillis = 15000,
        ?string $addr = null,
        int $requestSource = 0,
        ?int $brokerId = null,
    ): PullResult {
        $this->pullCalls++;
        $this->pullOffsets[] = $queueOffset;
        return new PullResult(PullStatus::NO_NEW_MSG, $queueOffset, 0, $this->maxOffsetValue, []);
    }

    public function queryConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $timeoutMillis = 5000,
        ?string $addr = null,
        bool $setZeroIfNotFound = false,
    ): ?int {
        $this->queryCalls++;
        if ($this->queryError !== null) {
            throw $this->queryError;
        }
        return $this->queryResult;
    }

    public function updateConsumerOffset(
        string $consumerGroup,
        MessageQueue $mq,
        int $commitOffset,
        int $timeoutMillis = 5000,
        ?string $addr = null,
    ): void {
        $this->updateCalls++;
    }

    public function getMaxOffset(MessageQueue $mq, int $timeoutMillis = 5000, ?string $addr = null): int
    {
        $this->maxCalls++;
        return $this->maxOffsetValue;
    }
}

final class RunClientAuditWave
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    private string $tmpHome = '';

    private string $oldHome = '';

    public function __construct()
    {
        $this->oldHome = (string) (getenv('HOME') ?: '');
        $this->tmpHome = sys_get_temp_dir() . '/rmq-audit-wave-' . getmypid();
        if (!is_dir($this->tmpHome)) {
            @mkdir($this->tmpHome, 0777, true);
        }
        putenv('HOME=' . $this->tmpHome);
    }

    public function check(bool $cond, string $label, string $detail = ''): void
    {
        if ($cond) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: {$label}" . ($detail === '' ? '' : " ({$detail})") . "\n");
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        $this->check($expected === $actual, $label,
            'expected ' . var_export($expected, true) . ', got ' . var_export($actual, true));
    }

    private function setPrivate(object $object, string $name, mixed $value): void
    {
        $prop = new \ReflectionProperty($object::class, $name);
        $prop->setAccessible(true);
        $prop->setValue($object, $value);
    }

    private function getPrivate(object $object, string $name): mixed
    {
        $prop = new \ReflectionProperty($object::class, $name);
        $prop->setAccessible(true);
        return $prop->getValue($object);
    }

    private function invokePrivate(object $object, string $method, array $args): mixed
    {
        $m = new \ReflectionMethod($object::class, $method);
        $m->setAccessible(true);
        return $m->invokeArgs($object, $args);
    }

    // ================================================================ A1/A2

    /**
     * Build a running-looking push consumer with one assigned queue and the given lock state.
     */
    private function pushConsumer(string $group, string $model, AuditFakeInstance $fake, MessageQueue $mq,
        bool $locked, ?float $grantedAt): DefaultMQPushConsumer
    {
        $c = new DefaultMQPushConsumer($group);
        $c->setInstanceName($group . '-inst');
        $c->clientId = $group . '-client';
        $c->messageModel = $model;
        $c->subscribe($mq->topic, '*');
        $c->mqClient = $fake;
        $this->setPrivate($c, 'started', true);
        $key = $mq->topic . $mq->brokerName . (string) $mq->queueId;
        $this->setPrivate($c, 'offsetTable', [$key => 0]);
        if ($locked) {
            $this->setPrivate($c, 'lockOk', [$key => true]);
        }
        if ($grantedAt !== null) {
            $this->setPrivate($c, 'lockTimestamps', [$key => $grantedAt]);
        }
        return $c;
    }

    private function pullOnce(DefaultMQPushConsumer $c, MessageQueue $mq, bool $orderly): bool
    {
        $key = $mq->topic . $mq->brokerName . (string) $mq->queueId;
        return (bool) $this->invokePrivate($c, 'pullOnce', [$mq, $key, $orderly, microtime(true)]);
    }

    private function testLockGate(): void
    {
        $mq = new MessageQueue('TAuditOrderly', 'b-audit', 0);

        // A2: a clustered orderly queue with a fresh grant pulls.
        $fake = new AuditFakeInstance();
        $now = microtime(true);
        $c = $this->pushConsumer('GID_AUDIT_A2_FRESH', MessageModel::CLUSTERING, $fake, $mq,
            true, $now - 5.0);
        $this->pullOnce($c, $mq, true);
        $this->checkSame(1, $fake->pullCalls, 'A2: a fresh lock grant lets the clustered orderly queue pull');

        // A2: the same grant older than 30s counts as unlocked.
        $fake = new AuditFakeInstance();
        $c = $this->pushConsumer('GID_AUDIT_A2_STALE', MessageModel::CLUSTERING, $fake, $mq,
            true, $now - 31.0);
        $this->pullOnce($c, $mq, true);
        $this->checkSame(0, $fake->pullCalls, 'A2: a grant older than 30s is treated as unlocked');

        // Paired control: far past 30s but no sweep result at all also blocks.
        $fake = new AuditFakeInstance();
        $c = $this->pushConsumer('GID_AUDIT_A2_NONE', MessageModel::CLUSTERING, $fake, $mq,
            false, null);
        $this->pullOnce($c, $mq, true);
        $this->checkSame(0, $fake->pullCalls, 'A2 control: no broker grant means no pull');

        // A1: broadcasting owns every queue locally, so it pulls without any lock.
        $fake = new AuditFakeInstance();
        $c = $this->pushConsumer('GID_AUDIT_A1_BC', MessageModel::BROADCASTING, $fake, $mq,
            false, null);
        $this->pullOnce($c, $mq, true);
        $this->checkSame(1, $fake->pullCalls,
            'A1: orderly broadcasting pulls without waiting for a broker lock');
    }

    // ================================================================ B5

    private function innerBatchEnvelope(string $body): MessageExt
    {
        $outer = new MessageExt();
        $outer->setTopic('TAuditBatch');
        $outer->queueId = 3;
        $outer->queueOffset = 70;
        $outer->setFlag(9);
        $outer->setBody($body);
        $outer->bornHost = '10.0.0.1';
        $outer->bornHostPort = 10911;
        $outer->bornTimestamp = 1111;
        $outer->storeHost = '10.0.0.2';
        $outer->storeHostPort = 10912;
        $outer->storeTimestamp = 2222;
        $outer->sysFlag = MessageSysFlag::INNER_BATCH_FLAG | MessageSysFlag::NEED_UNWRAP_FLAG;
        $outer->commitLogOffset = 3333;
        $outer->reconsumeTimes = 2;
        return $outer;
    }

    private function testInnerBatchUnwrap(): void
    {
        $m1 = new Message('TAuditBatch', 'body-one');
        $m1->setFlag(1);
        $m2 = new Message('TAuditBatch', 'body-two');
        $m2->setFlag(2);
        $innerBody = MessageDecoder::encodeMessages([$m1, $m2]);
        $outer = $this->innerBatchEnvelope($innerBody);

        $members = MessageDecoder::unwrapInnerBatch($outer);
        $this->checkSame(2, count($members), 'B5: two inner frames expand into two messages');
        $this->checkSame('body-one', $members[0]->getBody(), 'B5: first member keeps its body');
        $this->checkSame('body-two', $members[1]->getBody(), 'B5: second member keeps its body');
        $this->checkSame(70, $members[0]->queueOffset, 'B5: first member takes the envelope offset');
        $this->checkSame(71, $members[1]->queueOffset, 'B5: second member offset is envelope+i');
        $this->checkSame(3, $members[1]->queueId, 'B5: queueId is copied from the envelope');
        $this->checkSame('TAuditBatch', $members[0]->topic, 'B5: topic is copied from the envelope');
        $this->checkSame('10.0.0.1', $members[0]->bornHost, 'B5: bornHost is copied');
        $this->checkSame(2222, $members[1]->storeTimestamp, 'B5: store timestamp is copied');
        $this->checkSame(3333, $members[0]->commitLogOffset, 'B5: commitLogOffset is copied');
        $this->checkSame(MessageSysFlag::INNER_BATCH_FLAG | MessageSysFlag::NEED_UNWRAP_FLAG,
            $members[0]->sysFlag, 'B5: sysFlag is copied');

        // The decode entry point replaces the envelope in a FOUND result.
        $client = new MQClientInstance('audit-decode', []);
        $encoded = MessageDecoder::encodeMessageExt($outer);
        $response = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $response->body = $encoded;
        $response->extFields['nextBeginOffset'] = '72';
        $response->extFields['minOffset'] = '0';
        $response->extFields['maxOffset'] = '100';
        $mq = new MessageQueue('TAuditBatch', 'b-audit', 3);
        /** @var PullResult $result */
        $result = $this->invokePrivate($client, 'decodePullResponse', [$response, $mq]);
        $this->checkSame(2, count($result->msgFoundList), 'B5: the pull decoder returns the members');
        $this->checkSame('b-audit', $result->msgFoundList[1]->brokerName,
            'B5: members are stamped with the broker identity');
        $this->checkSame(71, $result->msgFoundList[1]->queueOffset,
            'B5: the decoder advances the member offsets');

        // A malformed body keeps the original envelope list (one message, body untouched).
        $badBody = substr($innerBody, 0, -2);
        $badOuter = $this->innerBatchEnvelope($badBody);
        $badResponse = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
        $badResponse->body = MessageDecoder::encodeMessageExt($badOuter);
        $badResponse->extFields['nextBeginOffset'] = '71';
        /** @var PullResult $badResult */
        $badResult = $this->invokePrivate($client, 'decodePullResponse', [$badResponse, $mq]);
        $this->checkSame(1, count($badResult->msgFoundList),
            'B5: a bad inner body keeps the envelope instead of dropping the batch');
        $this->checkSame($badBody, $badResult->msgFoundList[0]->getBody(),
            'B5: the kept envelope body is untouched');
    }

    // ================================================================ C1

    private function testOffsetQueryFailureSkipsQueue(): void
    {
        $mq = new MessageQueue('TAuditOffset', 'b-audit', 0);
        $key = $mq->topic . $mq->brokerName . (string) $mq->queueId;

        // Push: an error other than QUERY_NOT_FOUND skips the round; no tail RPC, no pull.
        $fake = new AuditFakeInstance();
        $fake->queryError = new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'query failed');
        $c = new DefaultMQPushConsumer('GID_AUDIT_C1_PUSH');
        $c->clientId = 'audit-c1-push';
        $c->subscribe($mq->topic, '*');
        $c->mqClient = $fake;
        $this->setPrivate($c, 'started', true);
        $this->pullOnce($c, $mq, false);
        $this->checkSame(0, $fake->pullCalls, 'C1 push: a failed offset query issues no pull');
        $this->checkSame(0, $fake->maxCalls, 'C1 push: a failed offset query does not fall back to the tail');

        // Paired control: QUERY_NOT_FOUND still follows the policy and starts at the tail.
        $fake2 = new AuditFakeInstance();
        $fake2->queryResult = null;
        $fake2->maxOffsetValue = 77;
        $c2 = new DefaultMQPushConsumer('GID_AUDIT_C1_PUSH_OK');
        $c2->clientId = 'audit-c1-push-ok';
        $c2->subscribe($mq->topic, '*');
        $c2->mqClient = $fake2;
        $this->setPrivate($c2, 'started', true);
        $this->pullOnce($c2, $mq, false);
        $this->checkSame([77], $fake2->pullOffsets,
            'C1 push control: not-found still resolves the policy (tail) and pulls');

        // Lite: the same split, observed through the initial offset resolver.
        $liteFake = new AuditFakeInstance();
        $liteFake->queryError = new MQBrokerException(ResponseCode::SYSTEM_ERROR, 'query failed');
        $lite = new DefaultLitePullConsumer('GID_AUDIT_C1_LITE');
        $lite->clientId = 'audit-c1-lite';
        $lite->messageModel = MessageModel::CLUSTERING;
        $lite->mqClient = $liteFake;
        $failed = false;
        try {
            $this->invokePrivate($lite, 'resolveInitialOffset', [$mq]);
        } catch (\Throwable) {
            $failed = true;
        }
        $this->check($failed, 'C1 lite: a failed offset query propagates instead of using the tail');

        $liteFake->queryError = null;
        $liteFake->queryResult = null;
        $liteFake->maxOffsetValue = 88;
        $offset = $this->invokePrivate($lite, 'resolveInitialOffset', [$mq]);
        $this->checkSame(88, $offset, 'C1 lite control: not-found still resolves the policy');

        // The pull round catches the failure and leaves the cursor unset for a later retry.
        $this->checkSame(0, $fake->updateCalls, 'C1: a skipped queue writes no offset');
        $this->checkSame(null, $this->getPrivate($c, 'offsetTable')[$key] ?? null,
            'C1: the skipped queue has no cached start offset');
    }

    // ================================================================ C3

    private function localPath(string $clientId, string $group): string
    {
        return local_offset_file_path($clientId, $group);
    }

    private function writeLocal(string $clientId, string $group, array $items, array $mqMap): void
    {
        $path = $this->localPath($clientId, $group);
        $dir = dirname($path);
        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
        }
        file_put_contents($path, build_local_offsets_json($items, $mqMap));
    }

    private function readLocal(string $clientId, string $group): array
    {
        $path = $this->localPath($clientId, $group);
        return parse_local_offsets_json((string) @file_get_contents($path)) ?? [];
    }

    private function testLocalPersistMerge(): void
    {
        $clientId = 'audit-c3';
        $group = 'GID_AUDIT_C3';
        $a = new MessageQueue('TAuditA', 'b-audit', 0);
        $b = new MessageQueue('TAuditB', 'b-audit', 0);
        $keyA = $a->topic . $a->brokerName . (string) $a->queueId;
        $keyB = $b->topic . $b->brokerName . (string) $b->queueId;
        // Queue B is already on disk but is not flushed by this round.
        $this->writeLocal($clientId, $group, [$keyB => 50], [$keyB => $b]);

        $c = new DefaultMQPushConsumer($group);
        $c->clientId = $clientId;
        $c->messageModel = MessageModel::BROADCASTING;
        $this->setPrivate($c, 'consumeOffsets', [$keyA => 20]);
        $this->setPrivate($c, 'mqMap', [$keyA => $a]);
        $this->invokePrivate($c, 'saveLocalOffsets', []);
        $table = $this->readLocal($clientId, $group);
        $this->checkSame(20, $table[$keyA] ?? null, 'C3: the flushed queue is written');
        $this->checkSame(50, $table[$keyB] ?? null,
            'C3: a queue not flushed this round keeps its stored offset');
    }

    // ================================================================ C4

    private function testBroadcastingUsesLocalStore(): void
    {
        $mq = new MessageQueue('TAuditBC', 'b-audit', 0);
        $key = $mq->topic . $mq->brokerName . (string) $mq->queueId;

        // Default pull consumer.
        $pullClient = 'audit-c4-pull';
        $pullGroup = 'GID_AUDIT_C4_PULL';
        $this->writeLocal($pullClient, $pullGroup, [$key => 40], [$key => $mq]);
        $fake = new AuditFakeInstance();
        $pull = new DefaultMQPullConsumer($pullGroup);
        $pull->clientId = $pullClient;
        $pull->messageModel = MessageModel::BROADCASTING;
        $pull->mqClient = $fake;
        $this->setPrivate($pull, 'started', true);
        $this->checkSame(40, $pull->fetchConsumeOffset($mq),
            'C4 pull: broadcasting reads the committed offset from the local file');
        $pull->updateConsumeOffset($mq, 41);
        $this->checkSame(41, $this->readLocal($pullClient, $pullGroup)[$key] ?? null,
            'C4 pull: broadcasting writes the offset to the local file');
        $this->checkSame(0, $fake->queryCalls + $fake->updateCalls,
            'C4 pull: broadcasting never asks the broker');

        // Lite pull consumer: initial offset, committed read and commit all stay local.
        $liteClient = 'audit-c4-lite';
        $liteGroup = 'GID_AUDIT_C4_LITE';
        $this->writeLocal($liteClient, $liteGroup, [$key => 60], [$key => $mq]);
        $liteFake = new AuditFakeInstance();
        $lite = new DefaultLitePullConsumer($liteGroup);
        $lite->clientId = $liteClient;
        $lite->messageModel = MessageModel::BROADCASTING;
        $lite->mqClient = $liteFake;
        $this->setPrivate($lite, 'started', true);
        $lite->assign([$mq]);
        $this->checkSame(60, $lite->pullCursorOf($mq),
            'C4 lite: broadcasting resolves the initial offset from the local file');
        $this->checkSame(60, $lite->committed($mq),
            'C4 lite: broadcasting reports the local committed offset');
        $lite->commit([$key => 61], true);
        $this->checkSame(61, $this->readLocal($liteClient, $liteGroup)[$key] ?? null,
            'C4 lite: broadcasting commits to the local file');
        $this->checkSame(0, $liteFake->queryCalls + $liteFake->updateCalls,
            'C4 lite: broadcasting never asks the broker');
    }

    // ================================================================ D1

    private function testStatsWindows(): void
    {
        $this->checkSame(7, ConsumerStatsManager::MINUTE_LIST_MAX, 'D1: minute chain cap is 7');
        $this->checkSame(7, ConsumerStatsManager::HOUR_LIST_MAX, 'D1: hour chain cap is 7');
        $this->checkSame(25, ConsumerStatsManager::DAY_LIST_MAX, 'D1: day chain cap is 25');

        $item = new StatsItem('PULL_RT', 'T@G');
        $item->addValue(15, 3);
        $item->sample();
        $this->checkSame(2, $item->getMinuteSampleCount(), 'D1: the first minute sample is seeded');
        $minute = $item->getStatsDataInMinute();
        $this->checkSame(15, $minute->sum, 'D1: the seed lets the first minute window report the total');
        $this->checkSame(3, $minute->times, 'D1: the seed also covers the accumulated times');

        $item->addValue(15, 1);
        $item->sample();
        $minute = $item->getStatsDataInMinute();
        $this->checkSame(30, $minute->sum, 'D1: later minute windows are deltas from the previous point');
        $this->checkSame(4, $minute->times, 'D1: later minute windows count later calls');
        $this->checkSame(7.5, $minute->avgpt, 'D1: avgpt uses the window times');

        for ($i = 0; $i < 10; $i++) {
            $item->sample();
        }
        $this->checkSame(7, $item->getMinuteSampleCount(), 'D1: the minute chain drops the oldest past 7');

        $item->sampleHour();
        $this->checkSame(2, $item->getHourSampleCount(), 'D1: the first hour sample is seeded');
        $this->checkSame(30, $item->getStatsDataInHour()->sum,
            'D1: the first hour window covers the total');
        for ($i = 0; $i < 10; $i++) {
            $item->sampleHour();
        }
        $this->checkSame(7, $item->getHourSampleCount(), 'D1: the hour chain drops the oldest past 7');

        $item->sampleDay();
        $this->checkSame(2, $item->getDaySampleCount(), 'D1: the first day sample is seeded');
        $this->checkSame(30, $item->getStatsDataInDay()->sum,
            'D1: the first day window covers the total');
        for ($i = 0; $i < 30; $i++) {
            $item->sampleDay();
        }
        $this->checkSame(25, $item->getDaySampleCount(), 'D1: the day chain drops the oldest past 25');
    }

    // ================================================================ F1

    private function testLiteStartStateMachine(): void
    {
        // No subscription is required: an empty consumer starts and can then be shut down.
        $lite = new DefaultLitePullConsumer('GID_AUDIT_F1_EMPTY');
        $lite->setNamesrvAddr('127.0.0.1:1');
        $lite->setInstanceName('audit-f1-empty');
        $ok = true;
        try {
            $lite->start();
        } catch (\Throwable $e) {
            $ok = false;
            fwrite(STDERR, 'F1 start without subscription failed: ' . $e->getMessage() . "\n");
        }
        $this->check($ok, 'F1: start does not require a subscription');

        $msg = '';
        try {
            $lite->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'The PullConsumer service state not OK, maybe started once, RUNNING'),
            'F1: a second start while running is refused with the state', $msg);

        $lite->shutdown();
        $msg = '';
        try {
            $lite->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'SHUTDOWN_ALREADY'),
            'F1: a start after shutdown is refused with SHUTDOWN_ALREADY', $msg);
        MQClientInstance::removeInstance($lite->clientId);

        // Suspend timeout below the broker suspend time is refused before any network work.
        $bad = new DefaultLitePullConsumer('GID_AUDIT_F1_SUSPEND');
        $bad->setNamesrvAddr('127.0.0.1:1');
        $bad->setInstanceName('audit-f1-suspend');
        $bad->consumerTimeoutMillisWhenSuspend = 100;
        $bad->brokerSuspendMaxTimeMillis = 200;
        $msg = '';
        try {
            $bad->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis'),
            'F1: a suspend timeout below the broker suspend time is refused', $msg);

        $bad->consumerTimeoutMillisWhenSuspend = 300;
        $msg = '';
        try {
            $bad->start();
        } catch (\Throwable $e) {
            $msg = $e->getMessage();
        }
        $this->check(str_contains($msg, 'START_FAILED'),
            'F1: a failed first start leaves START_FAILED, so a retry is refused', $msg);
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testLockGate();
        $this->testInnerBatchUnwrap();
        $this->testOffsetQueryFailureSkipsQueue();
        $this->testLocalPersistMerge();
        $this->testBroadcastingUsesLocalStore();
        $this->testStatsWindows();
        $this->testLiteStartStateMachine();

        if ($this->oldHome !== '') {
            putenv('HOME=' . $this->oldHome);
        } else {
            putenv('HOME');
        }
        $this->removeTree($this->tmpHome);

        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    private function removeTree(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $it = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );
        foreach ($it as $file) {
            $file->isDir() ? @rmdir($file->getPathname()) : @unlink($file->getPathname());
        }
        @rmdir($dir);
    }
}

exit((new RunClientAuditWave())->run());

<?php

declare(strict_types=1);

// Live-cluster ACL + all-message-types verification: a full set of positive evidence that
// "signed credentials are accepted", plus one negative assertion that "no credentials must be
// rejected with NO_PERMISSION(16)".
//
//   php examples/live_acl_message_types.php [namesrv] [accessKey] [secretKey] [legs]
//   e.g.: php examples/live_acl_message_types.php 127.0.0.1:9876 AK_TEST SK_TEST_SECRET_12345678
//   legs = all | neg,normal,async,oneway,batch,delay,timer,tx,order,tag,key,prop
//
// Why the negative assertion must run first and as its own process stage: on the broker, **all**
// authentication failures (missing signature / wrong signature) collapse into the same
// NO_PERMISSION(16); only the remark distinguishes "check signature failed." from "username
// cannot be null.". And "all green with credentials" cannot distinguish "the signature is
// accepted" from "the cluster has no authentication at all" without a negative control. Hence:
//   N1 a send without credentials is rejected and the response code is **exactly 16** (not
//   "some error");
//   N2 a wrong secretKey is likewise rejected with 16.
// The negative probe uses a producer **without hooks**: RPC hooks are registered at the client
// instance's remoting-layer assembly point (after start()), so the probe client that starts
// first must **shut down cleanly and rest 1s** before the credentialed client starts, keeping
// any instance-level sharing from carrying the hook-less connection into the positive legs.
//
// Message types covered (each counts only when read back from the broker; only oneway may fall
// back to read-back counts):
//   sync normal / async callback / oneway / batch send(array) / delay level / timer
//   TIMER_DELIVER_MS / transaction (half message -> local commit -> END_TRANSACTION; reading it
//   back proves the commit took effect) / orderly sendBySelector (same arg same queue + ordered
//   consumption preserves order) / Tag filtering / server-side Key query / user property pass-through.
//
// PHP adaptation (single-threaded): all background work of the push consumer converges in
// tick(); every wait window actively ticks the consumer in a polling loop (same approach as
// live_redelivery.php); sendAsync pumps internally until the callbacks have run before returning.
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\LocalTransactionState;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\SendCallbackImpl;
use RocketMQ\Client\SendResult;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\TransactionListener;
use RocketMQ\Client\TransactionSendResult;
use RocketMQ\Client\SelectMessageQueueByHash;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\AclClientRPCHook;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\SessionCredentials;

// Keep the log out of the user's $HOME: the default is logs/ under the current working
// directory; pin it to the temp directory here.
if (getenv('ROCKETMQ_CLIENT_LOG_DIR') === false) {
    putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_acl_types_live_logs');
}

// ------------------------------------------------------------------ harness

$pass = 0;
$fail = 0;
$check = function (bool $ok, string $name, string $detail = '') use (&$pass, &$fail): void {
    echo ($ok ? 'PASS' : 'FAIL') . ' ' . $name . ($detail !== '' ? ' | ' . $detail : '') . PHP_EOL;
    if ($ok) {
        $pass++;
    } else {
        $fail++;
    }
};

$ns = $argv[1] ?? '127.0.0.1:9876';
$ak = trim((string)($argv[2] ?? ''));
$sk = trim((string)($argv[3] ?? ''));
$legsArg = trim($argv[4] ?? 'all');
if ($ak === '' || $sk === '') {
    echo 'Usage: php ' . $argv[0] . ' <namesrv> <accessKey> <secretKey> [legs]' . PHP_EOL;
    echo 'Credentials are required when the cluster has ACL enabled; every positive leg of this command authenticates via the signing hook.' . PHP_EOL;
    exit(1);
}
$want = function (string $leg) use ($legsArg): bool {
    if ($legsArg === '' || $legsArg === 'all') {
        return true;
    }
    foreach (explode(',', $legsArg) as $l) {
        if (trim($l) === $leg) {
            return true;
        }
    }
    return false;
};

$stamp = time();
$prefix = 'PhpAclMt' . $stamp;
// The hook object is stateless and can be shared across multiple client instances in the same
// process (each start() registers it into that instance's remoting layer).
$acl = new AclClientRPCHook(new SessionCredentials($ak, $sk));

/** Polls the condition within the window; ticks the passed consumers each round (PHP has no background threads). */
function mt_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
{
    $deadline = microtime(true) + $windowSec;
    for (;;) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond()) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return $cond();
        }
        usleep(100_000);
    }
}

/**
 * Walks the exception chain looking for the broker response code.
 *
 * When a sync send wraps MQBrokerException(16) in an MQClientException, only the outer layer
 * carries the same code; the admin path may wrap once more — never look at the outermost layer
 * only, and never rely on the message text.
 */
function mt_broker_code(Throwable $e): ?int
{
    $seen = [];
    $cur = $e;
    while ($cur !== null) {
        if (isset($seen[spl_object_id($cur)])) {
            break;
        }
        $seen[spl_object_id($cur)] = true;
        if (property_exists($cur, 'responseCode') && $cur->responseCode !== null) {
            return (int) $cur->responseCode;
        }
        $cur = $cur->getPrevious();
    }
    return null;
}

/** Collecting listeners: concurrent / orderly variants, accounting by body and delivery order. */
final class MtBox
{
    /** @var list<MessageExt> */
    public array $msgs = [];

    public function add(MessageExt $m): void
    {
        $this->msgs[] = $m;
    }

    /** @return list<string> bodies in delivery order */
    public function bodies(): array
    {
        return array_map(static fn (MessageExt $m): string => (string) $m->getBody(), $this->msgs);
    }

    /** @return list<string> sorted bodies (removes cross-queue disorder) */
    public function sortedBodies(): array
    {
        $b = $this->bodies();
        sort($b, SORT_STRING);
        return $b;
    }

    /** @return list<MessageExt> */
    public function ofBody(string $body): array
    {
        return array_values(array_filter(
            $this->msgs,
            static fn (MessageExt $m): bool => (string) $m->getBody() === $body
        ));
    }
}

final class MtConcurrentListener implements MessageListenerConcurrently
{
    public function __construct(private readonly MtBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class MtOrderlyListener implements MessageListenerOrderly
{
    public function __construct(private readonly MtBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $ctx): ConsumeOrderlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeOrderlyStatus::SUCCESS;
    }
}

/** Local transaction always commits: half message -> COMMIT -> END_TRANSACTION; reading it back proves the commit took effect. */
final class MtCommitListener implements TransactionListener
{
    public array $executed = [];

    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState
    {
        $this->executed[] = (string) $msg->getBody();
        return LocalTransactionState::COMMIT_MESSAGE;
    }

    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState
    {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
}

function mt_new_admin(string $ns, AclClientRPCHook $hook, string $instance): DefaultMQAdminExt
{
    $a = new DefaultMQAdminExt($hook);
    $a->setNamesrvAddr($ns);
    $a->setInstanceName($instance);
    $a->start();
    return $a;
}

function mt_new_producer(string $ns, string $group, AclClientRPCHook $hook, string $instance): DefaultMQProducer
{
    $p = new DefaultMQProducer($group, $hook);
    $p->setNamesrvAddr($ns);
    $p->setInstanceName($instance);
    $p->sendMsgTimeout = 5000;
    $p->start();
    return $p;
}

// ------------------------------------------------------------------ main

echo '=== PHP ACL message-types live: ns=' . $ns . ' ak=' . $ak
    . ' prefix=' . $prefix . ' legs=' . ($legsArg === '' ? 'all' : $legsArg) . ' ===' . PHP_EOL;

// ---------------------------------------------------------------- Stage 0: negative probe
// Must run **first**: the hook-less probe client creates and deregisters its instance before the
// credentialed client starts.
if ($want('neg')) {
    echo PHP_EOL . '--- N missing/wrong credentials must be rejected by the broker with NO_PERMISSION(16) ---' . PHP_EOL;

    $anonTopic = $prefix . '_Anon';
    $anon = new DefaultMQProducer('GID_' . $prefix . '_anon');
    $anon->setNamesrvAddr($ns);
    $anon->setInstanceName('aclmt-anon-' . $stamp);
    $anon->sendMsgTimeout = 3000;
    // Disable retries: NO_PERMISSION is in the retryable code table; leaving retries on only
    // slows the probe with repeated rejections.
    $anon->setRetryTimesWhenSendFailed(0);
    $anon->start();
    try {
        $denied = false;
        $code = null;
        $detail = '';
        try {
            $anon->send(new Message($anonTopic, 'should-not-send'));
            $detail = 'unexpected send success (is the cluster not rejecting credential-less requests?)';
        } catch (Throwable $e) {
            $code = mt_broker_code($e);
            $denied = $code === ResponseCode::NO_PERMISSION;
            $detail = get_class($e) . ' code=' . var_export($code, true) . ' msg=' . $e->getMessage();
        }
        $check($denied, 'N1 credential-less send rejected by the broker, response code exactly 16', $detail);
    } finally {
        $anon->shutdown();
    }

    $bad = new DefaultMQProducer(
        'GID_' . $prefix . '_badsk',
        new AclClientRPCHook(new SessionCredentials($ak, 'WRONG_SECRET_KEY_12345678'))
    );
    $bad->setNamesrvAddr($ns);
    $bad->setInstanceName('aclmt-badsk-' . $stamp);
    $bad->sendMsgTimeout = 3000;
    $bad->setRetryTimesWhenSendFailed(0);
    $bad->start();
    try {
        $denied = false;
        $code = null;
        $detail = '';
        try {
            $bad->send(new Message($anonTopic, 'should-not-send-either'));
            $detail = 'unexpected send success with a wrong secretKey';
        } catch (Throwable $e) {
            $code = mt_broker_code($e);
            $denied = $code === ResponseCode::NO_PERMISSION;
            $detail = get_class($e) . ' code=' . var_export($code, true) . ' msg=' . $e->getMessage();
        }
        $check($denied, 'N2 wrong secretKey rejected by the broker, response code exactly 16', $detail);
    } finally {
        $bad->shutdown();
    }

    // Start the credentialed clients only after the probe instances are shut down (rest 1s to
    // let the deregistration settle).
    sleep(1);
}

// ---------------------------------------------------------------- shared clients for positive legs
$admin = mt_new_admin($ns, $acl, 'aclmt-admin-' . $stamp);
$createdTopics = [];
$shutdowns = [];
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerName = (string)(array_key_first($ci->brokerAddrTable) ?? 'broker-a');
} catch (Throwable $e) {
    echo '!! fetchBrokerClusterInfo failed: ' . $e->getMessage() . PHP_EOL;
    $admin->shutdown();
    exit(1);
}
$check(true, 'P0 credentialed admin handshakes with the broker cluster', 'broker=' . $brokerName);

$ensureTopic = static function (string $topic, int $queues) use ($admin, $brokerName, &$createdTopics): void {
    $admin->createTopic($brokerName, $topic, $queues);
    $createdTopics[] = $topic;
};

/**
 * Starts a credentialed push consumer, ticks within the window until expect messages arrive (or
 * the window expires), then keeps ticking for an extra settle seconds (giving any unexpected
 * expect+1-th message time to surface), and returns the accounting box.
 * Read-backs always use CONSUME_FROM_FIRST_OFFSET + a brand-new group, immune to historical offsets.
 */
$readBack = static function (string $topic, string $suffix, int $expect, float $windowSec,
    string $sub = '*', bool $orderly = false, float $settleSec = 0.0)
    use ($ns, $acl, $prefix, $stamp, &$shutdowns): MtBox {
    $box = new MtBox();
    $c = new DefaultMQPushConsumer('GID_' . $prefix . '_' . $suffix, $acl);
    $c->setNamesrvAddr($ns);
    $c->setInstanceName('aclmt-rc-' . $suffix . '-' . $stamp);
    $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    // Single-threaded tick polls queues in order: suspended long polls on idle queues would burn
    // the whole window, so both ends are shortened.
    $c->pullSuspendTimeoutMillis = 1000;
    $c->pullTimeoutMillis = 3000;
    $c->setMessageListener($orderly ? new MtOrderlyListener($box) : new MtConcurrentListener($box));
    $c->subscribe($topic, $sub);
    $c->start();
    $shutdowns[] = static fn () => $c->shutdown();
    mt_wait_until(20.0, static fn () => $c->assignedQueueCount() >= 1, $c);
    mt_wait_until($windowSec, static fn (): bool => count($box->msgs) >= $expect, $c);
    if ($settleSec > 0) {
        $deadline = microtime(true) + $settleSec;
        while (microtime(true) < $deadline) {
            $c->tick();
            usleep(100_000);
        }
    }
    return $box;
};

$producer = mt_new_producer($ns, 'GID_' . $prefix . '_prod', $acl, 'aclmt-prod-' . $stamp);
$shutdowns[] = static fn () => $producer->shutdown();

// Send with retries: fresh-cluster / route-registration jitter must not masquerade as a
// message-type defect.
$sendWithRetry = static function (callable $send) use ($producer): mixed {
    $last = null;
    for ($i = 0; $i < 10; $i++) {
        try {
            return $send();
        } catch (Throwable $e) {
            $last = $e;
            // An auth rejection has nothing to retry; rethrow and let the leg assert it.
            if (mt_broker_code($e) === ResponseCode::NO_PERMISSION) {
                throw $e;
            }
            usleep(1_000_000);
        }
    }
    throw $last ?? new RuntimeException('send never succeeded');
};

// ---------------------------------------------------------------- S1 sync normal
if ($want('normal')) {
    echo PHP_EOL . '--- S1 sync normal messages ---' . PHP_EOL;
    $topic = $prefix . '_Normal';
    $ensureTopic($topic, 1);
    $ok = 0;
    foreach (['norm-0', 'norm-1', 'norm-2'] as $body) {
        $r = $sendWithRetry(static fn () => $producer->send(new Message($topic, $body)));
        $ok += $r->sendStatus === SendStatus::SEND_OK ? 1 : 0;
    }
    $check($ok === 3, 'S1 credentialed sync sends 3 messages with SEND_OK', 'ok=' . $ok);
    $box = $readBack($topic, 'normal', 3, 30.0);
    $check($box->sortedBodies() === ['norm-0', 'norm-1', 'norm-2'], 'S1 push reads back 3 messages with matching content',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S2 async
if ($want('async')) {
    echo PHP_EOL . '--- S2 sendAsync callback chain ---' . PHP_EOL;
    $topic = $prefix . '_Async';
    $ensureTopic($topic, 1);
    $okResults = [];
    $errors = [];
    foreach (['async-0', 'async-1', 'async-2'] as $body) {
        // PHP single-threaded: sendAsync pumps internally until the callbacks have run before returning.
        $producer->sendAsync(new Message($topic, $body),
            new SendCallbackImpl(static function (?SendResult $r) use (&$okResults): void {
                $okResults[] = $r;
            }, static function (Throwable $e) use (&$errors): void {
                $errors[] = $e->getMessage();
            }));
    }
    $statusOk = count($okResults) === 3 && count($errors) === 0
        && array_reduce($okResults, static fn (bool $c, ?SendResult $r): bool =>
            $c && $r !== null && $r->sendStatus === SendStatus::SEND_OK, true);
    $check($statusOk, 'S2 async callbacks fired 3 times, all SEND_OK', 'ok=' . count($okResults) . ' err=' . implode(';', $errors));
    $box = $readBack($topic, 'async', 3, 30.0);
    $check($box->sortedBodies() === ['async-0', 'async-1', 'async-2'], 'S2 async messages read back 3',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S3 oneway
if ($want('oneway')) {
    echo PHP_EOL . '--- S3 sendOneway (no reply; falls back to read-back counts) ---' . PHP_EOL;
    $topic = $prefix . '_Oneway';
    $ensureTopic($topic, 1);
    $threw = '';
    try {
        foreach (['ow-0', 'ow-1', 'ow-2'] as $body) {
            $sendWithRetry(static fn () => $producer->sendOneway(new Message($topic, $body)));
        }
    } catch (Throwable $e) {
        $threw = $e->getMessage();
    }
    $box = $readBack($topic, 'oneway', 3, 30.0);
    $check($threw === '' && $box->sortedBodies() === ['ow-0', 'ow-1', 'ow-2'],
        'S3 oneway: 3 messages accepted by the broker and read back', 'got=' . implode(',', $box->bodies()) . ' ' . $threw);
}

// ---------------------------------------------------------------- S4 batch
if ($want('batch')) {
    echo PHP_EOL . '--- S4 batch send(array) (SEND_BATCH_MESSAGE) ---' . PHP_EOL;
    $topic = $prefix . '_Batch';
    $ensureTopic($topic, 1);
    $batch = [
        new Message($topic, 'batch-0'),
        new Message($topic, 'batch-1'),
        new Message($topic, 'batch-2'),
    ];
    $r = $sendWithRetry(static fn () => $producer->send($batch));
    $check($r->sendStatus === SendStatus::SEND_OK, 'S4 batch send SEND_OK', (string) $r->sendStatus->name);
    $box = $readBack($topic, 'batch', 3, 30.0);
    $check($box->sortedBodies() === ['batch-0', 'batch-1', 'batch-2'],
        'S4 batch messages read back one by one, 3 total', 'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S5 delay level
if ($want('delay')) {
    echo PHP_EOL . '--- S5 delayed message set_delay_time_level(3)=10s ---' . PHP_EOL;
    $topic = $prefix . '_Delay';
    $ensureTopic($topic, 1);
    $sendWithRetry(static fn () => $producer->send(new Message($topic, 'delay-normal')));
    $dm = new Message($topic, 'delay-level3');
    $dm->setDelayTimeLevel(3); // level 3 = 10s in the messageDelayLevel table
    $sendWithRetry(static fn () => $producer->send($dm));

    $box = $readBack($topic, 'delay', 2, 40.0);
    $delayed = $box->ofBody('delay-level3');
    $normal = $box->ofBody('delay-normal');
    $check(count($delayed) === 1 && count($normal) === 1, 'S5 both the delayed and the control message read back',
        'delayed=' . count($delayed) . ' normal=' . count($normal));
    if ($delayed !== []) {
        $drift = $delayed[0]->storeTimestamp - $delayed[0]->bornTimestamp;
        $check($drift >= 8000, 'S5 delay took effect (store_ts-born_ts >= 8s, level 3=10s)', 'drift=' . $drift . 'ms');
    } else {
        $check(false, 'S5 delay took effect (store_ts-born_ts >= 8s, level 3=10s)', 'no delayed message to verify');
    }
    if ($normal !== []) {
        $ndrift = $normal[0]->storeTimestamp - $normal[0]->bornTimestamp;
        $check($ndrift < 5000, 'S5 control normal message was not delayed', 'drift=' . $ndrift . 'ms');
    }
}

// ---------------------------------------------------------------- S6 timer
if ($want('timer')) {
    echo PHP_EOL . '--- S6 timer message TIMER_DELIVER_MS=+5s ---' . PHP_EOL;
    $topic = $prefix . '_Timer';
    $ensureTopic($topic, 1);
    $deliverAt = (int) (microtime(true) * 1000) + 5000;
    $tm = new Message($topic, 'timer-5s');
    $tm->setUserProperty('TIMER_DELIVER_MS', (string) $deliverAt);
    $sendWithRetry(static fn () => $producer->send($tm));

    $box = $readBack($topic, 'timer', 1, 40.0);
    $hit = $box->ofBody('timer-5s');
    $check(count($hit) === 1, 'S6 timer message delivered on time and read back', 'got=' . count($hit));
    if ($hit !== []) {
        $drift = $hit[0]->storeTimestamp - $hit[0]->bornTimestamp;
        $check($drift >= 4000, 'S6 timer took effect (store_ts-born_ts >= 4s, deliver point=+5s)', 'drift=' . $drift . 'ms');
    } else {
        $check(false, 'S6 timer took effect (store_ts-born_ts >= 4s, deliver point=+5s)', 'no timer message to verify');
    }
}

// ---------------------------------------------------------------- S7 transaction
if ($want('tx')) {
    echo PHP_EOL . '--- S7 transactional message: half message -> local COMMIT -> END_TRANSACTION -> read back ---' . PHP_EOL;
    $topic = $prefix . '_Tx';
    $ensureTopic($topic, 1);
    $listener = new MtCommitListener();
    /** @var TransactionSendResult $tsr */
    $tsr = $sendWithRetry(static fn () => $producer->sendMessageInTransaction(
        new Message($topic, 'tx-commit'), $listener));
    $halfOk = $tsr->getSendStatus() === SendStatus::SEND_OK;
    $check($halfOk, 'S7 half message sent with SEND_OK',
        'status=' . $tsr->getSendStatus()->name . ' txId=' . var_export($tsr->getTransactionId(), true));
    $check($listener->executed === ['tx-commit'], 'S7 local transaction executed exactly once',
        'executed=' . implode(',', $listener->executed));
    $check($tsr->getLocalTransactionState() === LocalTransactionState::COMMIT_MESSAGE,
        'S7 final state is COMMIT_MESSAGE', var_export($tsr->getLocalTransactionState()?->name, true));
    $box = $readBack($topic, 'tx', 1, 40.0);
    $check(count($box->ofBody('tx-commit')) === 1, 'S7 after commit the message is readable from the broker (became consumable)',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S8 orderly
if ($want('order')) {
    echo PHP_EOL . '--- S8 orderly messages: sendBySelector same arg same queue + ordered consumption preserves order ---' . PHP_EOL;
    $topic = $prefix . '_Order';
    $ensureTopic($topic, 4);
    $sentOrder = [];
    $qids = [];
    for ($i = 0; $i < 10; $i++) {
        $body = sprintf('ord-%02d', $i);
        $sentOrder[] = $body;
        $r = $sendWithRetry(static fn () => $producer->sendBySelector(
            new Message($topic, $body), new SelectMessageQueueByHash(), 'shard-A'));
        if ($r->messageQueue !== null) {
            $qids[$r->messageQueue->queueId] = true;
        }
    }
    $check(count($qids) === 1, 'S8 same arg routes all messages to the same queue', 'distinct_qids=' . implode(',', array_keys($qids)));
    $box = $readBack($topic, 'order', 10, 40.0, '*', orderly: true);
    $check($box->bodies() === $sentOrder, 'S8 orderly consumption reads back 10 messages in send order',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S9 Tag filter
if ($want('tag')) {
    echo PHP_EOL . '--- S9 Tag-filtered consumption (server delivers only TagA) ---' . PHP_EOL;
    $topic = $prefix . '_Tag';
    $ensureTopic($topic, 1);
    for ($i = 0; $i < 3; $i++) {
        $sendWithRetry(static fn () => $producer->send(new Message($topic, "tagA-$i", 'TagA')));
    }
    for ($i = 0; $i < 3; $i++) {
        $sendWithRetry(static fn () => $producer->send(new Message($topic, "tagB-$i", 'TagB')));
    }
    // First read all 6 back with a * subscription (TagB must land in the queue too), then switch
    // to a TagA subscription to assert server-side filtering.
    $all = $readBack($topic, 'tagall', 6, 30.0);
    $check(count($all->msgs) === 6, 'S9 all 6 messages of both Tags landed in the queue', 'got=' . count($all->msgs));
    // Settle window: after reaching 3, keep ticking for a while so any "strayed TagB" gets a
    // chance to show up before counting.
    $onlyA = $readBack($topic, 'taga', 3, 30.0, 'TagA', false, 5.0);
    $tagsOk = count($onlyA->msgs) === 3;
    foreach ($onlyA->msgs as $m) {
        if ($m->getTags() !== 'TagA') {
            $tagsOk = false;
        }
    }
    $check($tagsOk, 'S9 TagA subscription receives only 3 messages, all TagA',
        'got=' . implode(',', $onlyA->bodies()));
}

// ---------------------------------------------------------------- S10 Key query
if ($want('key')) {
    echo PHP_EOL . '--- S10 message Key + server-side query by Key ---' . PHP_EOL;
    $topic = $prefix . '_Key';
    $ensureTopic($topic, 1);
    $key = 'ACLMTKEY_' . $stamp;
    $r = $sendWithRetry(static fn () => $producer->send(new Message($topic, 'key-payload', 'KeyTag', $key)));
    $check($r->sendStatus === SendStatus::SEND_OK, 'S10 message with Key sent SEND_OK', 'key=' . $key);
    // The broker's Key index builds asynchronously: poll until it hits.
    $found = [];
    $hit = mt_wait_until(20.0, function () use ($admin, $topic, $key, &$found): bool {
        try {
            $found = $admin->queryMessageByKey($topic, $key);
            return count($found) >= 1;
        } catch (Throwable) {
            return false;
        }
    });
    $bodies = array_map(static fn (MessageExt $m): string => (string) $m->getBody(), $found);
    $check($hit && in_array('key-payload', $bodies, true), 'S10 server-side query by Key hits',
        'returned=' . implode(',', $bodies));
    $box = $readBack($topic, 'key', 1, 30.0);
    $keysOk = count($box->msgs) === 1 && $box->msgs[0]->getKeys() === $key;
    $check($keysOk, 'S10 KEYS property of the read-back message matches', 'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S11 user properties
if ($want('prop')) {
    echo PHP_EOL . '--- S11 user property pass-through ---' . PHP_EOL;
    $topic = $prefix . '_Prop';
    $ensureTopic($topic, 1);
    for ($i = 0; $i < 3; $i++) {
        $m = new Message($topic, "prop-$i", 'P');
        $m->setUserProperty('city', 'Hangzhou');
        $m->setUserProperty('env', 'prod');
        $sendWithRetry(static fn () => $producer->send($m));
    }
    $box = $readBack($topic, 'prop', 3, 30.0);
    $propOk = count($box->msgs) === 3;
    foreach ($box->msgs as $m) {
        if ($m->getUserProperty('city') !== 'Hangzhou' || $m->getUserProperty('env') !== 'prod') {
            $propOk = false;
        }
    }
    $check($propOk, 'S11 user properties city/env pass through end to end (3 messages)', 'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- teardown
echo PHP_EOL . '--- teardown ---' . PHP_EOL;
foreach ($shutdowns as $fn) {
    try {
        $fn();
    } catch (Throwable) {
        // Teardown failures must not affect the result
    }
}
if ($createdTopics !== []) {
    $delErr = '';
    try {
        foreach ($createdTopics as $t) {
            $admin->deleteTopic($t);
        }
    } catch (Throwable $e) {
        $delErr = $e->getMessage();
    }
    $check($delErr === '', 'cleanup: delete the topics created this round', 'topics=' . count($createdTopics) . ' ' . $delErr);
}
$admin->shutdown();

echo PHP_EOL . 'PASS=' . $pass . ' FAIL=' . $fail . PHP_EOL;
exit($fail === 0 ? 0 : 1);

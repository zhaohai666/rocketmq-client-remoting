<?php

declare(strict_types=1);

// Live transaction + stale-connection probe (php).
//
//   php examples/live_transaction_idle.php [namesrv]
//
// Scenarios (fresh topics + fresh groups per run; broker log is greppable for
// END_TRANSACTION rejection remarks afterwards):
//
//   S1  transaction legs on FRESH connections (the standalone baseline):
//       S1a COMMIT  -> half SEND_OK, local commit, message consumable;
//       S1b ROLLBACK (check-back) -> never consumable;
//       S1c UNKNOW  -> broker check-back commits -> consumable.
//   S2  stale-connection recovery: plain send OK, idle 130s (the broker's
//       serverChannelMaxIdleTimeSeconds=120 kills the channel behind our back;
//       PHP sees a still-valid socket resource), then plain send again. The
//       send must recover through its own retry instead of surfacing
//       "connection closed" to the caller.
//   S3  the user's sequential-run scenario: idle 130s, then a transaction
//       COMMIT leg. The half send recovers via retry (sync, evidence of the
//       reconnect), but END_TRANSACTION is a ONEWAY with zero feedback: a
//       write into a dead socket disappears silently and the half message
//       stays invisible. This leg pins whether that loss actually happens and
//       whether the recovery covers it.
//
// Closing line `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\LocalTransactionState;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\TransactionListener;
use RocketMQ\Client\TransactionSendResult;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

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
$stamp = time();
$prefix = 'PhpTxIdle' . $stamp;
const IDLE_SECONDS = 130; // > broker serverChannelMaxIdleTimeSeconds(120)

final class TxBox
{
    /** @var list<string> */
    public array $bodies = [];

    public function add(MessageExt $m): void
    {
        $this->bodies[] = (string) $m->getBody();
    }

    public function count(string $body): int
    {
        $n = 0;
        foreach ($this->bodies as $b) {
            if ($b === $body) {
                $n++;
            }
        }
        return $n;
    }
}

final class TxDispatchListener implements TransactionListener
{
    public function __construct(private readonly string $commitBody, private readonly string $rollbackBody)
    {
    }

    public function executeLocalTransaction(Message $msg, mixed $arg = null): LocalTransactionState
    {
        $body = (string) $msg->getBody();
        return $body === $this->commitBody ? LocalTransactionState::COMMIT_MESSAGE : LocalTransactionState::UNKNOW;
    }

    public function checkLocalTransaction(Message $msg): LocalTransactionState
    {
        $body = (string) $msg->getBody();
        return $body === $this->rollbackBody ? LocalTransactionState::ROLLBACK_MESSAGE : LocalTransactionState::COMMIT_MESSAGE;
    }
}

final class CollectListener implements MessageListenerConcurrently
{
    public function __construct(private readonly TxBox $box)
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

function tx_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
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

// ---------------------------------------------------------------- setup

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerNames = array_keys($ci->brokerAddrTable);
    sort($brokerNames, SORT_STRING);
    $brokerName = $brokerNames[0] ?? 'broker-a';
    $admin->createTopic($brokerName, $prefix . '_Tx1', 1);
    $admin->createTopic($brokerName, $prefix . '_TxIdle', 1);
    $admin->createTopic($brokerName, $prefix . '_Plain', 1);
    echo "topics created on $brokerName" . PHP_EOL;
} finally {
    $admin->shutdown();
}

// One collector for the whole run (fresh groups -> FIRST_OFFSET sees everything sent after start).
$box = new TxBox();
$txConsumer = new DefaultMQPushConsumer('GID_' . $prefix . '_reader');
$txConsumer->setNamesrvAddr($ns);
$txConsumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
foreach ([$prefix . '_Tx1', $prefix . '_TxIdle', $prefix . '_Plain'] as $t) {
    $txConsumer->subscribe($t, '*');
}
$txConsumer->setMessageListener(new CollectListener($box));
$txConsumer->start();
tx_wait_until(20.0, fn (): bool => $txConsumer->assignedQueueCount() >= 3, $txConsumer);
echo 'reader assigned=' . $txConsumer->assignedQueueCount() . PHP_EOL;

$plainProducer = new DefaultMQProducer('GID_' . $prefix . '_plain');
$plainProducer->setNamesrvAddr($ns);
$plainProducer->start();

// ---------------------------------------------------------------- S1: fresh-connection legs

echo PHP_EOL . '--- S1 transaction legs on fresh connections ---' . PHP_EOL;
$s1Listener = new TxDispatchListener("s1-commit-$stamp", "s1-rollback-$stamp");
$txProducer = new DefaultMQProducer('GID_' . $prefix . '_tx');
$txProducer->setNamesrvAddr($ns);
$txProducer->start();

$r = $txProducer->sendMessageInTransaction(new Message($prefix . '_Tx1', "s1-commit-$stamp"), $s1Listener);
$check($r instanceof TransactionSendResult && $r->getSendStatus() === SendStatus::SEND_OK,
    'S1a half message sent with SEND_OK',
    $r instanceof TransactionSendResult ? 'status=' . $r->getSendStatus()->name : 'no result');
$check($r instanceof TransactionSendResult && $r->getLocalTransactionState() === LocalTransactionState::COMMIT_MESSAGE,
    'S1a local transaction executed and returned COMMIT',
    $r instanceof TransactionSendResult ? ($r->getLocalTransactionState()?->name ?? '?') : '?');
$commitSeen = tx_wait_until(40.0, fn (): bool => $box->count("s1-commit-$stamp") >= 1, $txConsumer);
$check($commitSeen, 'S1a the committed transaction message became consumable',
    'got=' . $box->count("s1-commit-$stamp"));

$r = $txProducer->sendMessageInTransaction(new Message($prefix . '_Tx1', "s1-rollback-$stamp"), $s1Listener);
$check($r instanceof TransactionSendResult && $r->getSendStatus() === SendStatus::SEND_OK,
    'S1b rollback half message sent with SEND_OK',
    $r instanceof TransactionSendResult ? 'status=' . $r->getSendStatus()->name : 'no result');
// The check-back interval is seconds-scale (transactionCheckInterval=6s); 30s covers several.
tx_wait_until(30.0, fn (): bool => false, $txConsumer);
$check($box->count("s1-rollback-$stamp") === 0, 'S1b the rolled-back message never became consumable',
    'got=' . $box->count("s1-rollback-$stamp"));

$r = $txProducer->sendMessageInTransaction(new Message($prefix . '_Tx1', "s1-unknow-$stamp"), $s1Listener);
$check($r instanceof TransactionSendResult && $r->getSendStatus() === SendStatus::SEND_OK,
    'S1c UNKNOW half message sent with SEND_OK',
    $r instanceof TransactionSendResult ? 'status=' . $r->getSendStatus()->name : 'no result');
$unknowSeen = tx_wait_until(60.0, fn (): bool => $box->count("s1-unknow-$stamp") >= 1, $txConsumer, $txProducer);
$check($unknowSeen, 'S1c the check-back committed message became consumable',
    'got=' . $box->count("s1-unknow-$stamp"));

// ---------------------------------------------------------------- S2: idle 130s -> plain send

echo PHP_EOL . "--- S2 idle " . IDLE_SECONDS . "s then plain send (stale-connection recovery) ---" . PHP_EOL;
$ok0 = false;
try {
    $res = $plainProducer->send(new Message($prefix . '_Plain', "plain-before-$stamp"));
    $ok0 = $res->sendStatus === SendStatus::SEND_OK;
} catch (Throwable $e) {
    echo '  warm-up send error: ' . $e->getMessage() . PHP_EOL;
}
$check($ok0, 'S2 the warm-up plain send succeeded', 'status=' . ($ok0 ? 'SEND_OK' : 'error'));
echo "  sleeping " . IDLE_SECONDS . "s to let the broker kill the idle channel..." . PHP_EOL;
sleep(IDLE_SECONDS);
$recovered = false;
$errorText = '';
try {
    $res = $plainProducer->send(new Message($prefix . '_Plain', "plain-after-$stamp"));
    $recovered = $res->sendStatus === SendStatus::SEND_OK;
} catch (Throwable $e) {
    $errorText = get_class($e) . ': ' . $e->getMessage();
}
$check($recovered, 'S2 the plain send after the broker-side idle kill recovered (no surfaced error)',
    $errorText !== '' ? $errorText : 'SEND_OK');
$afterSeen = tx_wait_until(40.0, fn (): bool => $box->count("plain-after-$stamp") >= 1, $txConsumer);
$check($afterSeen, 'S2 the recovered plain message was consumed', 'got=' . $box->count("plain-after-$stamp"));

// ---------------------------------------------------------------- S3: idle 130s -> transaction COMMIT

echo PHP_EOL . "--- S3 idle " . IDLE_SECONDS . "s then transaction COMMIT (the sequential-run scenario) ---" . PHP_EOL;
sleep(IDLE_SECONDS);
$s3Body = "s3-tx-commit-$stamp";
$s3Listener = new TxDispatchListener($s3Body, 'never-rollback-' . $stamp);
// A brand-new producer would dodge the stale-connection state; reuse the S1 producer so
// its broker channel is exactly what the sequential run hands the next case.
$r = null;
$errorText = '';
try {
    $r = $txProducer->sendMessageInTransaction(new Message($prefix . '_TxIdle', $s3Body), $s3Listener);
} catch (Throwable $e) {
    $errorText = get_class($e) . ': ' . $e->getMessage();
}
$check($r instanceof TransactionSendResult && $r->getSendStatus() === SendStatus::SEND_OK,
    'S3 the half message after idle sent with SEND_OK (sync path recovered)',
    $r instanceof TransactionSendResult ? 'status=' . $r->getSendStatus()->name : $errorText);
$check($r instanceof TransactionSendResult && $r->getLocalTransactionState() === LocalTransactionState::COMMIT_MESSAGE,
    'S3 the local transaction executed and returned COMMIT',
    $r instanceof TransactionSendResult ? ($r->getLocalTransactionState()?->name ?? '?') : '?');
$s3Seen = tx_wait_until(90.0, fn (): bool => $box->count($s3Body) >= 1, $txConsumer, $txProducer);
$check($s3Seen, 'S3 the committed message after idle became consumable (END_TRANSACTION was not lost)',
    'got=' . $box->count($s3Body) . ($s3Seen ? '' : ' | if this FAILs, grep the broker log for the rejection remark'));

$txConsumer->shutdown();
$txProducer->shutdown();
$plainProducer->shutdown();

echo "############ PASS=$pass FAIL=$fail ############" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

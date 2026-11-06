<?php

declare(strict_types=1);

// Recall (RECALL_MESSAGE(370)) live smoke against a real 5.5.1 cluster, mirroring
// python/verify_recall_live.py and the go/node examples:
//
//   php examples/live_recall.php [namesrv]
//   e.g.: php examples/live_recall.php 127.0.0.1:9876
//
//   R0  the broker's recallMessageEnable is readable and flipped on for the run;
//   R1  only a timer message carries a recallHandle (plain does not), and two
//       timer messages get different handles;
//   R2  the broker's handle decodes with our codec and its topic/brokerName/
//       messageId match the send target;
//   R4  a %RETRY% topic is rejected locally ("topic is not supported");
//   R5  an illegal handle is rejected locally ("recall handle is invalid"),
//       before any network round trip;
//   R3  recallMessage() returns the withdrawn message's uniqKey;
//   R6  within a window longer than the timer delay the control timer and the
//       plain message are delivered while the recalled one never arrives;
//   R7  the broker flag is restored to its original value.
//
// PHP is single-threaded: the consume-observation window ticks the push consumer
// in a polling loop.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\RecallMessageHandle;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

$ns = $argv[1] ?? '127.0.0.1:9876';
$stamp = (string) intval(microtime(true) * 1000);
$topic = 'PhpRecallLive_' . $stamp;
$group = 'GID_PhpRecallLive_' . $stamp;
$delaySec = 12;
$windowSec = $delaySec + 20;
$configKey = 'recallMessageEnable';

$pass = 0;
$fail = 0;

function check(bool $ok, string $name, string $detail = ''): void
{
    global $pass, $fail;
    if ($ok) {
        $pass++;
        echo '  PASS  ' . $name . (($detail !== '') ? ('  <- ' . $detail) : '') . PHP_EOL;
    } else {
        $fail++;
        echo '  FAIL  ' . $name . (($detail !== '') ? ('  <- ' . $detail) : '') . PHP_EOL;
    }
}

/** Ticks the passed consumers every round (PHP has no background threads). */
function wait_until(float $windowSec, callable $cond, object ...$tickables): bool
{
    $deadline = microtime(true) + $windowSec;
    while (true) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond()) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return false;
        }
        usleep(100_000);
    }
}

/** Collects the consumed bodies (recall semantics are judged by absence, so nothing may be dropped). */
final class Box
{
    /** @var list<string> */
    public array $bodies = [];

    public function add(MessageExt $m): void
    {
        $this->bodies[] = (string) $m->getBody();
    }

    public function has(string $body): bool
    {
        return in_array($body, $this->bodies, true);
    }

    public function sorted(): array
    {
        $b = $this->bodies;
        sort($b, SORT_STRING);
        return $b;
    }
}

final class CollectListener implements MessageListenerConcurrently
{
    public function __construct(private readonly Box $box)
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

echo 'recall live check on ' . $ns . PHP_EOL;

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();

$prod = new DefaultMQProducer($group . '_P');
$prod->setNamesrvAddr($ns);
$prod->setInstanceName('recall-prod-' . $stamp);
$prod->start();

$consumer = null;
$brokerAddr = '127.0.0.1:10911';
$brokerName = 'broker-a';
$original = null;

try {
    // ---- R0: recallMessageEnable readable + flipped on ----
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    $brokerName = $names[0] ?? $brokerName;
    $bd = $ci->brokerAddrTable[$brokerName] ?? null;
    $brokerAddr = ($bd !== null && isset($bd->brokerAddrs[0])) ? (string) $bd->brokerAddrs[0] : $brokerAddr;

    $original = $admin->getBrokerConfig($brokerAddr, 5000)[$configKey] ?? null;
    check($original !== null, 'R0 the broker\'s ' . $configKey . ' is readable', 'value=' . var_export($original, true));
    if ($original !== 'true') {
        $flipped = false;
        try {
            $admin->updateBrokerConfig($brokerAddr, [$configKey => 'true'], 5000);
            usleep(500_000);
            $flipped = true;
        } catch (Throwable $e) {
            check(false, 'R0 ' . $configKey . ' flipped on', $e->getMessage());
        }
        if ($flipped) {
            $now = $admin->getBrokerConfig($brokerAddr, 5000)[$configKey] ?? null;
            check($now === 'true', 'R0 ' . $configKey . ' flipped on', 'was=' . var_export($original, true));
        }
    }
    $enabled = ($admin->getBrokerConfig($brokerAddr, 5000)[$configKey] ?? null) === 'true';
    if (!$enabled) {
        check(false, 'R0 recall was not enabled on the broker, the remaining checks would be meaningless');
        exit(1);
    }

    // ---- R1: only a timer message carries a recallHandle ----
    $toRecall = new Message($topic, 'to-recall');
    $toRecall->putProperty(MessageConst::PROPERTY_TIMER_DELAY_SEC, (string) $delaySec);
    $control = new Message($topic, 'control');
    $control->putProperty(MessageConst::PROPERTY_TIMER_DELAY_SEC, (string) $delaySec);
    $plain = new Message($topic, 'plain');

    $rRecall = $prod->send($toRecall);
    $rControl = $prod->send($control);
    $rPlain = $prod->send($plain);
    $handle = $rRecall->getRecallHandle() ?? '';
    check($handle !== '', 'R1 the timer message came back with a recallHandle', 'handle=' . $handle);
    check($rPlain->getRecallHandle() === null, 'R1 the ordinary message carries no recallHandle');
    check($handle !== '' && ($rControl->getRecallHandle() ?? '') !== $handle, 'R1 the two timer messages have different handles');

    // ---- R2: the broker's handle decodes and matches the send target ----
    try {
        $parsed = RecallMessageHandle::decodeHandle($handle);
        check(true, 'R2 the broker\'s handle can be decoded by our codec');
        check($parsed->topic === $topic && $parsed->brokerName === $brokerName,
            'R2 the topic/brokerName inside the handle match the send target',
            'topic=' . $parsed->topic . ' broker=' . $parsed->brokerName);
        check($parsed->messageId === $rRecall->getMsgId(),
            'R2 the uniqKey inside the handle is exactly this message\'s UNIQ_KEY',
            'handle=' . $parsed->messageId . ' send=' . $rRecall->getMsgId());
    } catch (Throwable $e) {
        check(false, 'R2 the broker\'s handle can be decoded by our codec', $e->getMessage());
    }

    // ---- R4/R5: local validation before touching the network ----
    try {
        $prod->recallMessage('%RETRY%' . $group, $handle);
        check(false, 'R4 a %RETRY% topic is rejected', 'no exception was raised');
    } catch (MQClientException $e) {
        check($e->getMessage() === 'topic is not supported', 'R4 a %RETRY% topic is rejected', $e->getMessage());
    }
    $began = microtime(true);
    try {
        $prod->recallMessage($topic, 'not-a-handle');
        check(false, 'R5 an illegal handle is rejected locally', 'no exception was raised');
    } catch (MQClientException $e) {
        $cost = microtime(true) - $began;
        check($e->getMessage() === RecallMessageHandle::INVALID_HANDLE && $cost < 0.2,
            'R5 an illegal handle is rejected locally',
            sprintf('%s (%.3fs)', $e->getMessage(), $cost));
    }

    // ---- R3: the recall returns the withdrawn message's uniqKey ----
    try {
        $recalledId = $prod->recallMessage($topic, $handle);
        check($recalledId === $rRecall->getMsgId(),
            'R3 recallMessage returns the uniqKey of the recalled message',
            'resp=' . $recalledId . ' send=' . $rRecall->getMsgId());
    } catch (Throwable $e) {
        check(false, 'R3 recallMessage returns the uniqKey of the recalled message', $e->getMessage());
    }

    // ---- R6: the control timers arrive, the recalled one never does ----
    // The window must outlast the delay: the control message shows up when its time comes,
    // the recalled one has to stay absent for the whole window.
    $box = new Box();
    $consumer = new DefaultMQPushConsumer($group . '_C');
    $consumer->setNamesrvAddr($ns);
    $consumer->setInstanceName('recall-cons-' . $stamp);
    $consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    $consumer->pullSuspendTimeoutMillis = 1000;
    $consumer->pullTimeoutMillis = 3000;
    $consumer->subscribe($topic, '*');
    $consumer->setMessageListener(new CollectListener($box));
    $consumer->start();
    $assigned = wait_until(20.0, static fn (): bool => $consumer->assignedQueueCount() >= 1, $consumer);
    check($assigned, 'R6 the consumer got at least one assigned queue');

    // Run the FULL window (no early exit): only a whole-window absence proves the recall.
    $deadline = microtime(true) + (float) $windowSec;
    while (microtime(true) < $deadline) {
        $consumer->tick();
        usleep(100_000);
    }
    $consumer->shutdown();
    $consumer = null;

    check($box->has('control'), 'R6 the control timer message was delivered on time',
        'got=[' . implode(',', $box->sorted()) . ']');
    check($box->has('plain'), 'R6 the ordinary message was delivered');
    check(!$box->has('to-recall'), 'R6 the recalled timer message was never delivered',
        'got=[' . implode(',', $box->sorted()) . ']');
} catch (Throwable $e) {
    check(false, 'unexpected exception', $e->getMessage() . ' @ ' . basename($e->getFile()) . ':' . $e->getLine());
} finally {
    // ---- R7: restore the flag ----
    if ($original !== null) {
        $restored = false;
        try {
            $admin->updateBrokerConfig($brokerAddr, [$configKey => (string) $original], 5000);
            usleep(500_000);
            $now = $admin->getBrokerConfig($brokerAddr, 5000)[$configKey] ?? null;
            $restored = $now === (string) $original;
        } catch (Throwable $e) {
            echo '  [diag] restore failed: ' . $e->getMessage() . PHP_EOL;
        }
        check($restored, 'R7 the broker\'s ' . $configKey . ' was restored to ' . var_export($original, true));
    }
    if ($consumer !== null) {
        try {
            $consumer->shutdown();
        } catch (Throwable) {
        }
    }
    try {
        $prod->shutdown();
    } catch (Throwable) {
    }
    try {
        $admin->shutdown();
    } catch (Throwable) {
    }
}

echo PHP_EOL . 'PASS=' . $pass . ' FAIL=' . $fail . PHP_EOL;
exit($fail === 0 ? 0 : 1);

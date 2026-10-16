<?php

declare(strict_types=1);

// Onewy sends reaching a **real-time** push consumer, verified against a real 5.5.1 cluster.
//
//   php examples/live_oneway_push.php [namesrv]
//   e.g.: php examples/live_oneway_push.php 127.0.0.1:9876
//
// Why this probe exists: the existing oneway assertion (live_acl_message_types S3) starts
// its consumer **after** the sends with CONSUME_FROM_FIRST_OFFSET (history sweep). That
// never covers the plain user scenario: consumer already running -> producer fires oneway
// -> push delivery in real time.
//
// What is asserted here:
//   O1  three sendOneway() (fault-strategy queue selection) plus one ordinary sync control
//       message, all delivered to the running consumer within the window;
//   O2  the sync control arriving proves the consumer itself is healthy, so a missing
//       oneway body cannot be blamed on the consumer.
//
// PHP is single-threaded: every wait window actively ticks the consumer in a polling loop.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

$ns = $argv[1] ?? '127.0.0.1:9876';
$stamp = (string) intval(microtime(true) * 1000);
$topic = 'PhpOnewayPush_' . $stamp;
$group = 'GID_PhpOnewayPush_' . $stamp;

$pass = 0;
$fail = 0;

function check(bool $ok, string $name, string $detail = ''): void
{
    global $pass, $fail;
    if ($ok) {
        $pass++;
        echo '  PASS  ' . $name . PHP_EOL;
    } else {
        $fail++;
        echo '  FAIL  ' . $name . (($detail !== '') ? ('  <- ' . $detail) : '') . PHP_EOL;
    }
}

/** Polls the condition within the window; ticks the passed consumers each round (PHP has no background threads). */
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

/** Collects the consumed bodies. */
final class Box
{
    /** @var list<string> */
    public array $bodies = [];

    public function add(MessageExt $m): void
    {
        $this->bodies[] = (string) $m->getBody();
    }

    public function sorted(): array
    {
        $b = $this->bodies;
        sort($b, SORT_STRING);
        return $b;
    }

    public function has(string $body): bool
    {
        return in_array($body, $this->bodies, true);
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

echo 'oneway -> real-time push consumer live check on ' . $ns . PHP_EOL;

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
$brokerName = 'broker-a';
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    $brokerName = $names[0] ?? 'broker-a';
} catch (Throwable $e) {
    echo '!! fetchBrokerClusterInfo failed: ' . $e->getMessage() . PHP_EOL;
    $admin->shutdown();
    exit(1);
}

$prod = new DefaultMQProducer($group . '_P');
$prod->setNamesrvAddr($ns);
$prod->start();

$consumer = null;
try {
    $admin->createTopic($brokerName, $topic, 4);
    $routeReady = wait_until(20.0, function () use ($admin, $topic): bool {
        try {
            $r = $admin->examineTopicRoute($topic);
        } catch (Throwable) {
            return false;
        }
        $n = 0;
        foreach ($r->queueDatas as $q) {
            $n += $q->readQueueNums;
        }
        return $n >= 4;
    });
    check($routeReady, 'T0 the topic route is ready (4 queues)');

    // ---- consumer first, then sends: the real-time shape ----
    // CONSUME_FROM_LAST_OFFSET: only messages sent after startup can arrive, which is what
    // makes this a real-time test rather than a history sweep.
    $box = new Box();
    $consumer = new DefaultMQPushConsumer($group);
    $consumer->setNamesrvAddr($ns);
    $consumer->setInstanceName('onewaypush-' . $stamp);
    $consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    $consumer->pullSuspendTimeoutMillis = 1000;
    $consumer->pullTimeoutMillis = 3000;
    $consumer->subscribe($topic, '*');
    $consumer->setMessageListener(new CollectListener($box));
    $consumer->start();

    // Wait for the queue assignment: without it the consumer holds no queue and nothing
    // can ever arrive (that would masquerade as a oneway defect).
    $assigned = wait_until(20.0, static fn (): bool => $consumer->assignedQueueCount() >= 1, $consumer);
    check($assigned, 'the consumer got at least one assigned queue');

    // ---- O1: oneway via fault-strategy selection + a sync control ----
    echo PHP_EOL . '---------- O1 sendOneway (auto queue selection) + sync control ----------' . PHP_EOL;
    $onewayBodies = [];
    for ($i = 0; $i < 3; $i++) {
        $body = 'oneway-' . $i;
        $threw = '';
        try {
            $prod->sendOneway(new Message($topic, $body));
        } catch (Throwable $e) {
            $threw = $e->getMessage();
        }
        check($threw === '', 'sendOneway(' . $body . ') did not throw', $threw);
        if ($threw === '') {
            $onewayBodies[] = $body;
        }
    }
    $controlBody = 'sync-control';
    $controlOk = false;
    $controlErr = '';
    for ($try = 0; $try < 10; $try++) {
        try {
            $r = $prod->send(new Message($topic, $controlBody));
            $controlOk = $r->sendStatus === SendStatus::SEND_OK;
            if (!$controlOk) {
                $controlErr = (string) $r->sendStatus;
            }
            break;
        } catch (Throwable $e) {
            $controlErr = $e->getMessage();
            usleep(300_000);
        }
    }
    check($controlOk, 'the sync control message was sent (SEND_OK)', $controlErr);

    // Everything must arrive within the same window; the control proves the consumer is live.
    $want = 4;
    $arrived = wait_until(30.0, static fn (): bool => count($box->bodies) >= $want, $consumer);
    check($arrived, 'all 4 messages reached the running consumer (3 oneway + 1 sync control)',
        'got=' . count($box->bodies) . ' [' . implode(',', $box->sorted()) . ']');
    foreach ($onewayBodies as $body) {
        check($box->has($body), 'the oneway body ' . $body . ' was delivered in real time',
            'inbox=[' . implode(',', $box->sorted()) . ']');
    }
    check($box->has($controlBody), 'the sync control was delivered (the consumer itself is healthy)',
        'inbox=[' . implode(',', $box->sorted()) . ']');
} catch (Throwable $e) {
    check(false, 'unexpected exception', $e->getMessage());
} finally {
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

echo PHP_EOL . $pass . ' PASS / ' . $fail . ' FAIL' . PHP_EOL;
exit($fail === 0 ? 0 : 1);

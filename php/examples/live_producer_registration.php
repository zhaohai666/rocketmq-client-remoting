<?php

declare(strict_types=1);

// Producer registration as the broker sees it, verified against a real 5.5.1 cluster.
//
//   php examples/live_producer_registration.php [namesrv]
//   e.g.: php examples/live_producer_registration.php 127.0.0.1:9876
//
// Why this probe exists: HEART_BEAT(34) is the only channel by which the broker's
// ProducerManager learns a clientId, and GET_PRODUCER_CONNECTION_LIST(204) is the only
// broker-side read of that table. A group that is missing there cannot be found for
// CHECK_TRANSACTION_STATE or for a reply push, so "the producer works" (sends succeed)
// does not prove "the producer is registered".
//
// What is asserted here:
//   R1  start() alone registers: a producer that never sends is listed by 204. The route
//       landing is what triggers the beat, so this must not wait for a periodic round;
//   R2  a producer that sends registers too (the send path resolves the route first);
//   R3  the registration is maintained: once the 30s period elapses the tick pump beats
//       again, and the client is still listed;
//   R4  control: a group nobody ever registered is not listed -- the same read that
//       answers R1/R2 says "no connection" for an unknown group, so a positive here cannot
//       come from the broker listing every group.
//   R5  shutdown() deregisters: the clientId disappears from 204.
//
// PHP is single-threaded: every wait window actively ticks the instance in a polling loop.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;

$ns = $argv[1] ?? '127.0.0.1:9876';
$stamp = (string) intval(microtime(true) * 1000);
$topic = 'PhpReg_' . $stamp;
$groupSilent = 'PID_PhpReg_silent_' . $stamp;
$groupSender = 'PID_PhpReg_sender_' . $stamp;
$groupNobody = 'PID_PhpReg_nobody_' . $stamp;

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

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();

/** The clientIds the broker lists for a producer group; an unknown group reads as an empty set. */
function producerClients(DefaultMQAdminExt $admin, string $group): array
{
    try {
        $conn = $admin->examineProducerConnectionInfo($group);
    } catch (Throwable $e) {
        return [];
    }
    return array_map(static fn($c) => (string) ($c->clientId ?? ''), $conn->connectionSet);
}

/** Poll the broker's table inside the window, returning the elapsed seconds of the first hit. */
function waitRegistered(DefaultMQAdminExt $admin, string $group, string $clientId, float $windowSec, MQClientInstance $inst): ?float
{
    $begin = microtime(true);
    while (microtime(true) - $begin < $windowSec) {
        if (in_array($clientId, producerClients($admin, $group), true)) {
            return microtime(true) - $begin;
        }
        $inst->tick();
        usleep(200_000);
    }
    return null;
}

try {
    // The topic has to exist before the send: relying on the default-topic fallback drags the
    // first send past the registration window (and would mask a real registration bug as a timeout).
    $cluster = $admin->fetchBrokerClusterInfo();
    $names = array_keys($cluster->brokerAddrTable);
    sort($names, SORT_STRING);
    $brokerName = $names[0] ?? 'broker-a';
    $admin->createTopic($brokerName, $topic, 4);

    // ---------------- R1: start() with no send at all
    $silent = new DefaultMQProducer($groupSilent, null, '', [$topic]);
    $silent->setInstanceName('php-reg-silent-' . $stamp);
    $silent->setNamesrvAddr($ns);
    $silent->setSendMsgTimeout(3000);
    $t0 = microtime(true);
    $silent->start();
    $instSilent = MQClientInstance::getInstance((string) $silent->clientId);
    check($instSilent instanceof MQClientInstance, 'R1 the silent producer owns an instance', (string) $silent->clientId);
    $latSilent = $instSilent === null ? null
        : waitRegistered($admin, $groupSilent, (string) $silent->clientId, 10.0, $instSilent);
    check($latSilent !== null, 'R1 start() alone registers the producer with the broker (204)',
        'not listed within 10s; waited ' . round(microtime(true) - $t0, 1) . 's');
    check($latSilent !== null && $latSilent < 5.0, 'R1 the beat is driven by the route landing, not by a periodic round',
        $latSilent === null ? 'never listed' : sprintf('listed after %.2fs', $latSilent));

    // ---------------- R3: the 30s pump keeps it registered
    $beatsBefore = $instSilent?->heartbeatCount() ?? 0;
    $instSilent?->tick((int) (microtime(true) * 1000) + MQClientInstance::HEARTBEAT_INTERVAL_MILLIS + 1);
    check($instSilent !== null && $instSilent->heartbeatCount() > $beatsBefore,
        'R3 the tick pump drives a beat once the period elapses',
        'beats before=' . $beatsBefore . ' after=' . (string) ($instSilent?->heartbeatCount() ?? -1));
    $stillSilent = in_array((string) $silent->clientId, producerClients($admin, $groupSilent), true);
    check($stillSilent, 'R3 the client is still listed after the re-beat');

    // ---------------- R2: a producer that sends
    $sender = new DefaultMQProducer($groupSender, null, '', [$topic]);
    $sender->setInstanceName('php-reg-sender-' . $stamp);
    $sender->setNamesrvAddr($ns);
    $sender->setSendMsgTimeout(3000);
    $sender->start();
    $res = $sender->send(new Message($topic, 'reg-body'));
    check($res->sendStatus === SendStatus::SEND_OK, 'R2 the sending producer sends', (string) $res->sendStatus->name);
    $instSender = MQClientInstance::getInstance((string) $sender->clientId);
    $latSender = $instSender === null ? null
        : waitRegistered($admin, $groupSender, (string) $sender->clientId, 10.0, $instSender);
    check($latSender !== null, 'R2 the sending producer is registered too (204)',
        'not listed within 10s');

    // ---------------- R4: control on the same read
    check(producerClients($admin, $groupNobody) === [], 'R4 control: a group nobody registered is not listed',
        implode(',', producerClients($admin, $groupNobody)));

    // ---------------- R5: shutdown deregisters
    $silent->shutdown();
    $gone = null;
    $deadline = microtime(true) + 10.0;
    while (microtime(true) < $deadline) {
        if (!in_array((string) $silent->clientId, producerClients($admin, $groupSilent), true)) {
            $gone = microtime(true);
            break;
        }
        usleep(200_000);
    }
    check($gone !== null, 'R5 shutdown() removes the registration from the broker',
        'still listed after 10s');
    $sender->shutdown();
} finally {
    $admin->shutdown();
    // The clientId is ip@instanceName, so release by the id the producers actually got.
    foreach ([$silent ?? null, $sender ?? null] as $p) {
        if ($p !== null && $p->clientId !== null) {
            MQClientInstance::removeInstance($p->clientId);
        }
    }
}

echo 'PASS=' . $pass . ' FAIL=' . $fail . PHP_EOL;
exit($fail === 0 ? 0 : 1);

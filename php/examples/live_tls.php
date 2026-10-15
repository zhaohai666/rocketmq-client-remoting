<?php

declare(strict_types=1);

// Live TLS/mTLS verification: three legs covering plain_tls / ca_verify / mtls.
//
//   php examples/live_tls.php <ns> <leg> <caCert> <serverName> [clientCert] [clientKey]
//
//
// The broker is started by the scripts/run_php_live.sh tls branch:
//   leg1+2: -Dtls.enable=true -Dtls.server.certPath=... -Dtls.server.keyPath=...
//   leg3  : additionally -Dtls.client.authServer=true (requires a client certificate; a missing
//           certificate fails the handshake outright)
//
// Each leg: start the consumer and wait for assignment (B5) -> send 3 -> receive all. A handshake
// failure throws RemotingConnectException at start/first request and FAILs directly.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

$ns = $argv[1] ?? '127.0.0.1:9876';
$leg = $argv[2] ?? 'plain_tls';
$caCert = $argv[3] ?? '';
$serverName = $argv[4] ?? '127.0.0.1';
$clientCert = $argv[5] ?? '';
$clientKey = $argv[6] ?? '';

if (!in_array($leg, ['plain_tls', 'ca_verify', 'mtls'], true)) {
    fwrite(STDERR, "usage: php live_tls.php <ns> <plain_tls|ca_verify|mtls> <caCert> <serverName> [clientCert] [clientKey]" . PHP_EOL);
    exit(2);
}

$stamp = time();
$topic = 'PhpTls_' . $leg . '_' . $stamp;
$group = 'GID_PhpTls_' . $leg . '_' . $stamp;

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

echo "=== PHP TLS live: ns=$ns leg=$leg topic=$topic ===" . PHP_EOL;

// TLS options assembled per leg (semantics see RemotingClient::$tlsOptions)
$tlsOptions = match ($leg) {
    'plain_tls' => null, // test-mode: trust self-signed (default behavior)
    'ca_verify' => ['caCert' => $caCert, 'serverName' => $serverName],
    'mtls' => ['caCert' => $caCert, 'serverName' => $serverName, 'clientCert' => $clientCert, 'clientKey' => $clientKey],
};

/** Delivery collector accounting by body (object reference semantics; the listener mutates the inner table directly). */
final class TlsCollector
{
    /** @var array<string,int> */
    public array $recs = [];

    public function add(string $body): void
    {
        $this->recs[$body] = ($this->recs[$body] ?? 0) + 1;
    }
}

final class TlsListener implements MessageListenerConcurrently
{
    public function __construct(private readonly TlsCollector $collector)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->collector->add((string) $m->getBody());
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

$collector = new TlsCollector();
$shutdowns = [];
try {
    $a = new DefaultMQAdminExt();
    $a->setNamesrvAddr($ns);
    $a->start();
    try {
        $ci = $a->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        $a->createTopic($names[0] ?? 'broker-a', $topic, 4);
    } finally {
        $a->shutdown();
    }

    // Start the consumer first (B5: consumer up before sending)
    $consumer = new DefaultMQPushConsumer($group);
    $consumer->setNamesrvAddr($ns);
    $consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    $consumer->tlsEnable = true;
    $consumer->tlsOptions = $tlsOptions;
    $consumer->consumeThreadMin = 2;
    $consumer->consumeThreadMax = 4;
    $consumer->setMessageListener(new TlsListener($collector));
    $consumer->subscribe($topic, '*');
    $consumer->start();
    $shutdowns[] = static fn() => $consumer->shutdown();

    $deadline = microtime(true) + 30.0;
    while (microtime(true) < $deadline) {
        $consumer->tick();
        if ($consumer->assignedQueueCountForTopic($topic) >= 1) {
            break;
        }
        usleep(200_000);
    }
    $check($consumer->assignedQueueCountForTopic($topic) >= 1, "[$leg] consumer assigned (TLS handshake passed)",
        'assigned=' . $consumer->assignedQueueCountForTopic($topic));

    // Send 3 messages
    $p = new DefaultMQProducer('PID_PhpTls_' . $leg . '_' . $stamp);
    $p->setNamesrvAddr($ns);
    $p->tlsEnable = true;
    $p->tlsOptions = $tlsOptions;
    $p->sendMsgTimeout = 5000;
    $p->start();
    $shutdowns[] = static fn() => $p->shutdown();

    $bodies = ["tls-$leg-0", "tls-$leg-1", "tls-$leg-2"];
    $sendErr = '';
    foreach ($bodies as $i => $body) {
        try {
            $r = $p->send(new Message($topic, $body));
            if ($r->sendStatus !== SendStatus::SEND_OK) {
                $sendErr = "send #$i status=" . $r->sendStatus->name;
                break;
            }
        } catch (Throwable $e) {
            $sendErr = "send #$i " . get_class($e) . ': ' . $e->getMessage();
            break;
        }
    }
    $check($sendErr === '', "[$leg] 3 messages sent over the TLS channel", $sendErr);

    $deadline = microtime(true) + 40.0;
    while (microtime(true) < $deadline) {
        $consumer->tick();
        $got = 0;
        foreach ($bodies as $body) {
            if (($collector->recs[$body] ?? 0) >= 1) {
                $got++;
            }
        }
        if ($got === count($bodies)) {
            break;
        }
        usleep(200_000);
    }
    $gotAll = true;
    foreach ($bodies as $body) {
        if (($collector->recs[$body] ?? 0) < 1) {
            $gotAll = false;
        }
    }
    $check($gotAll, "[$leg] 3 messages received over the TLS channel", 'got=' . json_encode($collector->recs, JSON_UNESCAPED_UNICODE));
} catch (Throwable $e) {
    $check(false, "[$leg] aborted by exception", get_class($e) . ': ' . $e->getMessage());
} finally {
    foreach ($shutdowns as $fn) {
        $fn();
    }
}

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

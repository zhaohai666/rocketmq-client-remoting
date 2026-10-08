<?php

declare(strict_types=1);

// PHP 端真机 TLS/mTLS 验证：对标 nodeJs/examples/live_tls.ts 并补齐 CA 校验与 mTLS 两腿。
//
//   php examples/live_tls.php <ns> <leg> <caCert> <serverName> [clientCert] [clientKey]
//
//   leg = plain_tls | ca_verify | mtls
//     plain_tls : tlsEnable=true、无证书选项 —— Java tls.test.mode.enable=true 口径
//                 （信任 broker 自签证书），最小握手/收发全链路。
//     ca_verify : tlsOptions['caCert'] = CA 证书 —— 真校验 broker 证书链 + 主机名
//                 （tls.test.mode.enable=false 口径）。
//     mtls      : 在 ca_verify 基础上带客户端证书 —— broker 端
//                 tls.client.authServer=true 时要求出示（mTLS 双向认证）。
//
// broker 由 scripts/run_php_live.sh tls 分支启动：
//   leg1+2: -Dtls.enable=true -Dtls.server.certPath=... -Dtls.server.keyPath=...
//   leg3  : 追加 -Dtls.client.authServer=true（要求客户端证书，缺证书即握手失败）
//
// 每条腿：先起消费者等分配（B5）→ 发 3 条 → 收满。握手失败会在 start/首次请求时抛
// RemotingConnectException，直接 FAIL。
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

// TLS 选项按腿装配（语义见 RemotingClient::$tlsOptions）
$tlsOptions = match ($leg) {
    'plain_tls' => null, // test-mode：信任自签（缺省行为）
    'ca_verify' => ['caCert' => $caCert, 'serverName' => $serverName],
    'mtls' => ['caCert' => $caCert, 'serverName' => $serverName, 'clientCert' => $clientCert, 'clientKey' => $clientKey],
};

/** 按 body 记账的投递收集器（对象引用语义，listener 直接改内部表）。 */
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

    // 先起消费者（B5：先起消费者再发消息）
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
    $check($consumer->assignedQueueCountForTopic($topic) >= 1, "[$leg] 消费者分配到位（TLS 握手已过）",
        'assigned=' . $consumer->assignedQueueCountForTopic($topic));

    // 发 3 条
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
    $check($sendErr === '', "[$leg] TLS 通道发送 3 条", $sendErr);

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
    $check($gotAll, "[$leg] TLS 通道收满 3 条", 'got=' . json_encode($collector->recs, JSON_UNESCAPED_UNICODE));
} catch (Throwable $e) {
    $check(false, "[$leg] 异常中止", get_class($e) . ': ' . $e->getMessage());
} finally {
    foreach ($shutdowns as $fn) {
        $fn();
    }
}

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

<?php

declare(strict_types=1);

// PHP 端真机 Request-Reply 往返：对标 nodeJs producer request()/reply 通路。
//
//   php examples/live_request_reply.php responder <namesrv> <topic> <expect>
//   php examples/live_request_reply.php requester <namesrv> <topic> <expect>
//
// PHP 单线程 ⇒ 应答方（consumer + producer.reply）与请求方（producer.request）必须
// 是两个进程，由 scripts/run_php_live.sh 的 request_reply 腿编排：
//   responder 后台起 → 打 RESPONDER_READY（建 topic + 路由就绪）→ requester 发
//   expect 条 request() → responder 逐条 reply() 后自行退出。
//
// 协议回顾（见 src/Client/RequestReply.php）：
//   请求方给消息写 CORRELATION_ID / REPLY_TO_CLIENT(clientId) / TTL；应答方
//   createReplyMessage 派生 topic=<CLUSTER>_REPLY_TOPIC、MSG_TYPE="reply" 的消息，
//   发送换码 SEND_REPLY_MESSAGE_V2(325)，broker 按 REPLY_TO_CLIENT 把应答经
//   PUSH_REPLY_MESSAGE_TO_CLIENT(326) 推回请求方连接 —— 不经过任何订阅。
//
// 收口：requester 打 `ROUNDTRIP ok=<n> total=<n>`；ok<total 非零退出。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\RequestTimeoutException;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

$mode = $argv[1] ?? '';
$ns = $argv[2] ?? '127.0.0.1:9876';
$topic = $argv[3] ?? ('PhpReqRep' . time());
$expect = (int) ($argv[4] ?? 3);

/** 在窗口内轮询条件（PHP 无后台线程，等待必须主动驱动）。 */
function rr_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
{
    $deadline = microtime(true) + $windowSec;
    while (microtime(true) < $deadline) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond()) {
            return true;
        }
        usleep(50_000);
    }
    return false;
}

if ($mode === 'responder') {
    // ---------------------------------------------------------- 应答方进程
    // 先显式建 topic：消费者没有默认 topic 兜底（TBW102 只给生产者），缺路由
    // = 无分配 = 无消费，那会伪装成客户端 bug（B5 不变量）。
    $admin = new DefaultMQAdminExt();
    $admin->setNamesrvAddr($ns);
    $admin->start();
    try {
        $ci = $admin->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        $brokerName = $names[0] ?? 'broker-a';
        // 业务 topic + reply topic 都要预建：应答发往 <CLUSTER>_REPLY_TOPIC，
        // 没有路由时 send 会在默认 topic 兜底/重试里拖到远超请求方 TTL，
        // 应答推回去时请求已经超时摘槽（"not matched any request"）。
        $topics = [$topic, MixAll::getReplyTopic('DefaultCluster')];
        foreach ($topics as $t) {
            $route = null;
            try {
                $route = $admin->examineTopicRoute($t);
            } catch (Throwable) {
            }
            if ($route === null || count($route->brokerDatas ?? []) === 0) {
                $admin->createTopic($brokerName, $t, 4);
            }
        }
    } finally {
        $admin->shutdown();
    }
    if (!rr_wait_until(30, function () use ($ns, $topic): bool {
        $a = new DefaultMQAdminExt();
        $a->setNamesrvAddr($ns);
        $a->start();
        try {
            $r = $a->examineTopicRoute($topic);
            $r2 = $a->examineTopicRoute(MixAll::getReplyTopic('DefaultCluster'));
            return $r !== null && count($r->brokerDatas ?? []) > 0
                && $r2 !== null && count($r2->brokerDatas ?? []) > 0;
        } catch (Throwable) {
            return false;
        } finally {
            $a->shutdown();
        }
    })) {
        echo "RESPONDER_FAIL no-route-for-{$topic}" . PHP_EOL;
        exit(2);
    }

    $replies = 0;
    $errors = [];
    $counter = new ArrayObject(['replies' => 0, 'errors' => [], 'log' => []]); // 引用可变的计数器
    $producer = new DefaultMQProducer('GID_RR_Responder_' . time());
    $producer->setNamesrvAddr($ns);
    $producer->start();

    $listener = new class($producer, $counter) implements MessageListenerConcurrently {
        public function __construct(
            private readonly DefaultMQProducer $producer,
            private readonly ArrayObject $counter,
        ) {
        }

        public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
        {
            foreach ($msgs as $m) {
                try {
                    $this->replyOne($m);
                } catch (Throwable $e) {
                    $this->counter['errors'][] = $e->getMessage();
                }
            }
            return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
        }

        private function replyOne(MessageExt $m): void
        {
            $body = (string) $m->getBody();
            $t0 = microtime(true);
            try {
                $result = $this->producer->reply($m, 'reply-of-' . $body, 10000);
            } catch (Throwable $e) {
                $chain = $e->getMessage();
                for ($p = $e->getPrevious(); $p !== null; $p = $p->getPrevious()) {
                    $chain .= ' <-- ' . get_class($p) . ': ' . $p->getMessage();
                }
                $this->counter['errors'][] = sprintf(
                    'reply failed after %.2fs: %s: %s',
                    microtime(true) - $t0,
                    get_class($e),
                    $chain
                );
                return;
            }
            if ($result->sendStatus !== SendStatus::SEND_OK) {
                $this->counter['errors'][] = 'reply sendStatus=' . $result->sendStatus;
                return;
            }
            $this->counter['log'][] = sprintf(
                'replied corr=%s in %.2fs',
                (string) $m->getProperty(\RocketMQ\Common\MessageConst::PROPERTY_CORRELATION_ID),
                microtime(true) - $t0
            );
            $this->counter['replies']++;
        }
    };

    $consumer = new DefaultMQPushConsumer('GID_RR_Responder_C_' . time());
    $consumer->setNamesrvAddr($ns);
    $consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    $consumer->setMessageListener($listener);
    $consumer->subscribe($topic, '*');
    $consumer->start();

    // 暖机窗：等 rebalance 分配到位（FIRST_OFFSET + 已有路由，通常 <2s）。
    rr_wait_until(10, fn (): bool => false, $consumer);
    echo "RESPONDER_READY topic={$topic}" . PHP_EOL;

    // 收满 expect 条应答即收口；超时也退出（让编排层拿 FAIL 而不是挂死）。
    $ok = rr_wait_until(90, fn (): bool => $counter['replies'] >= $expect, $consumer);
    $consumer->shutdown();
    $producer->shutdown();
    echo 'RESPONDER_DONE replied=' . $counter['replies'] . ' expect=' . $expect
        . ' errors=' . count($counter['errors']) . PHP_EOL;
    foreach ($counter['log'] as $l) {
        echo 'RESPONDER_LOG ' . $l . PHP_EOL;
    }
    foreach (array_slice($counter['errors'], 0, 5) as $e) {
        echo 'RESPONDER_ERROR ' . $e . PHP_EOL;
    }
    exit(($ok && $counter['replies'] >= $expect) ? 0 : 1);
}

if ($mode === 'requester') {
    // ---------------------------------------------------------- 请求方进程
    $producer = new DefaultMQProducer('GID_RR_Requester_' . time());
    $producer->setNamesrvAddr($ns);
    $producer->start();

    $okCount = 0;
    $results = [];
    for ($i = 1; $i <= $expect; $i++) {
        $payload = 'request-' . $i . '-' . time();
        $msg = new Message($topic, $payload);
        try {
            $response = $producer->request($msg, 15000);
        } catch (RequestTimeoutException $e) {
            $results[] = "FAIL req={$i} TIMEOUT " . $e->getMessage();
            continue;
        } catch (Throwable $e) {
            $results[] = 'FAIL req=' . $i . ' ' . get_class($e) . ' ' . $e->getMessage();
            continue;
        }
        $respBody = (string) $response->getBody();
        $respCorr = $response->getProperty(MessageConst::PROPERTY_CORRELATION_ID);
        $reqCorr = $msg->getProperty(MessageConst::PROPERTY_CORRELATION_ID);
        if ($respBody === 'reply-of-' . $payload && $respCorr !== null && $respCorr === $reqCorr) {
            $results[] = "PASS req={$i} body-ok corr-ok topic=" . $response->topic;
            $okCount++;
        } else {
            $results[] = "FAIL req={$i} body={$respBody} corr={$respCorr} want={$reqCorr}";
        }
    }

    $producer->shutdown();
    foreach ($results as $r) {
        echo $r . PHP_EOL;
    }
    echo 'ROUNDTRIP ok=' . $okCount . ' total=' . $expect . PHP_EOL;
    exit($okCount === $expect ? 0 : 1);
}

echo "usage: php live_request_reply.php responder|requester <namesrv> <topic> <expect>" . PHP_EOL;
exit(2);

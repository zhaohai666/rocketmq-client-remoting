<?php

declare(strict_types=1);

// Live Request-Reply round-trip: the producer request()/reply path.
//
//   php examples/live_request_reply.php responder <namesrv> <topic> <expect>
//   php examples/live_request_reply.php requester <namesrv> <topic> <expect>
//
// PHP is single-threaded => the responder (consumer + producer.reply) and the requester
// (producer.request) must be two processes, orchestrated by the request_reply leg of
// scripts/run_php_live.sh:
//   responder starts in the background -> prints RESPONDER_READY (topic created + route ready)
//   -> requester sends expect request() calls -> responder replies one by one and exits on its own.
//
// Protocol recap (see src/Client/RequestReply.php):
//   the requester writes CORRELATION_ID / REPLY_TO_CLIENT(clientId) / TTL onto the message; the
//   responder's createReplyMessage derives a message with topic=<CLUSTER>_REPLY_TOPIC and
//   MSG_TYPE="reply", sending with the alternate code SEND_REPLY_MESSAGE_V2(325); the broker
//   pushes the reply back to the requester's connection via PUSH_REPLY_MESSAGE_TO_CLIENT(326)
//   according to REPLY_TO_CLIENT — it never goes through any subscription.
//
// Closing: requester prints `ROUNDTRIP ok=<n> total=<n>`; ok<total exits non-zero.
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

/** Polls the condition within the window (PHP has no background threads; waiting must be driven actively). */
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
    // ---------------------------------------------------------- responder process
    // Create the topic explicitly first: the consumer has no default-topic fallback (TBW102 is
    // only for producers); a missing route = no assignment = no consumption, which would
    // masquerade as a client bug (the B5 invariant).
    $admin = new DefaultMQAdminExt();
    $admin->setNamesrvAddr($ns);
    $admin->start();
    try {
        $ci = $admin->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        $brokerName = $names[0] ?? 'broker-a';
        // Both the business topic and the reply topic must be pre-created: replies go to
        // <CLUSTER>_REPLY_TOPIC, and without a route the send would drag through the default
        // topic fallback/retries far past the requester's TTL; by the time the reply is pushed
        // back the request has already timed out and been swept ("not matched any request").
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
    $counter = new ArrayObject(['replies' => 0, 'errors' => [], 'log' => []]); // mutable-by-reference counter
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

    // Warm-up window: wait for the rebalance assignment to land (FIRST_OFFSET + route already
    // present, usually <2s).
    rr_wait_until(10, fn (): bool => false, $consumer);
    echo "RESPONDER_READY topic={$topic}" . PHP_EOL;

    // Settle once expect replies arrive; also exits on timeout (letting the orchestration layer
    // get a FAIL rather than hanging).
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
    // ---------------------------------------------------------- requester process
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

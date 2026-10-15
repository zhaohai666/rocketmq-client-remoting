<?php

declare(strict_types=1);

// Live integration of static topics (513 createStaticTopic + endpoint rewriting), covering the
// full S1-S5 scenario set:
//
//   php examples/live_static_topic.php [namesrv]
//
// S1 createStaticTopic(513): single broker, totalQueues=8, 4 physical queues.
// S2 Route read-back: TopicQueueMappingByBroker non-empty.
// S3 topicRouteData2EndpointsForStaticTopic: 8 endpoints (0-3 -> broker-a,
//    4-7 -> __syslo__none__).
// S4 Instance rewriting: getBrokerNameFromMessageQueue resolves mock queue names to the real
//    broker; out-of-table ids and non-static topics pass through.
// S5 End to end: a plain producer sends 4, a PUSH consumer receives all 4.
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\StaticTopic;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\TopicConfig;
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
$topic = 'StaticLive_' . $stamp;
$group = 'StaticPG_' . $stamp;
$physicalQueues = 4;
$totalQueues = 8;
$brokerName = '';

echo "=== PHP static topic live: ns=$ns topic=$topic ===" . PHP_EOL;

/** The TopicQueueMappingDetail document for a single broker (513 body; field names follow the
 * server-side fastjson2 serialization convention, fixed field by field).
 * Note the keys of currIdMap/hostedQueues are gid strings — a PHP array of consecutive integer
 * keys would be json_encode'd into a JSON array and rejected by the broker's fastjson2, so
 * stdClass is required. */
$mappingDetail = static function (string $bname) use ($topic, $physicalQueues, $totalQueues): array {
    $currIdMap = new stdClass();
    $hosted = new stdClass();
    for ($gid = 0; $gid < $physicalQueues; $gid++) {
        $currIdMap->{$gid} = $gid;
        $hosted->{$gid} = [[
            'gen' => 0,
            'queueId' => $gid,
            'bname' => $bname,
            'logicOffset' => 0,
            'startOffset' => 0,
            'endOffset' => -1,
            'timeOfStart' => -1,
            'timeOfEnd' => -1,
        ]];
    }

    return [
        'topic' => $topic,
        'scope' => '__global__',
        'totalQueues' => $totalQueues,
        'bname' => $bname,
        'epoch' => 0,
        'dirty' => false,
        'currIdMap' => $currIdMap,
        'hostedQueues' => $hosted,
    ];
};

// ---------------- S1 createStaticTopic(513) ----------------
echo 'S1 starting the admin client, createStaticTopic(513)' . PHP_EOL;
$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
$masterAddr = null;
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    if ($names !== []) {
        $brokerName = $names[0];
        $ids = $ci->brokerAddrTable[$names[0]];
        ksort($ids);
        $masterAddr = $ids[0] ?? null;
    }
    $check($masterAddr !== null, 'S1 broker address resolved',
        $brokerName . '(' . ($masterAddr ?? 'none') . ')');
    if ($masterAddr === null) {
        echo 'FAIL no broker in the cluster, aborting' . PHP_EOL;
        exit(1);
    }

    $config = new TopicConfig($topic, $physicalQueues, $physicalQueues);
    $admin->createStaticTopic($masterAddr, 'TBW102', $config,
        $mappingDetail($brokerName), true);
    $check(true, 'S1 createStaticTopic(513) accepted', 'totalQueues=' . $totalQueues);
} catch (Throwable $e) {
    $check(false, 'S1 createStaticTopic(513)', get_class($e) . ': ' . $e->getMessage());
} finally {
    $admin->shutdown();
}

// ---------------- S2/S3 route read-back + endpoint-set derivation ----------------
echo 'S2/S3 route read-back + endpoint-set derivation' . PHP_EOL;
$endpoints = null;
$deadline = microtime(true) + 30;
while (microtime(true) < $deadline) {
    $probe = new DefaultMQAdminExt();
    $probe->setNamesrvAddr($ns);
    $probe->start();
    try {
        $route = $probe->examineTopicRoute($topic);
        if ($route->topicQueueMappingByBroker !== null
            && $route->topicQueueMappingByBroker !== []) {
            $endpoints = StaticTopic::topicRouteData2EndpointsForStaticTopic($topic, $route);
            break;
        }
    } catch (Throwable) {
        // The route does not carry mapping info yet; keep waiting
    } finally {
        $probe->shutdown();
    }
    sleep(2);
}
$check($endpoints !== null, 'S2 the route carries TopicQueueMappingByBroker');
if ($endpoints === null) {
    echo "PASS=$pass FAIL=$fail" . PHP_EOL;
    exit(1);
}

$mockName = StaticTopic::getMockBrokerName('__global__');
$check(count($endpoints) === $totalQueues, 'S3 total endpoints = totalQueues',
    'got=' . count($endpoints) . " want=$totalQueues");

$hostedOk = true;
for ($gid = 0; $gid < $physicalQueues; $gid++) {
    $key = StaticTopic::queueKey(new MessageQueue($topic, $mockName, $gid));
    $hostedOk = $hostedOk && ($endpoints[$key] ?? null) === $brokerName;
}
$check($hostedOk, 'S3 hosted ids 0-' . ($physicalQueues - 1) . ' -> broker');

$unhostedOk = true;
for ($gid = $physicalQueues; $gid < $totalQueues; $gid++) {
    $key = StaticTopic::queueKey(new MessageQueue($topic, $mockName, $gid));
    $unhostedOk = $unhostedOk
        && ($endpoints[$key] ?? null) === StaticTopic::LOGICAL_QUEUE_MOCK_NOT_EXIST;
}
$check($unhostedOk, 'S3 unhosted ids ' . $physicalQueues . '-' . ($totalQueues - 1)
    . ' -> __syslo__none__');

// ---------------- S4/S5 instance rewriting + end-to-end send/receive ----------------
echo 'S4/S5 instance rewriting + end-to-end send/receive' . PHP_EOL;

$bodies = [];

$listener = new class($bodies) implements MessageListenerConcurrently {
    /** @param array<string> $bodies shared by reference (single-process PHP has no concurrent race window) */
    public function __construct(private array &$bodies)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->bodies[] = $m->body;
        }

        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

$consumer = new DefaultMQPushConsumer($group);
$consumer->setNamesrvAddr($ns);
$consumer->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
$consumer->setMessageListener($listener);
$consumer->subscribe($topic, '*');
$consumer->start();
// PHP has no background threads: the push consumer only pulls messages / refreshes routes when
// actively ticked; a bare sleep = frozen.
$until = microtime(true) + 4;
while (microtime(true) < $until) {
    $consumer->tick();
    usleep(100_000);
}

$inst = $consumer->mqClient;
$rewriteOk = $inst !== null;
if ($inst !== null) {
    for ($gid = 0; $gid < $physicalQueues; $gid++) {
        $got = $inst->getBrokerNameFromMessageQueue(new MessageQueue($topic, $mockName, $gid));
        if ($got !== $brokerName) {
            $rewriteOk = false;
            break;
        }
    }
}
$check($rewriteOk, 'S4 the instance rewrites mock queues to the real broker');

// id totalQueues-1 is inside the endpoint table whose value is __syslo__none__ (protocol
// semantics: return the endpoint value); the pass-through case needs an out-of-table id
// (the unit tests here use 99).
$unhostedRewrite = $inst?->getBrokerNameFromMessageQueue(
    new MessageQueue($topic, $mockName, $totalQueues - 1));
$check($unhostedRewrite === StaticTopic::LOGICAL_QUEUE_MOCK_NOT_EXIST,
    'S4 unhosted id returns the sentinel value', (string) $unhostedRewrite);

$outsideRewrite = $inst?->getBrokerNameFromMessageQueue(
    new MessageQueue($topic, $mockName, 99));
$check($outsideRewrite === $mockName, 'S4 out-of-table id passes the mock name through', (string) $outsideRewrite);

$plainRewrite = $inst?->getBrokerNameFromMessageQueue(
    new MessageQueue('Plain_' . $stamp, $brokerName, 0));
$check($plainRewrite === $brokerName, 'S4 non-static topic passes through unchanged', (string) $plainRewrite);

$prod = new DefaultMQProducer('StaticProd_' . $stamp);
$prod->setNamesrvAddr($ns);
$prod->start();
$sent = 0;
try {
    for ($i = 0; $i < $physicalQueues; $i++) {
        $r = $prod->send(new Message($topic, 'static-' . $i));
        if ($r->sendStatus === SendStatus::SEND_OK) {
            $sent++;
        }
    }
} finally {
    $prod->shutdown();
}
$check($sent === $physicalQueues, 'S5 sent ' . $physicalQueues . ' messages', 'sent=' . $sent);

$ok = false;
$deadline = microtime(true) + 40;
while (microtime(true) < $deadline) {
    $consumer->tick();
    if (count($bodies) >= $physicalQueues) {
        $ok = true;
        break;
    }
    usleep(100_000);
}
$check($ok, 'S5 consumed all ' . $physicalQueues . ' messages', 'got=' . count($bodies));
$consumer->shutdown();

echo "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

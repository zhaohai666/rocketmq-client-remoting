<?php

declare(strict_types=1);

// PHP 端静态主题（513 createStaticTopic + 端点重写）真机联调：
// 七端口径的 php 版，对应 python/verify_static_topic_live.py、
// rust/examples/live_static_topic.rs、cpp/examples/live_static_topic.cpp、
// csharp/examples/LiveStaticTopic.cs（同一套场景 S1-S5）。
//
//   php examples/live_static_topic.php [namesrv]
//
// S1 createStaticTopic(513)：单 broker，totalQueues=8，物理 4 队列。
// S2 路由回读：TopicQueueMappingByBroker 非空。
// S3 topicRouteData2EndpointsForStaticTopic：8 条端点（0-3 → broker-a，
//    4-7 → __syslo__none__）。
// S4 实例重写：getBrokerNameFromMessageQueue 对 mock 队列名解析到真实
//    broker；表外 id 与非静态主题透传。
// S5 端到端：普通生产者发 4 条、PUSH 消费者收齐 4 条。
//
// 收口行 `PASS=<n> FAIL=<n>`；任一 FAIL 非 0 退出。
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

/** 单 broker 的 TopicQueueMappingDetail 文档（513 body，Java
 * TopicQueueMappingDetail 的 fastjson2 字段名，与 python 参考版逐字段一致）。
 * 注意 currIdMap/hostedQueues 的键是 gid 字符串——PHP 连续整数键数组会被
 * json_encode 编成 JSON 数组，broker 的 fastjson2 会拒收，必须用 stdClass。 */
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
echo 'S1 起管理端，createStaticTopic(513)' . PHP_EOL;
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
    $check($masterAddr !== null, 'S1 broker 地址解析',
        $brokerName . '(' . ($masterAddr ?? '无') . ')');
    if ($masterAddr === null) {
        echo 'FAIL 集群里没有任何 broker，中止' . PHP_EOL;
        exit(1);
    }

    $config = new TopicConfig($topic, $physicalQueues, $physicalQueues);
    $admin->createStaticTopic($masterAddr, 'TBW102', $config,
        $mappingDetail($brokerName), true);
    $check(true, 'S1 createStaticTopic(513) 被接受', 'totalQueues=' . $totalQueues);
} catch (Throwable $e) {
    $check(false, 'S1 createStaticTopic(513)', get_class($e) . ': ' . $e->getMessage());
} finally {
    $admin->shutdown();
}

// ---------------- S2/S3 路由回读 + 端点集推导 ----------------
echo 'S2/S3 路由回读 + 端点集推导' . PHP_EOL;
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
        // 路由还没带上 mapping 信息，继续等
    } finally {
        $probe->shutdown();
    }
    sleep(2);
}
$check($endpoints !== null, 'S2 路由携带 TopicQueueMappingByBroker');
if ($endpoints === null) {
    echo "PASS=$pass FAIL=$fail" . PHP_EOL;
    exit(1);
}

$mockName = StaticTopic::getMockBrokerName('__global__');
$check(count($endpoints) === $totalQueues, 'S3 端点总数 = totalQueues',
    'got=' . count($endpoints) . " want=$totalQueues");

$hostedOk = true;
for ($gid = 0; $gid < $physicalQueues; $gid++) {
    $key = StaticTopic::queueKey(new MessageQueue($topic, $mockName, $gid));
    $hostedOk = $hostedOk && ($endpoints[$key] ?? null) === $brokerName;
}
$check($hostedOk, 'S3 承载 id 0-' . ($physicalQueues - 1) . ' → broker');

$unhostedOk = true;
for ($gid = $physicalQueues; $gid < $totalQueues; $gid++) {
    $key = StaticTopic::queueKey(new MessageQueue($topic, $mockName, $gid));
    $unhostedOk = $unhostedOk
        && ($endpoints[$key] ?? null) === StaticTopic::LOGICAL_QUEUE_MOCK_NOT_EXIST;
}
$check($unhostedOk, 'S3 无承载 id ' . $physicalQueues . '-' . ($totalQueues - 1)
    . ' → __syslo__none__');

// ---------------- S4/S5 实例重写 + 端到端收发 ----------------
echo 'S4/S5 实例重写 + 端到端收发' . PHP_EOL;

$bodies = [];

$listener = new class($bodies) implements MessageListenerConcurrently {
    /** @param array<string> $bodies 引用共享（单进程 PHP 无并发竞争窗口） */
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
// PHP 无后台线程：push 消费者要主动 tick 才会拉消息/刷路由，裸 sleep = 冻住。
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
$check($rewriteOk, 'S4 实例把 mock 队列重写到真实 broker');

// id totalQueues-1 在端点表内，值就是 __syslo__none__（Java 语义：返回端点值）；
// 透传用例要用表外 id（Go 单测用 99）。
$unhostedRewrite = $inst?->getBrokerNameFromMessageQueue(
    new MessageQueue($topic, $mockName, $totalQueues - 1));
$check($unhostedRewrite === StaticTopic::LOGICAL_QUEUE_MOCK_NOT_EXIST,
    'S4 无承载 id 返回哨兵值', (string) $unhostedRewrite);

$outsideRewrite = $inst?->getBrokerNameFromMessageQueue(
    new MessageQueue($topic, $mockName, 99));
$check($outsideRewrite === $mockName, 'S4 表外 id 透传 mock 名', (string) $outsideRewrite);

$plainRewrite = $inst?->getBrokerNameFromMessageQueue(
    new MessageQueue('Plain_' . $stamp, $brokerName, 0));
$check($plainRewrite === $brokerName, 'S4 非静态主题原样透传', (string) $plainRewrite);

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
$check($sent === $physicalQueues, 'S5 发送 ' . $physicalQueues . ' 条', 'sent=' . $sent);

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
$check($ok, 'S5 消费收齐 ' . $physicalQueues . ' 条', 'got=' . count($bodies));
$consumer->shutdown();

echo "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

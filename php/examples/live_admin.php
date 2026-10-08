<?php

declare(strict_types=1);

// PHP 端 DefaultMQAdminExt 真机冒烟：对标 go/examples/live_admin 的场景面（精简版）。
//
//   php examples/live_admin.php [namesrv]
//
// 覆盖：集群探活、topic CRUD + 路由、topic 列表、topicConfig、集群归属、
// broker 配置/运行时 stats、KV 配置 CRUD、订阅组 CRUD、发消息 + topic 统计、
// 位点/时间戳查询、KEYS 索引查询 + viewMessage、deleteTopic 收尾。
//
// 本工具自己造 topic + 订阅组 + KV namespace，跑完自己删（名字带时间戳），
// 可以反复跑；中途失败残留的只是垃圾 topic，不影响下次。
//
// 收口行 `PASS=<n> FAIL=<n> TOPIC=<topic>`；任一 FAIL 非 0 退出。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\SubscriptionGroupConfig;

$pass = 0;
$fail = 0;
/** 统一签名：check(bool $ok, string $name, string $detail = '')。 */
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
$topic = 'PhpAdmin_' . $stamp;
$group = 'GID_PhpAdmin_' . $stamp;
$kvns = 'PHP_ADMIN_KV_' . $stamp;

echo "=== PHP admin live: ns=$ns topic=$topic ===" . PHP_EOL;

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();

$created = false;
try {
    // ---------- 1) 集群探活 ----------
    $masterAddr = null;
    $clusterName = null;
    $ci = null;
    try {
        $ci = $admin->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        if ($names !== []) {
            $ids = $ci->brokerAddrTable[$names[0]];
            ksort($ids);
            $masterAddr = $ids[0] ?? null; // brokerId 0 = master
            $clusterName = array_key_first($ci->clusterAddrTable);
        }
    } catch (Throwable $e) {
        $check(false, '集群探活 fetchBrokerClusterInfo', $e->getMessage());
    }
    $check($masterAddr !== null, '集群探活 fetchBrokerClusterInfo',
        'master=' . ($masterAddr ?? '无') . ' cluster=' . ($clusterName ?? '?'));
    if ($masterAddr === null) {
        echo 'FAIL 集群里没有任何 broker，中止' . PHP_EOL;
        exit(1);
    }
    $brokerName = array_key_first($ci->brokerAddrTable);

    // ---------- 2) topic CRUD + 路由 ----------
    try {
        $admin->createTopic($brokerName, $topic, 4);
        $created = true;
        $check(true, 'CreateTopic(queueNum=4)', 'broker=' . $brokerName);
    } catch (Throwable $e) {
        $check(false, 'CreateTopic(queueNum=4)', $e->getMessage());
    }

    try {
        $route = $admin->examineTopicRoute($topic);
        $qnums = [];
        foreach ($route->queueDatas ?? [] as $qd) {
            $qnums[] = $qd->readQueueNums;
        }
        $all4 = $qnums !== [] && count(array_unique($qnums)) === 1 && $qnums[0] === 4;
        $check($all4 && count($route->brokerDatas ?? []) > 0, '路由 readQueueNums==4',
            'queueDatas=' . json_encode($qnums) . ' brokerDatas=' . count($route->brokerDatas ?? []));
    } catch (Throwable $e) {
        $check(false, '路由 readQueueNums==4', $e->getMessage());
    }

    try {
        $tl = $admin->fetchAllTopicList();
        $hit = in_array($topic, $tl->topicList ?? [], true);
        $check($hit, '新 topic 出现在 FetchAllTopicList', 'topics=' . count($tl->topicList ?? []));
    } catch (Throwable $e) {
        $check(false, '新 topic 出现在 FetchAllTopicList', $e->getMessage());
    }

    try {
        $tc = $admin->examineTopicConfig($masterAddr, $topic);
        $check($tc->readQueueNums === 4 && $tc->writeQueueNums === 4, 'TopicConfig 队列数与创建一致',
            "read={$tc->readQueueNums} write={$tc->writeQueueNums}");
    } catch (Throwable $e) {
        $check(false, 'TopicConfig 队列数与创建一致', $e->getMessage());
    }

    try {
        $cl = $admin->getTopicClusterList($topic);
        // 返回的是 list<string>（不是按集群名键控的 map），用 in_array 判定
        $check(in_array($clusterName, $cl, true), 'GetTopicClusterList 含本集群',
            'clusters=' . implode(',', $cl));
    } catch (Throwable $e) {
        $check(false, 'GetTopicClusterList 含本集群', $e->getMessage());
    }

    // ---------- 3) broker 配置 / 运行时 stats ----------
    try {
        $cfg = $admin->getBrokerConfig($masterAddr);
        $check($cfg !== [] && ($cfg['brokerName'] ?? '') !== '', 'getBrokerConfig 解析出非空 k=v',
            'keys=' . count($cfg));
    } catch (Throwable $e) {
        $check(false, 'getBrokerConfig 解析出非空 k=v', $e->getMessage());
    }

    try {
        $rt = $admin->fetchBrokerRuntimeStats($masterAddr);
        $check(count($rt->table ?? []) > 0, 'FetchBrokerRuntimeStats 非空',
            'entries=' . count($rt->table ?? []));
    } catch (Throwable $e) {
        $check(false, 'FetchBrokerRuntimeStats 非空', $e->getMessage());
    }

    // ---------- 4) KV 配置 CRUD ----------
    try {
        $admin->putKvConfig($kvns, 'k1', 'v1');
        $v = $admin->getKvConfig($kvns, 'k1');
        $check($v === 'v1', 'KV 值往返一致', 'v=' . var_export($v, true));

        $kvt = $admin->getKvListByNamespace($kvns);
        $check(($kvt->table['k1'] ?? null) === 'v1', 'GetKVListByNamespace 含 k1',
            'table=' . json_encode($kvt->table ?? []));

        $admin->deleteKvConfig($kvns, 'k1');
        $gone = $admin->getKvConfig($kvns, 'k1');
        $check($gone === null, 'KV 删除后不再存在', 'after=' . var_export($gone, true));
    } catch (Throwable $e) {
        $check(false, 'KV 配置 CRUD', $e->getMessage());
    }

    // ---------- 5) 订阅组 CRUD ----------
    try {
        $sgc = new SubscriptionGroupConfig();
        $sgc->groupName = $group;
        $sgc->retryMaxTimes = 5;
        $sgc->retryQueueNums = 1;
        $admin->createAndUpdateSubscriptionGroupConfig($masterAddr, $sgc);
        $got = $admin->getSubscriptionGroupConfig($masterAddr, $group);
        $check($got !== null && $got->retryMaxTimes === 5, '订阅组 retryMaxTimes 往返一致',
            'got=' . ($got !== null ? 'retryMaxTimes=' . $got->retryMaxTimes : 'null'));
    } catch (Throwable $e) {
        $check(false, '订阅组 retryMaxTimes 往返一致', $e->getMessage());
    }

    // ---------- 6) 发消息 + topic 统计 + 位点/时间戳查询 ----------
    $sentMsg = null;
    try {
        $p = new DefaultMQProducer('PID_PhpAdmin_' . $stamp);
        $p->setNamesrvAddr($ns);
        $p->start();
        for ($i = 0; $i < 3; $i++) {
            $m = new Message($topic, 'php-admin-body-' . $i, 'TagA', 'PHPADMIN' . $stamp . $i);
            $r = $p->send($m);
            if ($r->sendStatus !== SendStatus::SEND_OK) {
                throw new RuntimeException('send status=' . $r->sendStatus->name);
            }
            if ($i === 0) {
                $sentMsg = $m;
            }
        }
        $p->shutdown();

        $stats = $admin->examineTopicStats($topic);
        // 3 条消息轮询散到多条队列：按 SUM 断言（取 MAX 会在分散时恒为 1）
        $totalSum = 0;
        $totalMax = 0;
        foreach ($stats->offsetTable ?? [] as $e) {
            $totalSum += $e['value']->maxOffset;
            $totalMax = max($totalMax, $e['value']->maxOffset);
        }
        $check($totalSum >= 3, 'ExamineTopicStats 汇总 maxOffset 总和>=3',
            'queues=' . count($stats->offsetTable ?? []) . " sum=$totalSum max=$totalMax putTps={$stats->topicPutTps}");

        // 选一条非空队列做位点/时间戳查询
        $q = null;
        foreach ($stats->offsetTable ?? [] as $e) {
            if ($e['value']->maxOffset > 0) {
                $q = $e['mq'];
                break;
            }
        }
        $check($q !== null, '选出一条非空队列',
            $q === null ? '' : (($q->topic ?? '?') . '#' . ($q->queueId ?? '?') . '@' . ($q->brokerName ?? '?')));
        if ($q instanceof MessageQueue) {
            $maxOff = $admin->maxOffset($q);
            $minOff = $admin->minOffset($q);
            $check($maxOff >= $minOff && $maxOff >= 1, 'MaxOffset >= MinOffset 且队列非空',
                "max=$maxOff min=$minOff");

            $now = (int) (microtime(true) * 1000);
            $up = $admin->searchOffset($q, $now);
            $check($up === $maxOff, '时间戳=now 时边界塌到 maxOffset（队尾之后的下一个位点）',
                "up=$up max=$maxOff");
            $early = $admin->searchOffset($q, 0);
            $check($early === $minOff, '时间戳早于全部消息时塌到 minOffset', "early=$early min=$minOff");
            $check($up !== $early, '两个时间戳边界确实不同', "up=$up early=$early");

            $est = $admin->earliestMsgStoreTime($q);
            $check($est > 0, 'EarliestMsgStoreTime 非零', "est=$est");

            // 全新组、无消费者：broker 侧没有已提交位点
            $off = $admin->examineConsumerOffset($group, $q);
            $check($off === null, '无消费时位点未提交（null）', 'off=' . var_export($off, true));
        }
    } catch (Throwable $e) {
        $check(false, '发消息 + 统计/位点查询', get_class($e) . ': ' . $e->getMessage());
    }

    // ---------- 7) KEYS 索引查询 + viewMessage ----------
    try {
        $key = 'PHPADMIN' . $stamp . '0';
        $msgs = [];
        // 索引文件异步构建，轮询回读
        for ($try = 0; $try < 8; $try++) {
            $msgs = $admin->queryMessageByKey($topic, $key, 32);
            if (count($msgs) >= 1) {
                break;
            }
            sleep(1);
        }
        $check(count($msgs) >= 1, 'QueryMessageByKey 命中', count($msgs) . ' 条');
        if (count($msgs) >= 1 && $sentMsg !== null) {
            $view = $admin->viewMessage($topic, $msgs[0]->msgId);
            $check($view->getBody() === $sentMsg->getBody(), 'viewMessage body 与索引一致',
                'body=' . substr((string) $view->getBody(), 0, 32));
        }
    } catch (Throwable $e) {
        $check(false, 'QueryMessageByKey/viewMessage', $e->getMessage());
    }

    // ---------- 8) 收尾：deleteTopic ----------
    if ($created) {
        try {
            $admin->deleteTopic($topic);
            $gone = false;
            for ($try = 0; $try < 5; $try++) {
                $tl = $admin->fetchAllTopicList();
                if (!in_array($topic, $tl->topicList ?? [], true)) {
                    $gone = true;
                    break;
                }
                sleep(1);
            }
            $check($gone, 'deleteTopic 后 topic 消失', 'topic=' . $topic);
        } catch (Throwable $e) {
            $check(false, 'deleteTopic 后 topic 消失', $e->getMessage());
        }
    }
} finally {
    $admin->shutdown();
}

echo PHP_EOL . "PASS=$pass FAIL=$fail TOPIC=$topic" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

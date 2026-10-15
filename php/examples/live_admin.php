<?php

declare(strict_types=1);

// DefaultMQAdminExt live smoke test (a reduced scenario surface).
//
//   php examples/live_admin.php [namesrv]
//
// Covers: cluster liveness, topic CRUD + route, topic list, topicConfig, cluster membership,
// broker config/runtime stats, KV config CRUD, subscription group CRUD, send message + topic
// stats, offset/timestamp queries, KEYS index query + viewMessage, deleteTopic teardown.
//
// The tool creates its own topic + subscription group + KV namespace and deletes them when done
// (names carry a timestamp), so it is rerunnable; leftovers from mid-run failures are just junk
// topics and do not affect the next run.
//
// Closing line `PASS=<n> FAIL=<n> TOPIC=<topic>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\SubscriptionGroupConfig;

$pass = 0;
$fail = 0;
/** Uniform signature: check(bool $ok, string $name, string $detail = ''). */
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
    // ---------- 1) cluster liveness ----------
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
        $check(false, 'cluster liveness fetchBrokerClusterInfo', $e->getMessage());
    }
    $check($masterAddr !== null, 'cluster liveness fetchBrokerClusterInfo',
        'master=' . ($masterAddr ?? 'none') . ' cluster=' . ($clusterName ?? '?'));
    if ($masterAddr === null) {
        echo 'FAIL no broker in the cluster, aborting' . PHP_EOL;
        exit(1);
    }
    $brokerName = array_key_first($ci->brokerAddrTable);

    // ---------- 2) topic CRUD + route ----------
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
        $check($all4 && count($route->brokerDatas ?? []) > 0, 'route readQueueNums==4',
            'queueDatas=' . json_encode($qnums) . ' brokerDatas=' . count($route->brokerDatas ?? []));
    } catch (Throwable $e) {
        $check(false, 'route readQueueNums==4', $e->getMessage());
    }

    try {
        $tl = $admin->fetchAllTopicList();
        $hit = in_array($topic, $tl->topicList ?? [], true);
        $check($hit, 'new topic appears in FetchAllTopicList', 'topics=' . count($tl->topicList ?? []));
    } catch (Throwable $e) {
        $check(false, 'new topic appears in FetchAllTopicList', $e->getMessage());
    }

    try {
        $tc = $admin->examineTopicConfig($masterAddr, $topic);
        $check($tc->readQueueNums === 4 && $tc->writeQueueNums === 4, 'TopicConfig queue counts match creation',
            "read={$tc->readQueueNums} write={$tc->writeQueueNums}");
    } catch (Throwable $e) {
        $check(false, 'TopicConfig queue counts match creation', $e->getMessage());
    }

    try {
        $cl = $admin->getTopicClusterList($topic);
        // Returns a list<string> (not a map keyed by cluster name); use in_array to decide
        $check(in_array($clusterName, $cl, true), 'GetTopicClusterList contains this cluster',
            'clusters=' . implode(',', $cl));
    } catch (Throwable $e) {
        $check(false, 'GetTopicClusterList contains this cluster', $e->getMessage());
    }

    // ---------- 3) broker config / runtime stats ----------
    try {
        $cfg = $admin->getBrokerConfig($masterAddr);
        $check($cfg !== [] && ($cfg['brokerName'] ?? '') !== '', 'getBrokerConfig parses non-empty k=v',
            'keys=' . count($cfg));
    } catch (Throwable $e) {
        $check(false, 'getBrokerConfig parses non-empty k=v', $e->getMessage());
    }

    try {
        $rt = $admin->fetchBrokerRuntimeStats($masterAddr);
        $check(count($rt->table ?? []) > 0, 'FetchBrokerRuntimeStats non-empty',
            'entries=' . count($rt->table ?? []));
    } catch (Throwable $e) {
        $check(false, 'FetchBrokerRuntimeStats non-empty', $e->getMessage());
    }

    // ---------- 4) KV config CRUD ----------
    try {
        $admin->putKvConfig($kvns, 'k1', 'v1');
        $v = $admin->getKvConfig($kvns, 'k1');
        $check($v === 'v1', 'KV value round-trips unchanged', 'v=' . var_export($v, true));

        $kvt = $admin->getKvListByNamespace($kvns);
        $check(($kvt->table['k1'] ?? null) === 'v1', 'GetKVListByNamespace contains k1',
            'table=' . json_encode($kvt->table ?? []));

        $admin->deleteKvConfig($kvns, 'k1');
        $gone = $admin->getKvConfig($kvns, 'k1');
        $check($gone === null, 'KV no longer exists after deletion', 'after=' . var_export($gone, true));
    } catch (Throwable $e) {
        $check(false, 'KV config CRUD', $e->getMessage());
    }

    // ---------- 5) subscription group CRUD ----------
    try {
        $sgc = new SubscriptionGroupConfig();
        $sgc->groupName = $group;
        $sgc->retryMaxTimes = 5;
        $sgc->retryQueueNums = 1;
        $admin->createAndUpdateSubscriptionGroupConfig($masterAddr, $sgc);
        $got = $admin->getSubscriptionGroupConfig($masterAddr, $group);
        $check($got !== null && $got->retryMaxTimes === 5, 'subscription group retryMaxTimes round-trips unchanged',
            'got=' . ($got !== null ? 'retryMaxTimes=' . $got->retryMaxTimes : 'null'));
    } catch (Throwable $e) {
        $check(false, 'subscription group retryMaxTimes round-trips unchanged', $e->getMessage());
    }

    // ---------- 6) send message + topic stats + offset/timestamp queries ----------
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
        // 3 messages round-robin across multiple queues: assert on SUM (MAX would always be 1 when spread)
        $totalSum = 0;
        $totalMax = 0;
        foreach ($stats->offsetTable ?? [] as $e) {
            $totalSum += $e['value']->maxOffset;
            $totalMax = max($totalMax, $e['value']->maxOffset);
        }
        $check($totalSum >= 3, 'ExamineTopicStats aggregated maxOffset sum>=3',
            'queues=' . count($stats->offsetTable ?? []) . " sum=$totalSum max=$totalMax putTps={$stats->topicPutTps}");

        // Pick a non-empty queue for the offset/timestamp queries
        $q = null;
        foreach ($stats->offsetTable ?? [] as $e) {
            if ($e['value']->maxOffset > 0) {
                $q = $e['mq'];
                break;
            }
        }
        $check($q !== null, 'picked a non-empty queue',
            $q === null ? '' : (($q->topic ?? '?') . '#' . ($q->queueId ?? '?') . '@' . ($q->brokerName ?? '?')));
        if ($q instanceof MessageQueue) {
            $maxOff = $admin->maxOffset($q);
            $minOff = $admin->minOffset($q);
            $check($maxOff >= $minOff && $maxOff >= 1, 'MaxOffset >= MinOffset and the queue is non-empty',
                "max=$maxOff min=$minOff");

            $now = (int) (microtime(true) * 1000);
            $up = $admin->searchOffset($q, $now);
            $check($up === $maxOff, 'timestamp=now collapses to maxOffset (next offset past the tail)',
                "up=$up max=$maxOff");
            $early = $admin->searchOffset($q, 0);
            $check($early === $minOff, 'timestamp earlier than all messages collapses to minOffset', "early=$early min=$minOff");
            $check($up !== $early, 'the two timestamp boundaries truly differ', "up=$up early=$early");

            $est = $admin->earliestMsgStoreTime($q);
            $check($est > 0, 'EarliestMsgStoreTime non-zero', "est=$est");

            // Brand-new group, no consumers: the broker side has no committed offset
            $off = $admin->examineConsumerOffset($group, $q);
            $check($off === null, 'no consumption yet, offset uncommitted (null)', 'off=' . var_export($off, true));
        }
    } catch (Throwable $e) {
        $check(false, 'send message + stats/offset queries', get_class($e) . ': ' . $e->getMessage());
    }

    // ---------- 7) KEYS index query + viewMessage ----------
    try {
        $key = 'PHPADMIN' . $stamp . '0';
        $msgs = [];
        // The index file builds asynchronously; poll for the read-back
        for ($try = 0; $try < 8; $try++) {
            $msgs = $admin->queryMessageByKey($topic, $key, 32);
            if (count($msgs) >= 1) {
                break;
            }
            sleep(1);
        }
        $check(count($msgs) >= 1, 'QueryMessageByKey hits', count($msgs) . ' message(s)');
        if (count($msgs) >= 1 && $sentMsg !== null) {
            $view = $admin->viewMessage($topic, $msgs[0]->msgId);
            $check($view->getBody() === $sentMsg->getBody(), 'viewMessage body matches the index',
                'body=' . substr((string) $view->getBody(), 0, 32));
        }
    } catch (Throwable $e) {
        $check(false, 'QueryMessageByKey/viewMessage', $e->getMessage());
    }

    // ---------- 8) teardown: deleteTopic ----------
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
            $check($gone, 'topic disappears after deleteTopic', 'topic=' . $topic);
        } catch (Throwable $e) {
            $check(false, 'topic disappears after deleteTopic', $e->getMessage());
        }
    }
} finally {
    $admin->shutdown();
}

echo PHP_EOL . "PASS=$pass FAIL=$fail TOPIC=$topic" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

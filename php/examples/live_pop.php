<?php

declare(strict_types=1);

// PHP 端真机 POP 消费验证：对标 go/examples/live_pop（S1/S2 全口径）。
//
//   php examples/live_pop.php [namesrv] [legs]     # legs = all | s1,s2
//
// POP 是**唯一一条失败全静默**的消费路径：客户端没有位点可回读、没有 send-back
// 可观察、也没有错误码可断言——broker 交出一批"不可见"消息，客户端要么 ACK 要么
// 申请延长不可见时间。POP_CK 差一段，每个 ACK 在 broker 侧都是空操作，而客户端一路
// 报成功；唯一症状是消息在 popInvisibleTime 之后又回来。所以判据只能是**计时观察
// 窗口 + broker 侧可见状态**，绝不是返回码：
//
//   S1 ACK 真的生效：消费 6 条后继续盯 2.5x 不可见窗口，断言没有任何 body 被投
//      第二次。若 ACK 用错段（把批次起始 0 当消息自身 offset）或用错 topic，broker
//      会在窗口内复活整批重投。同腿钉住"POP 不持有客户端位点表"。
//   S2 失败退避：只对一条消息首投 RECONSUME_LATER → changePopInvisibleTime 延长
//      窗口 → revive 搬进 %RETRY%<group>_<topic> → 重投回来时 POP_CK 的 retry
//      marker 必须是 "1"（既是"来自重试 topic"的证据，也是"后续 ACK 必须发回
//      重试 topic"的原因），且 1ST_POP_TIME 保留 broker 的值不被改写。
//
// 集群硬前提（POP 专用 broker.conf，见 scripts/run_php_live.sh pop 分支）：
//   timerWheelEnable=true / defaultMessageRequestMode=PULL（PHP 端 401 由本工具经
//   Admin 显式发送）/ popResponseReturnActualRetryTopic=false / enablePopBatchAck=false。
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
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;

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
$legsArg = trim($argv[2] ?? 'all');
$stamp = time();
$prefix = 'PhpPop' . $stamp;

// ------------------------------------------------------------------ 记账

/** 一次投递 = listener 视角的 body / topic / recon / POP_CK / 1ST_POP_TIME / 时刻。 */
final class PopBox
{
    /** @var list<array{body:string,topic:string,recon:int,ck:string,firstPop:string,at:float}> */
    public array $recs = [];

    public function add(Message $m): void
    {
        $this->recs[] = [
            'body' => (string) $m->getBody(),
            'topic' => (string) $m->getTopic(),
            'recon' => $m instanceof MessageExt ? $m->reconsumeTimes : 0,
            'ck' => (string) ($m->getUserProperty(MessageConst::PROPERTY_POP_CK) ?? ''),
            'firstPop' => (string) ($m->getUserProperty(MessageConst::PROPERTY_FIRST_POP_TIME) ?? ''),
            'at' => microtime(true),
        ];
    }

    /** @return list<array{body:string,topic:string,recon:int,ck:string,firstPop:string,at:float}> */
    public function ofBody(string $body): array
    {
        return array_values(array_filter($this->recs, static fn(array $r): bool => $r['body'] === $body));
    }

    /** @return array<string,int> body → 投递次数（按 body 记账，C3） */
    public function summary(): array
    {
        $out = [];
        foreach ($this->recs as $r) {
            $out[$r['body']] = ($out[$r['body']] ?? 0) + 1;
        }
        ksort($out, SORT_STRING);
        return $out;
    }

    public function noDupes(): bool
    {
        return max([0, ...array_values($this->summary())]) <= 1;
    }

    public function dupes(): string
    {
        $out = [];
        foreach ($this->summary() as $body => $n) {
            if ($n > 1) {
                $out[] = "$body=$n";
            }
        }
        return implode(' ', $out);
    }
}

final class PopListener implements MessageListenerConcurrently
{
    public function __construct(private readonly PopBox $box, private readonly string $poison = '')
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        if ($this->poison === '') {
            return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
        }
        foreach ($msgs as $m) {
            if ($m->getBody() === $this->poison) {
                // 只翻首投这一次；失败路径走 changePopInvisibleTime（POP 语义），
                // 不是 PULL 的 CONSUMER_SEND_MSG_BACK。
                return ConsumeConcurrentlyStatus::RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

// ------------------------------------------------------------------ helpers

/** 在窗口内轮询条件；每轮先 tick 消费者（PHP 无后台线程）。 */
function pop_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
{
    $deadline = microtime(true) + $windowSec;
    for (;;) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond()) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return $cond();
        }
        usleep(100_000);
    }
}

/** 观察窗：持续 tick 但不做条件判断——给"不应再发生的事"留出暴露时间。 */
function pop_observe(float $sec, object ...$tickables): void
{
    $deadline = microtime(true) + $sec;
    while (microtime(true) < $deadline) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        usleep(100_000);
    }
}

function pop_new_admin(string $ns): DefaultMQAdminExt
{
    $a = new DefaultMQAdminExt();
    $a->setNamesrvAddr($ns);
    $a->start();
    return $a;
}

function pop_ensure_topic(string $ns, string $topic, int $queues): void
{
    $a = pop_new_admin($ns);
    try {
        $ci = $a->fetchBrokerClusterInfo();
        $names = array_keys($ci->brokerAddrTable);
        sort($names, SORT_STRING);
        $a->createTopic($names[0] ?? 'broker-a', $topic, $queues);
    } finally {
        $a->shutdown();
    }
}

/** topic 路由指向的逻辑 broker 名（必须出现在 checkpoint 第 5 段，否则 ACK 无法寻址）。 */
function pop_broker_name(string $ns, string $topic): string
{
    $a = pop_new_admin($ns);
    try {
        $route = $a->examineTopicRoute($topic);
        $bn = $route->brokerDatas[0]->brokerName ?? null;
        if ($bn === null || $bn === '') {
            throw new RuntimeException("no broker route for $topic");
        }
        return (string) $bn;
    } finally {
        $a->shutdown();
    }
}

function pop_send(DefaultMQProducer $p, string $topic, string $body): void
{
    $last = '';
    for ($i = 0; $i < 30; $i++) {
        try {
            $r = $p->send(new Message($topic, $body));
            if ($r->sendStatus === SendStatus::SEND_OK) {
                return;
            }
            $last = 'status=' . $r->sendStatus->name;
        } catch (Throwable $e) {
            $last = get_class($e) . ': ' . $e->getMessage();
        }
        sleep(2);
    }
    throw new RuntimeException("send '$body' never succeeded: $last");
}

/** POP_CK 形状断言依赖的各段。 */
function pop_parse_ck(string $ck): array
{
    $seg = ExtraInfoUtil::split($ck !== '' ? $ck : null);
    if (count($seg) !== 8) {
        throw new InvalidArgumentException("checkpoint has " . count($seg) . " segments, want 8: \"$ck\"");
    }
    return [
        'segments' => 8,
        'ckOffset' => ExtraInfoUtil::getCkQueueOffset($seg),
        'popTime' => ExtraInfoUtil::getPopTime($seg),
        'invisible' => ExtraInfoUtil::getInvisibleTime($seg),
        'retry' => ExtraInfoUtil::getRetry($seg),
        'broker' => ExtraInfoUtil::getBrokerName($seg),
        'queueId' => ExtraInfoUtil::getQueueId($seg),
        'queueOffset' => ExtraInfoUtil::getQueueOffset($seg),
    ];
}

function pop_ck_summary(string $ck): string
{
    try {
        $s = pop_parse_ck($ck);
        return sprintf('ckOffset=%d queueOffset=%d retry=%s broker=%s qid=%d popTime=%d invisible=%d',
            $s['ckOffset'], $s['queueOffset'], $s['retry'], $s['broker'], $s['queueId'], $s['popTime'], $s['invisible']);
    } catch (Throwable $e) {
        return $e->getMessage();
    }
}

function pop_wait_route(string $ns, string $topic, float $windowSec): bool
{
    $a = pop_new_admin($ns);
    try {
        return pop_wait_until($windowSec, function () use ($a, $topic): bool {
            try {
                $r = $a->examineTopicRoute($topic);
                return $r !== null && count($r->brokerDatas ?? []) > 0;
            } catch (Throwable) {
                return false;
            }
        });
    } finally {
        $a->shutdown();
    }
}

/**
 * 把消费组切到 POP 投递模式（SET_MESSAGE_REQUEST_MODE(401)）。
 *
 * Java classic 是在 DefaultMQPushConsumerImpl.start() 里自动发的；PHP 端有意做成
 * 显式步骤（与"消费者不做默认 topic 兜底"同属显式化约定），用 Admin 发送。
 */
function pop_set_request_mode(string $ns, string $topic, string $group): void
{
    $a = pop_new_admin($ns);
    try {
        $route = $a->examineTopicRoute($topic);
        $bd = $route->brokerDatas[0] ?? null;
        if ($bd === null) {
            throw new RuntimeException("no route for $topic");
        }
        $addrs = $bd->brokerAddrs ?? [];
        ksort($addrs);
        $addr = $addrs[0] ?? null;
        if ($addr === null) {
            throw new RuntimeException("no broker addr for $topic");
        }
        $a->setMessageRequestMode((string) $addr, $topic, $group, 'POP', 8);
    } finally {
        $a->shutdown();
    }
}

// ------------------------------------------------------------------ main

echo "=== PHP POP live: ns=$ns prefix=$prefix legs=" . ($legsArg === '' ? 'all' : $legsArg) . ' ===' . PHP_EOL;

$want = function (string $leg) use ($legsArg): bool {
    if ($legsArg === '' || $legsArg === 'all') {
        return true;
    }
    foreach (explode(',', $legsArg) as $l) {
        if (trim($l) === $leg) {
            return true;
        }
    }
    return false;
};

$createdTopics = [];
$shutdowns = [];
$cleanup = function () use (&$shutdowns, &$createdTopics, $ns): void {
    foreach ($shutdowns as $fn) {
        $fn();
    }
    if ($createdTopics !== []) {
        try {
            $a = pop_new_admin($ns);
            foreach ($createdTopics as $t) {
                try {
                    $a->deleteTopic($t);
                } catch (Throwable $e) {
                    echo '  delete ' . $t . ': ' . $e->getMessage() . PHP_EOL;
                }
            }
            $a->shutdown();
        } catch (Throwable) {
        }
    }
};

// ------------------------------------------------------------------ S1
if ($want('s1')) {
    echo PHP_EOL . '--- S1 基础：POP 消费 + ACK 真的生效（窗口内无重投）+ 不提交位点 ---' . PHP_EOL;
    $topic = $prefix . '_Basic';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s1';
    $invisible = 5000; // 2.5x 观察窗 ≈ 12.5s
    try {
        pop_ensure_topic($ns, $topic, 1);
        $brokerName = pop_broker_name($ns, $topic);
        $p = new DefaultMQProducer('GID_' . $prefix . '_s1_prod');
        $p->setNamesrvAddr($ns);
        $p->sendMsgTimeout = 5000;
        $p->start();
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new PopBox();
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->popMode = true;                    // 必须在 start 之前设
        $c->popInvisibleTime = $invisible;
        $c->popBatchNums = 32;
        $c->consumeMessageBatchMaxSize = 5;
        $c->setMessageListener(new PopListener($b));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();

        pop_set_request_mode($ns, $topic, $group);
        // POP 消费者像 pull 一样 rebalance（broker 的组模式只决定协议应答侧），
        // 分配里是 2 条：业务 topic 1 条 + 隐式 %RETRY%<group> 1 条（C1：按
        // (topic,broker,queueId) 计，只看 queueID 会幻影重叠）。
        pop_wait_until(20.0, fn() => $c->assignedQueueCountForTopic($topic) >= 1, $c);
        $check($c->assignedQueueCountForTopic($topic) >= 1 && $c->assignedQueueCount() >= 2,
            'S1 分配到业务 topic 的 1 条队列（+ 隐式 %RETRY%<group>）',
            'biz=' . $c->assignedQueueCountForTopic($topic) . ' total=' . $c->assignedQueueCount()
            . ' popQueues=' . $c->popQueueCount());

        $bodies = ['pop-a', 'pop-b', 'pop-c', 'pop-d', 'pop-e', 'pop-f'];
        $sendErr = '';
        try {
            foreach ($bodies as $body) {
                pop_send($p, $topic, $body);
            }
        } catch (Throwable $e) {
            $sendErr = $e->getMessage();
        }
        $check($sendErr === '', 'S1 发送 6 条', $sendErr);

        $gotAll = pop_wait_until(40.0, function () use ($b, $bodies): bool {
            return count($b->summary()) === count($bodies);
        }, $c);
        $check($gotAll, 'S1 6 条都被 POP 到', json_encode($b->summary(), JSON_UNESCAPED_UNICODE));

        // 每条消息必须带 ACK 可用的 checkpoint：8 段、段 0 是批次起始、段 7 是本条
        // 自身 offset（两者相等 ⇒ ACK 段没选错）、marker 是业务 topic、段 5 是逻辑 broker 名。
        $recs = $b->recs;
        $shapeOK = false;
        $shape = count($recs) === 0 ? 'no delivery' : '';
        if ($recs !== []) {
            $shape = pop_ck_summary($recs[0]['ck']);
            try {
                $s = pop_parse_ck($recs[0]['ck']);
                $shapeOK = $s['ckOffset'] === $s['queueOffset'] && $s['retry'] === '0'
                    && $s['broker'] === $brokerName && $s['queueId'] === 0
                    && $s['invisible'] === $invisible && $s['popTime'] > 0;
            } catch (Throwable) {
                $shapeOK = false;
            }
        }
        $check($shapeOK, 'S1 POP_CK 形状：8 段 / ackOffset==msgQueueOffset / marker=0 / 逻辑 broker 名', $shape);

        $firstPopOK = false;
        if ($recs !== []) {
            try {
                $s = pop_parse_ck($recs[0]['ck']);
                $firstPopOK = $recs[0]['firstPop'] !== '' && $recs[0]['firstPop'] === (string) $s['popTime'];
            } catch (Throwable) {
            }
        }
        $check($firstPopOK, 'S1 1ST_POP_TIME == 本次 popTime',
            $recs === [] ? 'no delivery' : 'firstPop=' . $recs[0]['firstPop']);

        $listenerTopicOK = $recs !== [] && $recs[0]['topic'] === $topic;
        $check($listenerTopicOK, 'S1 listener 看到的是业务 topic',
            $recs === [] ? 'no delivery' : 'got ' . $recs[0]['topic']);

        // ACK 债务必须归零：每条 pop 出来的消息都被应答。
        $check(pop_wait_until(5.0, fn() => $c->popWaitAckCount() === 0, $c),
            'S1 ACK 债务归零', 'waitAck=' . $c->popWaitAckCount());

        // 中央断言。刻意盯"无重复"而不是"ACK 200 回来了"：broker 对找不到
        // checkpoint 的 POP_ACK 也回 SUCCESS。若 ACK 是空操作，broker 会在窗口
        // 关闭后 revive 整批，重投必然落在 2.5x 窗口内。
        pop_observe($invisible * 2.5 / 1000.0, $c);
        $check($b->noDupes(), 'S1 2.5x 不可见窗口内无重复投递（ACK 落到 broker）',
            'dup=' . $b->dupes() . ' all=' . json_encode($b->summary(), JSON_UNESCAPED_UNICODE));

        // POP 不留客户端游标：broker 的 revive 队列就是游标，客户端位点表必须空。
        $check($c->localOffsetCount() === 0, 'S1 POP 模式下客户端不持有位点表',
            'localOffsets=' . $c->localOffsetCount());
    } catch (Throwable $e) {
        $check(false, 'S1 异常中止', get_class($e) . ': ' . $e->getMessage());
    }
}

// ------------------------------------------------------------------ S2
if ($want('s2')) {
    echo PHP_EOL . '--- S2 失败退避：改不可见时间 → %RETRY%<group>_<topic> → 重投 + 1ST_POP_TIME 稳定 ---' . PHP_EOL;
    $topic = $prefix . '_Retry';
    $createdTopics[] = $topic;
    $group = 'GID_' . $prefix . '_s2';
    $retryTopic = '%RETRY%' . $group . '_' . $topic;
    $poison = 'pop-poison';
    $invisible = 5000;
    try {
        pop_ensure_topic($ns, $topic, 1);
        pop_broker_name($ns, $topic);
        $p = new DefaultMQProducer('GID_' . $prefix . '_s2_prod');
        $p->setNamesrvAddr($ns);
        $p->sendMsgTimeout = 5000;
        $p->start();
        $shutdowns[] = static fn() => $p->shutdown();

        $b = new PopBox();
        // batchMax=1 是必须而非装饰：客户端按这个粒度把 pop 批次拆给 listener，
        // 失败不能波及邻居（否则"其余消息只投一次"就没意义了）。
        $c = new DefaultMQPushConsumer($group);
        $c->setNamesrvAddr($ns);
        $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        $c->popMode = true;
        $c->popInvisibleTime = $invisible;
        $c->popBatchNums = 32;
        $c->consumeMessageBatchMaxSize = 1;
        $c->maxReconsumeTimes = 16;
        $c->setMessageListener(new PopListener($b, $poison));
        $c->subscribe($topic, '*');
        $c->start();
        $shutdowns[] = static fn() => $c->shutdown();

        pop_set_request_mode($ns, $topic, $group);
        pop_wait_until(20.0, fn() => $c->assignedQueueCountForTopic($topic) >= 1, $c);

        $sendErr = '';
        try {
            foreach (['keep-1', $poison, 'keep-2'] as $body) {
                pop_send($p, $topic, $body);
            }
        } catch (Throwable $e) {
            $sendErr = $e->getMessage();
        }
        $check($sendErr === '', 'S2 发送 3 条', $sendErr);

        $firstArrived = pop_wait_until(40.0, fn() => count($b->ofBody($poison)) >= 1, $c);
        $check($firstArrived, 'S2 首投 3 条',
            json_encode($b->summary(), JSON_UNESCAPED_UNICODE) . ' popQueues=' . $c->popQueueCount());
        if (!$firstArrived) {
            $cleanup();
            echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
            exit($fail === 0 ? 0 : 1);
        }
        $first = $b->ofBody($poison)[0];
        $firstMarker = '';
        try {
            $firstMarker = pop_parse_ck($first['ck'])['retry'];
        } catch (Throwable) {
        }
        $check($first['recon'] === 0 && $firstMarker === '0', 'S2 首投 recon=0 且 marker=0（来自业务 topic）',
            "recon={$first['recon']} marker=$firstMarker");

        // 失败消息被再次隐藏而非回发：changePopInvisibleTime 向 broker 申请更长窗口
        // （popDelayLevel[recon=0] = 10s），窗口关闭后 revive 服务把它搬进
        // %RETRY%<group>_<topic>，下次 pop 带回来。
        $redelivered = pop_wait_until(120.0, fn() => count($b->ofBody($poison)) >= 2, $c);
        $check($redelivered, 'S2 失败消息被重投（改不可见时间 + revive 生效）',
            json_encode($b->summary(), JSON_UNESCAPED_UNICODE));
        if ($redelivered) {
            $second = $b->ofBody($poison)[1];
            $check($second['recon'] >= 1, 'S2 重投 recon 递增',
                "recon {$first['recon']} -> {$second['recon']}");
            // marker 断言：'1' = 消息从 %RETRY%<group>_<topic> 被 pop 出来——既是
            // revive 搬运成功的证据，也是 ACK 必须寻址到重试 topic 的原因（broker
            // 把 checkpoint 记在重试 topic 下）。marker 错成 '0' 就是本腿要抓的 bug。
            $secondMarker = '';
            try {
                $secondMarker = pop_parse_ck($second['ck'])['retry'];
            } catch (Throwable) {
            }
            $check($secondMarker === '1', 'S2 重投 marker=1（来自 POP 重试 topic，ACK 必须发回那里）',
                "marker=$secondMarker");
            $check($second['topic'] === $topic && $first['topic'] === $topic,
                'S2 重投 listener 看到的仍是业务 topic',
                "first={$first['topic']} second={$second['topic']} want=$topic");
            // 1ST_POP_TIME 在 Java（MQClientAPIImpl:1224）与本端都是 computeIfAbsent：
            // broker 提供的值必须保留，绝不能盖成重投自己的 popTime。刻意**不**断言与
            // 首投逐字节相等——broker 打的是被 revive 选中的那个 checkpoint 的
            // popTime，与首投差几毫秒是 broker 记账问题，本腿只钉客户端这半。
            $secondPopTime = 0;
            try {
                $secondPopTime = pop_parse_ck($second['ck'])['popTime'];
            } catch (Throwable) {
            }
            $kept = $second['firstPop'] !== '' && $second['firstPop'] !== (string) $secondPopTime;
            $check($kept, 'S2 1ST_POP_TIME 不被客户端改写（保留 broker 的值）',
                "first={$first['firstPop']} second={$second['firstPop']} secondPopTime=$secondPopTime");
        }

        $sum = $b->summary();
        $check(($sum['keep-1'] ?? 0) === 1 && ($sum['keep-2'] ?? 0) === 1, 'S2 其余消息只投一次',
            json_encode($sum, JSON_UNESCAPED_UNICODE));

        // 重试 topic 由 revive 服务创建，它的路由出现是整条路径的独立 broker 侧确认。
        $check(pop_wait_route($ns, $retryTopic, 45.0), 'S2 %RETRY%<group>_<topic> 路由出现', $retryTopic);

        // 重投也被 ACK 了，债务再次归零。
        $check(pop_wait_until(10.0, fn() => $c->popWaitAckCount() === 0, $c),
            'S2 ACK 债务归零', 'waitAck=' . $c->popWaitAckCount());
    } catch (Throwable $e) {
        $check(false, 'S2 异常中止', get_class($e) . ': ' . $e->getMessage());
    }
}

$cleanup();

echo PHP_EOL . "PASS=$pass FAIL=$fail" . PHP_EOL;
exit($fail === 0 ? 0 : 1);

<?php

declare(strict_types=1);

// 真机 ACL + 全消息类型验证：一整套「带凭据签名被接受」的正向证据 + 一条
// 「不带凭据必须被拒成 NO_PERMISSION(16)」的反向断言。
//
//   php examples/live_acl_message_types.php [namesrv] [accessKey] [secretKey] [legs]
//   例：php examples/live_acl_message_types.php 127.0.0.1:9876 AK_TEST SK_TEST_SECRET_12345678
//   legs = all | neg,normal,async,oneway,batch,delay,timer,tx,order,tag,key,prop
//
// 为什么反向断言必须在最前面、且独立成一个进程阶段：broker 侧**所有**鉴权失败
// （没签名 / 签名不对）都收敛成同一个 NO_PERMISSION(16)，靠 remark 才能区分
// "check signature failed." 与 "username cannot be null."；而「带凭据全绿」在没有
// 反向断言的对照下，无法区分「签名被接受」与「集群根本没开鉴权」。所以：
//   N1 无凭据发送被拒且响应码**恰好是 16**（不是"某个错误"）；
//   N2 错误 secretKey 同样被拒成 16。
// 反向探针用的是**不带钩子**的生产者：RPC 钩子注册在客户端实例的 remoting 层
// 装配点（start() 之后），先起的探针客户端必须**关干净并歇 1s** 再起带凭据的
// 客户端，避免任何实例级共享把无钩子连接带进正向腿。
//
// 覆盖的消息类型（每类都从 broker 读回才算数，只有 oneway 允许靠回读条数兜底）：
//   sync 普通 / async 回调 / oneway / 批量 send(array) / 延迟等级 / 定时 TIMER_DELIVER_MS /
//   事务（半消息→本地 commit→END_TRANSACTION，读回证明变得可消费）/
//   顺序 sendBySelector（同 arg 同队列 + 顺序消费保序）/ Tag 过滤 / Key 服务端查询 / 用户属性透传。
//
// PHP 适配（单线程）：push 消费者的全部后台工作收敛在 tick()，所有等待窗都在
// 主动 tick 消费者地轮询（同 live_redelivery.php 的口径）；sendAsync 内部已泵到
// 回调走完才返回。
//
// 收口行 `PASS=<n> FAIL=<n>`；任一 FAIL 非 0 退出。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\LocalTransactionState;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\SendCallbackImpl;
use RocketMQ\Client\SendResult;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\TransactionListener;
use RocketMQ\Client\TransactionSendResult;
use RocketMQ\Client\SelectMessageQueueByHash;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\AclClientRPCHook;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\SessionCredentials;

// 日志不落在用户 $HOME 下：默认写当前工作目录的 logs/，这里显式钉到临时目录。
if (getenv('ROCKETMQ_CLIENT_LOG_DIR') === false) {
    putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_acl_types_live_logs');
}

// ------------------------------------------------------------------ harness

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
$ak = trim((string)($argv[2] ?? ''));
$sk = trim((string)($argv[3] ?? ''));
$legsArg = trim($argv[4] ?? 'all');
if ($ak === '' || $sk === '') {
    echo '用法: php ' . $argv[0] . ' <namesrv> <accessKey> <secretKey> [legs]' . PHP_EOL;
    echo '集群开了 ACL 时必须给凭据；本命令的全部正向腿都靠签名钩子过鉴权。' . PHP_EOL;
    exit(1);
}
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

$stamp = time();
$prefix = 'PhpAclMt' . $stamp;
// 钩子对象无状态，可以在同一进程的多个客户端实例间复用（每次 start 注册进各自的 remoting 层）。
$acl = new AclClientRPCHook(new SessionCredentials($ak, $sk));

/** 在窗口内轮询条件；每轮先 tick 传进来的消费者（PHP 无后台线程）。 */
function mt_wait_until(float $windowSec, callable $cond, object ...$tickables): bool
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

/**
 * 沿异常链找 broker 响应码。
 *
 * 同步发送把 MQBrokerException(16) 包进 MQClientException 时只在外层带同一个码，
 * admin 路径也可能包一层——不能只看最外层，更不能只看消息文本。
 */
function mt_broker_code(Throwable $e): ?int
{
    $seen = [];
    $cur = $e;
    while ($cur !== null) {
        if (isset($seen[spl_object_id($cur)])) {
            break;
        }
        $seen[spl_object_id($cur)] = true;
        if (property_exists($cur, 'responseCode') && $cur->responseCode !== null) {
            return (int) $cur->responseCode;
        }
        $cur = $cur->getPrevious();
    }
    return null;
}

/** 收集型 listener：并发 / 顺序两套，按 body 与投递次序记账。 */
final class MtBox
{
    /** @var list<MessageExt> */
    public array $msgs = [];

    public function add(MessageExt $m): void
    {
        $this->msgs[] = $m;
    }

    /** @return list<string> 按投递顺序的 body */
    public function bodies(): array
    {
        return array_map(static fn (MessageExt $m): string => (string) $m->getBody(), $this->msgs);
    }

    /** @return list<string> 排序后的 body（去队列间乱序影响） */
    public function sortedBodies(): array
    {
        $b = $this->bodies();
        sort($b, SORT_STRING);
        return $b;
    }

    /** @return list<MessageExt> */
    public function ofBody(string $body): array
    {
        return array_values(array_filter(
            $this->msgs,
            static fn (MessageExt $m): bool => (string) $m->getBody() === $body
        ));
    }
}

final class MtConcurrentListener implements MessageListenerConcurrently
{
    public function __construct(private readonly MtBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

final class MtOrderlyListener implements MessageListenerOrderly
{
    public function __construct(private readonly MtBox $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $ctx): ConsumeOrderlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeOrderlyStatus::SUCCESS;
    }
}

/** 本地事务恒提交：半消息 → COMMIT → END_TRANSACTION，读回即证明提交生效。 */
final class MtCommitListener implements TransactionListener
{
    public array $executed = [];

    public function executeLocalTransaction(Message $msg, mixed $arg): LocalTransactionState
    {
        $this->executed[] = (string) $msg->getBody();
        return LocalTransactionState::COMMIT_MESSAGE;
    }

    public function checkLocalTransaction(MessageExt $msg): LocalTransactionState
    {
        return LocalTransactionState::COMMIT_MESSAGE;
    }
}

function mt_new_admin(string $ns, AclClientRPCHook $hook, string $instance): DefaultMQAdminExt
{
    $a = new DefaultMQAdminExt($hook);
    $a->setNamesrvAddr($ns);
    $a->setInstanceName($instance);
    $a->start();
    return $a;
}

function mt_new_producer(string $ns, string $group, AclClientRPCHook $hook, string $instance): DefaultMQProducer
{
    $p = new DefaultMQProducer($group, $hook);
    $p->setNamesrvAddr($ns);
    $p->setInstanceName($instance);
    $p->sendMsgTimeout = 5000;
    $p->start();
    return $p;
}

// ------------------------------------------------------------------ main

echo '=== PHP ACL message-types live: ns=' . $ns . ' ak=' . $ak
    . ' prefix=' . $prefix . ' legs=' . ($legsArg === '' ? 'all' : $legsArg) . ' ===' . PHP_EOL;

// ---------------------------------------------------------------- 阶段 0：反向探针
// 必须**最先**跑：不带钩子的探针客户端先建实例、先注销，再起带凭据的客户端。
if ($want('neg')) {
    echo PHP_EOL . '--- N 无凭据/错凭据必须被 broker 拒成 NO_PERMISSION(16) ---' . PHP_EOL;

    $anonTopic = $prefix . '_Anon';
    $anon = new DefaultMQProducer('GID_' . $prefix . '_anon');
    $anon->setNamesrvAddr($ns);
    $anon->setInstanceName('aclmt-anon-' . $stamp);
    $anon->sendMsgTimeout = 3000;
    // 关掉重试：NO_PERMISSION 在重试码表里，留着只会被重复拒绝拖慢探针。
    $anon->setRetryTimesWhenSendFailed(0);
    $anon->start();
    try {
        $denied = false;
        $code = null;
        $detail = '';
        try {
            $anon->send(new Message($anonTopic, 'should-not-send'));
            $detail = '意外发送成功（集群没在拒无凭据请求？）';
        } catch (Throwable $e) {
            $code = mt_broker_code($e);
            $denied = $code === ResponseCode::NO_PERMISSION;
            $detail = get_class($e) . ' code=' . var_export($code, true) . ' msg=' . $e->getMessage();
        }
        $check($denied, 'N1 无凭据发送被 broker 拒绝，响应码恰为 16', $detail);
    } finally {
        $anon->shutdown();
    }

    $bad = new DefaultMQProducer(
        'GID_' . $prefix . '_badsk',
        new AclClientRPCHook(new SessionCredentials($ak, 'WRONG_SECRET_KEY_12345678'))
    );
    $bad->setNamesrvAddr($ns);
    $bad->setInstanceName('aclmt-badsk-' . $stamp);
    $bad->sendMsgTimeout = 3000;
    $bad->setRetryTimesWhenSendFailed(0);
    $bad->start();
    try {
        $denied = false;
        $code = null;
        $detail = '';
        try {
            $bad->send(new Message($anonTopic, 'should-not-send-either'));
            $detail = '错 secretKey 意外发送成功';
        } catch (Throwable $e) {
            $code = mt_broker_code($e);
            $denied = $code === ResponseCode::NO_PERMISSION;
            $detail = get_class($e) . ' code=' . var_export($code, true) . ' msg=' . $e->getMessage();
        }
        $check($denied, 'N2 错误 secretKey 被 broker 拒绝，响应码恰为 16', $detail);
    } finally {
        $bad->shutdown();
    }

    // 探针实例收干净后再起带凭据的客户端（歇 1s 让注销落定）。
    sleep(1);
}

// ---------------------------------------------------------------- 正向腿共用客户端
$admin = mt_new_admin($ns, $acl, 'aclmt-admin-' . $stamp);
$createdTopics = [];
$shutdowns = [];
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $brokerName = (string)(array_key_first($ci->brokerAddrTable) ?? 'broker-a');
} catch (Throwable $e) {
    echo '!! fetchBrokerClusterInfo failed: ' . $e->getMessage() . PHP_EOL;
    $admin->shutdown();
    exit(1);
}
$check(true, 'P0 带凭据 admin 与 broker 集群握手成功', 'broker=' . $brokerName);

$ensureTopic = static function (string $topic, int $queues) use ($admin, $brokerName, &$createdTopics): void {
    $admin->createTopic($brokerName, $topic, $queues);
    $createdTopics[] = $topic;
};

/**
 * 起一个带凭据的 push 消费者，在窗口内 tick 到 expect 条（或超时），再额外 settle
 * 秒持续 tick（给"不该出现的第 expect+1 条"留出暴露时间），返回记账盒。
 * 读回一律 CONSUME_FROM_FIRST_OFFSET + 全新分组，不受历史位点影响。
 */
$readBack = static function (string $topic, string $suffix, int $expect, float $windowSec,
    string $sub = '*', bool $orderly = false, float $settleSec = 0.0)
    use ($ns, $acl, $prefix, $stamp, &$shutdowns): MtBox {
    $box = new MtBox();
    $c = new DefaultMQPushConsumer('GID_' . $prefix . '_' . $suffix, $acl);
    $c->setNamesrvAddr($ns);
    $c->setInstanceName('aclmt-rc-' . $suffix . '-' . $stamp);
    $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    // 单线程 tick 顺序轮询各队列：空闲队列的挂起长轮询会把窗口耗光，把两端都调短。
    $c->pullSuspendTimeoutMillis = 1000;
    $c->pullTimeoutMillis = 3000;
    $c->setMessageListener($orderly ? new MtOrderlyListener($box) : new MtConcurrentListener($box));
    $c->subscribe($topic, $sub);
    $c->start();
    $shutdowns[] = static fn () => $c->shutdown();
    mt_wait_until(20.0, static fn () => $c->assignedQueueCount() >= 1, $c);
    mt_wait_until($windowSec, static fn (): bool => count($box->msgs) >= $expect, $c);
    if ($settleSec > 0) {
        $deadline = microtime(true) + $settleSec;
        while (microtime(true) < $deadline) {
            $c->tick();
            usleep(100_000);
        }
    }
    return $box;
};

$producer = mt_new_producer($ns, 'GID_' . $prefix . '_prod', $acl, 'aclmt-prod-' . $stamp);
$shutdowns[] = static fn () => $producer->shutdown();

// 带重试的发送：新集群/路由注册抖动不该伪装成消息类型缺陷。
$sendWithRetry = static function (callable $send) use ($producer): mixed {
    $last = null;
    for ($i = 0; $i < 10; $i++) {
        try {
            return $send();
        } catch (Throwable $e) {
            $last = $e;
            // 鉴权被拒没有重试的意义，直接上抛让腿自己断言。
            if (mt_broker_code($e) === ResponseCode::NO_PERMISSION) {
                throw $e;
            }
            usleep(1_000_000);
        }
    }
    throw $last ?? new RuntimeException('send never succeeded');
};

// ---------------------------------------------------------------- S1 同步普通
if ($want('normal')) {
    echo PHP_EOL . '--- S1 sync 普通消息 ---' . PHP_EOL;
    $topic = $prefix . '_Normal';
    $ensureTopic($topic, 1);
    $ok = 0;
    foreach (['norm-0', 'norm-1', 'norm-2'] as $body) {
        $r = $sendWithRetry(static fn () => $producer->send(new Message($topic, $body)));
        $ok += $r->sendStatus === SendStatus::SEND_OK ? 1 : 0;
    }
    $check($ok === 3, 'S1 带凭据 sync 发送 3 条 SEND_OK', 'ok=' . $ok);
    $box = $readBack($topic, 'normal', 3, 30.0);
    $check($box->sortedBodies() === ['norm-0', 'norm-1', 'norm-2'], 'S1 push 读回 3 条且内容一致',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S2 异步
if ($want('async')) {
    echo PHP_EOL . '--- S2 sendAsync 回调链 ---' . PHP_EOL;
    $topic = $prefix . '_Async';
    $ensureTopic($topic, 1);
    $okResults = [];
    $errors = [];
    foreach (['async-0', 'async-1', 'async-2'] as $body) {
        // PHP 单线程：sendAsync 内部已泵到回调走完才返回。
        $producer->sendAsync(new Message($topic, $body),
            new SendCallbackImpl(static function (?SendResult $r) use (&$okResults): void {
                $okResults[] = $r;
            }, static function (Throwable $e) use (&$errors): void {
                $errors[] = $e->getMessage();
            }));
    }
    $statusOk = count($okResults) === 3 && count($errors) === 0
        && array_reduce($okResults, static fn (bool $c, ?SendResult $r): bool =>
            $c && $r !== null && $r->sendStatus === SendStatus::SEND_OK, true);
    $check($statusOk, 'S2 异步回调 3 次全部 SEND_OK', 'ok=' . count($okResults) . ' err=' . implode(';', $errors));
    $box = $readBack($topic, 'async', 3, 30.0);
    $check($box->sortedBodies() === ['async-0', 'async-1', 'async-2'], 'S2 异步消息读回 3 条',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S3 oneway
if ($want('oneway')) {
    echo PHP_EOL . '--- S3 sendOneway（无应答，靠读回条数兜底） ---' . PHP_EOL;
    $topic = $prefix . '_Oneway';
    $ensureTopic($topic, 1);
    $threw = '';
    try {
        foreach (['ow-0', 'ow-1', 'ow-2'] as $body) {
            $sendWithRetry(static fn () => $producer->sendOneway(new Message($topic, $body)));
        }
    } catch (Throwable $e) {
        $threw = $e->getMessage();
    }
    $box = $readBack($topic, 'oneway', 3, 30.0);
    $check($threw === '' && $box->sortedBodies() === ['ow-0', 'ow-1', 'ow-2'],
        'S3 oneway 3 条被 broker 收下并读回', 'got=' . implode(',', $box->bodies()) . ' ' . $threw);
}

// ---------------------------------------------------------------- S4 批量
if ($want('batch')) {
    echo PHP_EOL . '--- S4 批量 send(array)（SEND_BATCH_MESSAGE） ---' . PHP_EOL;
    $topic = $prefix . '_Batch';
    $ensureTopic($topic, 1);
    $batch = [
        new Message($topic, 'batch-0'),
        new Message($topic, 'batch-1'),
        new Message($topic, 'batch-2'),
    ];
    $r = $sendWithRetry(static fn () => $producer->send($batch));
    $check($r->sendStatus === SendStatus::SEND_OK, 'S4 批量发送 SEND_OK', (string) $r->sendStatus->name);
    $box = $readBack($topic, 'batch', 3, 30.0);
    $check($box->sortedBodies() === ['batch-0', 'batch-1', 'batch-2'],
        'S4 批量消息逐条读回 3 条', 'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S5 延迟等级
if ($want('delay')) {
    echo PHP_EOL . '--- S5 延迟消息 set_delay_time_level(3)=10s ---' . PHP_EOL;
    $topic = $prefix . '_Delay';
    $ensureTopic($topic, 1);
    $sendWithRetry(static fn () => $producer->send(new Message($topic, 'delay-normal')));
    $dm = new Message($topic, 'delay-level3');
    $dm->setDelayTimeLevel(3); // messageDelayLevel 表里 3 = 10s
    $sendWithRetry(static fn () => $producer->send($dm));

    $box = $readBack($topic, 'delay', 2, 40.0);
    $delayed = $box->ofBody('delay-level3');
    $normal = $box->ofBody('delay-normal');
    $check(count($delayed) === 1 && count($normal) === 1, 'S5 延迟与对照消息都被读回',
        'delayed=' . count($delayed) . ' normal=' . count($normal));
    if ($delayed !== []) {
        $drift = $delayed[0]->storeTimestamp - $delayed[0]->bornTimestamp;
        $check($drift >= 8000, 'S5 延迟生效（store_ts-born_ts >= 8s，等级 3=10s）', 'drift=' . $drift . 'ms');
    } else {
        $check(false, 'S5 延迟生效（store_ts-born_ts >= 8s，等级 3=10s）', '无延迟消息可校验');
    }
    if ($normal !== []) {
        $ndrift = $normal[0]->storeTimestamp - $normal[0]->bornTimestamp;
        $check($ndrift < 5000, 'S5 对照普通消息未被延迟', 'drift=' . $ndrift . 'ms');
    }
}

// ---------------------------------------------------------------- S6 定时（timer）
if ($want('timer')) {
    echo PHP_EOL . '--- S6 定时消息 TIMER_DELIVER_MS=+5s ---' . PHP_EOL;
    $topic = $prefix . '_Timer';
    $ensureTopic($topic, 1);
    $deliverAt = (int) (microtime(true) * 1000) + 5000;
    $tm = new Message($topic, 'timer-5s');
    $tm->setUserProperty('TIMER_DELIVER_MS', (string) $deliverAt);
    $sendWithRetry(static fn () => $producer->send($tm));

    $box = $readBack($topic, 'timer', 1, 40.0);
    $hit = $box->ofBody('timer-5s');
    $check(count($hit) === 1, 'S6 定时消息到点投递并被读回', 'got=' . count($hit));
    if ($hit !== []) {
        $drift = $hit[0]->storeTimestamp - $hit[0]->bornTimestamp;
        $check($drift >= 4000, 'S6 定时生效（store_ts-born_ts >= 4s，投递点=+5s）', 'drift=' . $drift . 'ms');
    } else {
        $check(false, 'S6 定时生效（store_ts-born_ts >= 4s，投递点=+5s）', '无定时消息可校验');
    }
}

// ---------------------------------------------------------------- S7 事务
if ($want('tx')) {
    echo PHP_EOL . '--- S7 事务消息：半消息 → 本地 COMMIT → END_TRANSACTION → 读回 ---' . PHP_EOL;
    $topic = $prefix . '_Tx';
    $ensureTopic($topic, 1);
    $listener = new MtCommitListener();
    /** @var TransactionSendResult $tsr */
    $tsr = $sendWithRetry(static fn () => $producer->sendMessageInTransaction(
        new Message($topic, 'tx-commit'), $listener));
    $halfOk = $tsr->getSendStatus() === SendStatus::SEND_OK;
    $check($halfOk, 'S7 半消息发送 SEND_OK',
        'status=' . $tsr->getSendStatus()->name . ' txId=' . var_export($tsr->getTransactionId(), true));
    $check($listener->executed === ['tx-commit'], 'S7 本地事务被执行一次',
        'executed=' . implode(',', $listener->executed));
    $check($tsr->getLocalTransactionState() === LocalTransactionState::COMMIT_MESSAGE,
        'S7 终态为 COMMIT_MESSAGE', var_export($tsr->getLocalTransactionState()?->name, true));
    $box = $readBack($topic, 'tx', 1, 40.0);
    $check(count($box->ofBody('tx-commit')) === 1, 'S7 提交后消息可从 broker 读回（变得可消费）',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S8 顺序
if ($want('order')) {
    echo PHP_EOL . '--- S8 顺序消息：sendBySelector 同 arg 同队列 + 顺序消费保序 ---' . PHP_EOL;
    $topic = $prefix . '_Order';
    $ensureTopic($topic, 4);
    $sentOrder = [];
    $qids = [];
    for ($i = 0; $i < 10; $i++) {
        $body = sprintf('ord-%02d', $i);
        $sentOrder[] = $body;
        $r = $sendWithRetry(static fn () => $producer->sendBySelector(
            new Message($topic, $body), new SelectMessageQueueByHash(), 'shard-A'));
        if ($r->messageQueue !== null) {
            $qids[$r->messageQueue->queueId] = true;
        }
    }
    $check(count($qids) === 1, 'S8 同 arg 全部路由到同一队列', 'distinct_qids=' . implode(',', array_keys($qids)));
    $box = $readBack($topic, 'order', 10, 40.0, '*', orderly: true);
    $check($box->bodies() === $sentOrder, 'S8 顺序消费按发送序读回 10 条',
        'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S9 Tag 过滤
if ($want('tag')) {
    echo PHP_EOL . '--- S9 Tag 过滤消费（服务端只投 TagA） ---' . PHP_EOL;
    $topic = $prefix . '_Tag';
    $ensureTopic($topic, 1);
    for ($i = 0; $i < 3; $i++) {
        $sendWithRetry(static fn () => $producer->send(new Message($topic, "tagA-$i", 'TagA')));
    }
    for ($i = 0; $i < 3; $i++) {
        $sendWithRetry(static fn () => $producer->send(new Message($topic, "tagB-$i", 'TagB')));
    }
    // 先用 * 订阅把 6 条都读回来（TagB 也要落队），再换 TagA 订阅断言服务端过滤。
    $all = $readBack($topic, 'tagall', 6, 30.0);
    $check(count($all->msgs) === 6, 'S9 两种 Tag 共 6 条都落队', 'got=' . count($all->msgs));
    // settle 窗：达到 3 条后继续 tick 一段，让"混进来的 TagB"有机会现身再计数。
    $onlyA = $readBack($topic, 'taga', 3, 30.0, 'TagA', false, 5.0);
    $tagsOk = count($onlyA->msgs) === 3;
    foreach ($onlyA->msgs as $m) {
        if ($m->getTags() !== 'TagA') {
            $tagsOk = false;
        }
    }
    $check($tagsOk, 'S9 TagA 订阅只收到 3 条且全为 TagA',
        'got=' . implode(',', $onlyA->bodies()));
}

// ---------------------------------------------------------------- S10 Key 查询
if ($want('key')) {
    echo PHP_EOL . '--- S10 消息 Key + 服务端按 Key 查询 ---' . PHP_EOL;
    $topic = $prefix . '_Key';
    $ensureTopic($topic, 1);
    $key = 'ACLMTKEY_' . $stamp;
    $r = $sendWithRetry(static fn () => $producer->send(new Message($topic, 'key-payload', 'KeyTag', $key)));
    $check($r->sendStatus === SendStatus::SEND_OK, 'S10 带 Key 消息发送 SEND_OK', 'key=' . $key);
    // broker 的 Key 索引异步构建：轮询到命中为止。
    $found = [];
    $hit = mt_wait_until(20.0, function () use ($admin, $topic, $key, &$found): bool {
        try {
            $found = $admin->queryMessageByKey($topic, $key);
            return count($found) >= 1;
        } catch (Throwable) {
            return false;
        }
    });
    $bodies = array_map(static fn (MessageExt $m): string => (string) $m->getBody(), $found);
    $check($hit && in_array('key-payload', $bodies, true), 'S10 服务端按 Key 查询命中',
        'returned=' . implode(',', $bodies));
    $box = $readBack($topic, 'key', 1, 30.0);
    $keysOk = count($box->msgs) === 1 && $box->msgs[0]->getKeys() === $key;
    $check($keysOk, 'S10 读回消息的 KEYS 属性一致', 'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- S11 用户属性
if ($want('prop')) {
    echo PHP_EOL . '--- S11 用户属性透传 ---' . PHP_EOL;
    $topic = $prefix . '_Prop';
    $ensureTopic($topic, 1);
    for ($i = 0; $i < 3; $i++) {
        $m = new Message($topic, "prop-$i", 'P');
        $m->setUserProperty('city', 'Hangzhou');
        $m->setUserProperty('env', 'prod');
        $sendWithRetry(static fn () => $producer->send($m));
    }
    $box = $readBack($topic, 'prop', 3, 30.0);
    $propOk = count($box->msgs) === 3;
    foreach ($box->msgs as $m) {
        if ($m->getUserProperty('city') !== 'Hangzhou' || $m->getUserProperty('env') !== 'prod') {
            $propOk = false;
        }
    }
    $check($propOk, 'S11 用户属性 city/env 端到端透传（3 条）', 'got=' . implode(',', $box->bodies()));
}

// ---------------------------------------------------------------- 收尾
echo PHP_EOL . '--- 收尾 ---' . PHP_EOL;
foreach ($shutdowns as $fn) {
    try {
        $fn();
    } catch (Throwable) {
        // 收尾失败不影响结果
    }
}
if ($createdTopics !== []) {
    $delErr = '';
    try {
        foreach ($createdTopics as $t) {
            $admin->deleteTopic($t);
        }
    } catch (Throwable $e) {
        $delErr = $e->getMessage();
    }
    $check($delErr === '', '清理：删除本轮创建的 topic', 'topics=' . count($createdTopics) . ' ' . $delErr);
}
$admin->shutdown();

echo PHP_EOL . 'PASS=' . $pass . ' FAIL=' . $fail . PHP_EOL;
exit($fail === 0 ? 0 : 1);

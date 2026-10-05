<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageQueue;

/**
 * 消息聚合发送器（对应 org.apache.rocketmq.client.producer.ProduceAccumulator，Java 5.5.0）。
 *
 * 蓝本：`python/client/produce_accumulator.py`（已与 Java 逐条对齐、真机验证）。
 * 生产者打开 `autoBatch` 后，`send(Message)` 不再逐条直发，而是先按
 * `AggregateKey(topic, mq, waitStoreMsgOK, tag)` 归并进 `MessageAccumulation`，攒够
 * `holdSize` / `holdMs` 再合成**一个** `MessageBatch` 发出去，最后把 broker 回的**批量**
 * `SendResult` 拆回每条消息各自的 `SendResult` —— 调用方拿到的东西与直发一致
 * （msgId / offsetMsgId / queueOffset 都是这一条自己的）。
 *
 * ## 多线程 → 显式 pump（PHP 适配）
 *
 * 蓝本用后台守卫线程 + `threading.Lock/Condition` 做「HoldMs 到期自动 flush」。PHP 是单线程，
 * 这里**不起线程/子进程**，把守卫线程的一轮主体暴露成可显式调用的泵：
 *
 *   * `tick($nowMs = null)` —— 复刻守卫线程的 `doWork`：对每个 accumulation 判 `readyToSend`
 *     并 flush，然后把 `messagesSize == 0` 的空批次置 `closed` 并摘表；
 *   * `flush($key)` —— 立刻 flush 某一个 `AggregateKey`（同步 + 异步两张表都试）；
 *   * `flushAll()` —— 立刻 flush 两张表里的全部批次。
 *
 * 语义保值点（与蓝本逐条一致，别"顺手优化"）：
 *   1. `AggregateKey` 四元组：tag 不同不合并；指定 mq 与不指定 mq 不合并；缺省 WAIT 即 true。
 *   2. `tryAddMessage` 全局字节闸门：`currentlyHoldSize < totalHoldSize` 才放行并记账；
 *      **批次真的发完**才归还（同步在 `finally`，异步在回调里）。
 *   3. 批量应答拆条：msgId 含逗号才按逗号拆（条数对不上抛异常）；不含逗号时所有下标
 *      **共享同一个 `SendResult` 对象**（就地共享，不复制）；`queueOffset + i` 逐条递增。
 *   4. **同步 add 收集 keys，异步 add 不收集**（Java 的不对称行为）→ 异步批次 `KEYS=""`。
 *   5. 批级 `KEYS` 无条件写（`implode(" ", keys)`；空集合写出空串），tag 为 null 时不写 TAGS。
 *   6. 发完的批次只置 `closed`、**不重置** `messagesSize`，因此会**留在表里**，直到下一次
 *      同键 `send` 拿到它、`add` 返回 -1 才被摘掉重取 —— 表项移除只发生在 pump / `add` 返回 -1
 *      的调用方，绝不在 send 里顺手 remove。
 *   7. 守卫「同一 key 正在发送时不重复触发」用 `closed` 位 + `GuardService` 的在途集合双重保证。
 *
 * ⚠ 与蓝本的两处**结构性**差异（单线程所必需，非语义优化）：
 *   * 蓝本的同步 `add` 会**阻塞**到本批发完再返回，从而同步 `send` 能直接拿到 SendResult。
 *     PHP 无法阻塞等待另一个执行体，因此同步 `send` 改为「入队后立即泵本 key（`flush`），再取
 *     结果」；批量攒批请用低阶 API：`getOrCreateSyncBatch()` + `MessageAccumulation::add()`，
 *     再显式 `tick()` / `flush()`。
 *   * 守卫在 PHP 里兼顾「蓝本中由被阻塞的 add 自己完成的发送」——同步 `tick` 会对到期批次
 *     直接 `sendSync()`（蓝本守卫只 `wakeup()`，真正发送由被唤醒的 add 执行）。
 */
final class ProduceAccumulator
{
    // Java 的三个默认值（totalHoldSize / holdSize / holdMs）。
    public const DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024;
    public const DEFAULT_HOLD_SIZE = 32 * 1024;
    public const DEFAULT_HOLD_MS = 10;

    /** 进程级复用表：对应 Java MQClientManager.getOrCreateProduceAccumulator（按 clientId 缓存）。 */
    /** @var array<string,ProduceAccumulator> */
    private static array $sharedInstances = [];

    private int $holdMs = self::DEFAULT_HOLD_MS;
    private int $holdSize = self::DEFAULT_HOLD_SIZE;
    private int $totalHoldSize = self::DEFAULT_TOTAL_HOLD_SIZE;
    private int $currentlyHoldSize = 0;

    /** @var array<string,MessageAccumulation> 同步表，键为 AggregateKey::keyString() */
    private array $syncSendBatches = [];
    /** @var array<string,MessageAccumulation> 异步表，键为 AggregateKey::keyString() */
    private array $asyncSendBatches = [];

    /** @var (\Closure(MessageBatch, ?MessageQueue, ?InternalSendCallback): ?SendResult)|null */
    private ?\Closure $sender;
    /** @var \Closure(): int */
    private \Closure $clock;

    private bool $started = false;

    public readonly GuardForSyncSend $guardSync;
    public readonly GuardForAsyncSend $guardAsync;

    /**
     * @param string        $instanceName 对应 Java 的 instanceName（clientId）
     * @param callable|null $sender       注入的发送函数，签名
     *                                    `function (MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $callback): ?SendResult`
     *                                    —— 等价于 Java `DefaultMQProducer.sendDirect(batch, mq, callback)`：
     *                                    同步（$callback 为 null）返回该批量的 SendResult；异步必须回调
     *                                    `$callback->onSuccess()/onException()`。
     * @param callable|null $clock        注入的当前时间（毫秒），默认 `(int)(microtime(true)*1000)`；
     *                                    便于单测推进 HoldMs。
     */
    public function __construct(
        public readonly string $instanceName,
        ?callable $sender = null,
        ?callable $clock = null,
    ) {
        $this->sender = $sender === null ? null : $sender(...);
        $this->clock = $clock === null
            ? static fn(): int => (int) (microtime(true) * 1000)
            : $clock(...);
        $this->guardSync = new GuardForSyncSend($this);
        $this->guardAsync = new GuardForAsyncSend($this);
    }

    // ================================================================ 生命周期

    /** 幂等且可重复：`start → shutdown → start`（生产者重启）不会被线程"只能启动一次"卡住。 */
    public function start(): void
    {
        $this->guardSync->start();
        $this->guardAsync->start();
        $this->started = true;
    }

    public function shutdown(): void
    {
        $this->guardSync->shutdown();
        $this->guardAsync->shutdown();
        $this->started = false;
    }

    public function isStarted(): bool
    {
        return $this->started;
    }

    // ================================================================ 参数（校验口径与文案逐字照抄 Java/Python）

    public function getBatchMaxDelayMs(): int
    {
        return $this->holdMs;
    }

    public function batchMaxDelayMs(int $holdMs): void
    {
        if ($holdMs <= 0 || $holdMs > 30 * 1000) {
            throw new \InvalidArgumentException(
                sprintf('batchMaxDelayMs expect between 1ms and 30s, but get %d!', $holdMs)
            );
        }
        $this->holdMs = $holdMs;
    }

    public function getBatchMaxBytes(): int
    {
        return $this->holdSize;
    }

    public function batchMaxBytes(int $holdSize): void
    {
        if ($holdSize <= 0 || $holdSize > 2 * 1024 * 1024) {
            throw new \InvalidArgumentException(
                sprintf('batchMaxBytes expect between 1B and 2MB, but get %d!', $holdSize)
            );
        }
        $this->holdSize = $holdSize;
    }

    /** Java 这里也返回 holdSize（不是 totalHoldSize）—— 上游笔误，照抄。 */
    public function getTotalBatchMaxBytes(): int
    {
        return $this->holdSize;
    }

    public function totalBatchMaxBytes(int $totalHoldSize): void
    {
        if ($totalHoldSize <= 0) {
            throw new \InvalidArgumentException(
                sprintf('totalBatchMaxBytes must bigger then 0, but get %d!', $totalHoldSize)
            );
        }
        $this->totalHoldSize = $totalHoldSize;
    }

    public function getTotalHoldSize(): int
    {
        return $this->totalHoldSize;
    }

    public function getHoldMs(): int
    {
        return $this->holdMs;
    }

    public function getHoldSize(): int
    {
        return $this->holdSize;
    }

    public function currentlyHoldSize(): int
    {
        return $this->currentlyHoldSize;
    }

    /** 注入时钟的当前值（毫秒）。 */
    public function nowMs(): int
    {
        return ($this->clock)();
    }

    // ================================================================ 全局字节闸门

    /** Java `tryAddMessage`：还有额度就记账放行，否则拒绝（调用方退回直发）。 */
    public function tryAddMessage(Message $message): bool
    {
        if ($this->currentlyHoldSize < $this->totalHoldSize) {
            $body = $message->getBody();
            $bodySize = $body === null ? 0 : strlen($body);
            if ($bodySize > 0) {
                $this->currentlyHoldSize += $bodySize;
            }
            return true;
        }
        return false;
    }

    /** 批次发送完成后的归还（Java 直接 `currentlyHoldSize.addAndGet(-size)`）。 */
    public function releaseHold(int $size): void
    {
        $this->currentlyHoldSize -= $size;
    }

    // ================================================================ 表操作

    /** @return list<MessageAccumulation> */
    public function syncSendBatchesSnapshot(): array
    {
        return array_values($this->syncSendBatches);
    }

    /** @return list<MessageAccumulation> */
    public function asyncSendBatchesSnapshot(): array
    {
        return array_values($this->asyncSendBatches);
    }

    public function getOrCreateSyncBatch(AggregateKey $key): MessageAccumulation
    {
        $ks = $key->keyString();
        return $this->syncSendBatches[$ks] ??= new MessageAccumulation($key, $this);
    }

    public function getOrCreateAsyncBatch(AggregateKey $key): MessageAccumulation
    {
        $ks = $key->keyString();
        return $this->asyncSendBatches[$ks] ??= new MessageAccumulation($key, $this);
    }

    /** Java `syncSendBatchs.remove(key, batch)`：只在值仍是它时才摘。 */
    public function removeSyncBatch(AggregateKey $key, MessageAccumulation $batch): void
    {
        $ks = $key->keyString();
        if (($this->syncSendBatches[$ks] ?? null) === $batch) {
            unset($this->syncSendBatches[$ks]);
        }
    }

    public function removeAsyncBatch(AggregateKey $key, MessageAccumulation $batch): void
    {
        $ks = $key->keyString();
        if (($this->asyncSendBatches[$ks] ?? null) === $batch) {
            unset($this->asyncSendBatches[$ks]);
        }
    }

    // ================================================================ 发送落点

    /**
     * 等价 Java `DefaultMQProducer.sendDirect(batch, mq, callback)`：把组装好的批量交给注入的 sender。
     */
    public function sendDirect(MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $callback): ?SendResult
    {
        if ($this->sender === null) {
            throw new MQClientException('sender is not configured, can not send batch message');
        }
        return ($this->sender)($batch, $mq, $callback);
    }

    // ================================================================ 显式 pump

    /**
     * 复刻守卫线程的一轮 `doWork`：同步 + 异步两张表各跑一遍。
     *
     * @param int|null $nowMs 注入的"当前时间"（毫秒），null 用注入时钟
     */
    public function tick(?int $nowMs = null): void
    {
        $this->guardSync->doWork($nowMs);
        $this->guardAsync->doWork($nowMs);
    }

    /** 立刻 flush 某一个 key（同步 + 异步两张表都试）。 */
    public function flush(AggregateKey|string $key): void
    {
        $ks = $key instanceof AggregateKey ? $key->keyString() : $key;
        if (isset($this->syncSendBatches[$ks])) {
            $this->pumpOne($this->syncSendBatches[$ks], true);
        }
        if (isset($this->asyncSendBatches[$ks])) {
            $this->pumpOne($this->asyncSendBatches[$ks], false);
        }
    }

    /** 立刻 flush 两张表里的全部批次。 */
    public function flushAll(): void
    {
        foreach ($this->syncSendBatchesSnapshot() as $batch) {
            $this->pumpOne($batch, true);
        }
        foreach ($this->asyncSendBatchesSnapshot() as $batch) {
            $this->pumpOne($batch, false);
        }
    }

    /** pump 里对单个批次的发送：包住异常（守卫线程同样靠外层 catch 吞掉并继续）。 */
    private function pumpOne(MessageAccumulation $batch, bool $sync): void
    {
        $guard = $sync ? $this->guardSync : $this->guardAsync;
        $ks = $batch->aggregateKey->keyString();
        if (!$guard->tryEnter($ks)) {
            return; // 同一 key 正在发送，不重复触发
        }
        try {
            if ($batch->closed) {
                return;
            }
            if ($sync) {
                $batch->sendSync();
            } else {
                $batch->sendAsync(null);
            }
        } catch (\Throwable $e) {
            Logger::warning($guard->name . ' flush exception. ' . $e->getMessage());
        } finally {
            $guard->leave($ks);
        }
    }

    // ================================================================ 对外发送入口

    /** Java `send(Message, DefaultMQProducer)`：只返回本条消息自己的 SendResult。 */
    public function send(Message $msg): SendResult
    {
        return $this->sendSyncImpl(AggregateKey::ofMessage($msg), $msg);
    }

    public function sendWithMq(Message $msg, MessageQueue $mq): SendResult
    {
        return $this->sendSyncImpl(AggregateKey::ofMessageWithMq($msg, $mq), $msg);
    }

    private function sendSyncImpl(AggregateKey $key, Message $msg): SendResult
    {
        while (true) {
            $batch = $this->getOrCreateSyncBatch($key);
            $index = $batch->add($msg);
            if ($index === -1) {
                // 本批在本次 add 之前就被关掉了：摘掉它，重取/新建一个再试
                $this->removeSyncBatch($key, $batch);
                continue;
            }
            // 单线程适配：没有阻塞的 add 等待者，入队后立即泵本 key 才能取到结果（异常照常抛出）。
            $guardKey = $key->keyString();
            if ($this->guardSync->tryEnter($guardKey)) {
                try {
                    if (!$batch->closed) {
                        $batch->sendSync();
                    }
                } finally {
                    $this->guardSync->leave($guardKey);
                }
            }
            $result = $batch->sendResultAt($index);
            if ($result === null) {
                throw new MQClientException('send result is null, the batch may be in-flight');
            }
            return $result;
        }
    }

    /** Java `send(Message, SendCallback, DefaultMQProducer)`：入队，结果经 pump 后回调。 */
    public function sendAsync(Message $msg, callable $sendCallback): void
    {
        $this->sendAsyncImpl(AggregateKey::ofMessage($msg), $msg, $sendCallback);
    }

    public function sendAsyncWithMq(Message $msg, MessageQueue $mq, callable $sendCallback): void
    {
        $this->sendAsyncImpl(AggregateKey::ofMessageWithMq($msg, $mq), $msg, $sendCallback);
    }

    private function sendAsyncImpl(AggregateKey $key, Message $msg, callable $sendCallback): void
    {
        while (true) {
            $batch = $this->getOrCreateAsyncBatch($key);
            if (!$batch->addAsync($msg, $sendCallback)) {
                $this->removeAsyncBatch($key, $batch);
                continue;
            }
            return;
        }
    }

    // ================================================================ 进程级复用

    /**
     * 对应 Java `MQClientManager.getOrCreateProduceAccumulator`：按 clientId 缓存。
     *
     * 同进程里 clientId 相同的调用方共享同一个累加器（与同一对守卫状态），因此第一个建出来的
     * sender/clock 胜出；后传的 sender/clock 只在首次创建时生效（与 Java 的"第一次记下的那个
     * 用到天荒地老"一致）。
     */
    public static function getOrCreateProduceAccumulator(
        string $clientId,
        ?callable $sender = null,
        ?callable $clock = null,
    ): self {
        return self::$sharedInstances[$clientId] ??= new self($clientId, $sender, $clock);
    }

    /** 清空进程级复用表（单测隔离用）。 */
    public static function clearSharedInstances(): void
    {
        self::$sharedInstances = [];
    }
}

/**
 * 归并键：`topic + mq + waitStoreMsgOK + tag`（Java `ProduceAccumulator.AggregateKey`）。
 */
final class AggregateKey
{
    public function __construct(
        public readonly string $topic,
        public readonly ?MessageQueue $mq,
        public readonly bool $waitStoreMsgOk,
        public readonly ?string $tag,
    ) {
    }

    public static function ofMessage(Message $msg): self
    {
        // Java 用 message.isWaitStoreMsgOK()：**缺省即 true**。
        return new self($msg->getTopic(), null, Message::isWaitStoreMsgOk($msg), $msg->getTags());
    }

    public static function ofMessageWithMq(Message $msg, MessageQueue $mq): self
    {
        return new self($msg->getTopic(), $mq, Message::isWaitStoreMsgOk($msg), $msg->getTags());
    }

    public function equals(self $other): bool
    {
        if ($this->waitStoreMsgOk !== $other->waitStoreMsgOk
            || $this->topic !== $other->topic
            || $this->tag !== $other->tag) {
            return false;
        }
        if ($this->mq === null || $other->mq === null) {
            return $this->mq === $other->mq;
        }
        return $this->mq->equals($other->mq);
    }

    /**
     * 本进程内的表键：只要「相等的键给出相同的串」即可，具体形式无需与 Java hashCode 对齐。
     *
     * tag 的 null 与 ""（空串）是两回事、mq 的 null 与"有值"也是两回事，故各自做了区分标记。
     */
    public function keyString(): string
    {
        $mqPart = $this->mq === null
            ? "\x01no-mq"
            : "\x01mq" . $this->mq->topic . "\x02" . $this->mq->brokerName . "\x02" . $this->mq->queueId;
        $tagPart = $this->tag === null ? "\x01no-tag" : "\x01tag" . $this->tag;
        return $this->topic . "\x00" . ($this->waitStoreMsgOk ? '1' : '0') . $tagPart . $mqPart;
    }

    public function __toString(): string
    {
        return $this->keyString();
    }
}

/**
 * 把两个函数包成 Java 匿名 `SendCallback`（对应蓝本的 `_InternalSendCallback`）。
 *
 * 注入的 sender 在异步路径拿到本对象后，成功调 `onSuccess($result)`、失败调 `onException($e)`。
 */
final class InternalSendCallback
{
    /** @var \Closure(?SendResult): void */
    private \Closure $onSuccess;
    /** @var \Closure(\Throwable): void */
    private \Closure $onException;

    /**
     * @param callable(?SendResult): void $onSuccess
     * @param callable(\Throwable): void  $onException
     */
    public function __construct(callable $onSuccess, callable $onException)
    {
        $this->onSuccess = $onSuccess(...);
        $this->onException = $onException(...);
    }

    public function onSuccess(?SendResult $sendResult): void
    {
        ($this->onSuccess)($sendResult);
    }

    public function onException(\Throwable $e): void
    {
        ($this->onException)($e);
    }
}

/**
 * 一批待归并的消息（Java `ProduceAccumulator.MessageAccumulation`）。
 *
 * ⚠ PHP 单线程下 `add()` **不阻塞**（蓝本会阻塞到本批发完）：入队后由 `ProduceAccumulator`
 * 的显式 pump（`tick`/`flush`/`flushAll`）或同步 `send()` 来驱动发送。
 */
final class MessageAccumulation
{
    /** @var list<Message> */
    public array $messages = [];
    /** @var list<callable(?SendResult, ?\Throwable): void> 每条消息各自的异步回调 */
    public array $sendCallbacks = [];
    /** @var array<string,true> 同步 add 收集、异步 add 不收集（Java 的不对称行为） */
    public array $keys = [];
    public bool $closed = false;
    /** @var list<?SendResult> */
    public array $sendResults = [];
    public int $messagesSize = 0;
    public int $count = 0;
    public int $createTime;

    public function __construct(
        public readonly AggregateKey $aggregateKey,
        private readonly ProduceAccumulator $owner,
    ) {
        $this->createTime = $owner->nowMs();
    }

    // ---------------- 阈值 ----------------

    /** Java `readyToSend()`：按**本批**字节数或本批存活时间（不是全局限额）。 */
    public function readyToSend(?int $nowMs = null): bool
    {
        if ($this->messagesSize > $this->owner->getHoldSize()) {
            return true;
        }
        $now = $nowMs ?? $this->owner->nowMs();
        return $now >= $this->createTime + $this->owner->getHoldMs();
    }

    // ---------------- 加入 ----------------

    /**
     * 同步加入；返回本条消息在本批里的下标，`-1` 表示本批已关闭（调用方需重取/新建）。
     */
    public function add(Message $msg): int
    {
        if ($this->closed) {
            return -1;
        }
        $ret = $this->count;
        $this->count++;
        $this->messages[] = $msg;
        $body = $msg->getBody();
        $len = $body === null ? 0 : strlen($body);
        if ($len > 0) {
            $this->messagesSize += $len;
        }
        $msgKeys = $msg->getKeys();
        if ($msgKeys !== null) {
            foreach (self::splitKeys($msgKeys) as $k) {
                $this->keys[$k] = true;
            }
        }
        return $ret;
    }

    /** 异步加入；`false` 表示本批已关闭（调用方需重取/新建）。 */
    public function addAsync(Message $msg, callable $sendCallback): bool
    {
        if ($this->closed) {
            return false;
        }
        $this->count++;
        $this->messages[] = $msg;
        $this->sendCallbacks[] = $sendCallback;
        $body = $msg->getBody();
        $len = $body === null ? 0 : strlen($body);
        if ($len > 0) {
            $this->messagesSize += $len;
        }
        if ($this->readyToSend()) {
            $this->sendAsync($sendCallback);
        }
        return true;
    }

    /**
     * Java `wakeup()`：叫醒正在 `add` 里等阈值的调用方去自查 `readyToSend`。
     *
     * PHP 没有阻塞的等待者，本方法保留为语义占位（无实际唤醒动作）。
     */
    public function wakeup(): void
    {
        // no-op：单线程下不存在阻塞在 add 里等待的调用方。
    }

    // ---------------- 组装 / 拆分 ----------------

    /** Java `batch()`：把本批组装成一个 MessageBatch。 */
    public function batch(): MessageBatch
    {
        $batch = new MessageBatch($this->messages);
        $batch->setTopic($this->aggregateKey->topic);
        $batch->setWaitStoreMsgOk($this->aggregateKey->waitStoreMsgOk);
        // 无条件写（空集合即 KEYS=""）
        $batch->setKeys(implode(MessageConst::KEY_SEPARATOR, array_keys($this->keys)));
        if ($this->aggregateKey->tag !== null) {
            $batch->setTags($this->aggregateKey->tag);
        }
        MessageClientIdSetter::setUniqId($batch);
        $batch->setBody($batch->encode());
        return $batch;
    }

    /** Java `splitSendResults`：批量应答拆成逐条 SendResult。 */
    public function splitSendResults(?SendResult $sendResult): void
    {
        if ($sendResult === null) {
            throw new \InvalidArgumentException('sendResult is null');
        }
        $msgId = $sendResult->msgId ?? '';
        $this->sendResults = array_fill(0, $this->count, null);
        if (str_contains($msgId, ',')) {
            $msgIds = explode(',', $msgId);
            $offsetMsgIds = explode(',', $sendResult->offsetMsgId ?? '');
            if (count($offsetMsgIds) !== $this->count || count($msgIds) !== $this->count) {
                throw new \InvalidArgumentException('sendResult is illegal');
            }
            for ($i = 0; $i < $this->count; $i++) {
                $this->sendResults[$i] = new SendResult(
                    $sendResult->sendStatus,
                    $msgIds[$i],
                    $sendResult->messageQueue,
                    $sendResult->queueOffset + $i,
                    $sendResult->transactionId,
                    $offsetMsgIds[$i],
                    $sendResult->regionId,
                );
            }
        } else {
            // 不含逗号：老 broker / 单条应答，所有下标共享同一个 result 对象（Java 同）。
            for ($i = 0; $i < $this->count; $i++) {
                $this->sendResults[$i] = $sendResult;
            }
        }
    }

    public function sendResultAt(int $index): ?SendResult
    {
        return $this->sendResults[$index] ?? null;
    }

    // ---------------- 发送 ----------------

    /** Java `MessageAccumulation.send()`（同步）。 */
    public function sendSync(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $batch = $this->batch();
        try {
            $result = $this->owner->sendDirect($batch, $this->aggregateKey->mq, null);
            $this->splitSendResults($result);
        } finally {
            // 无论成败都归还全局字节额度（Java：finally 里 currentlyHoldSize -= messagesSize）
            $this->owner->releaseHold($this->messagesSize);
        }
    }

    /**
     * Java `MessageAccumulation.send(SendCallback)`（异步）。
     *
     * 参数 `$sendCallback` 与 Java 一样**不参与**逻辑（Java 收了但没用），回调来自
     * `self.sendCallbacks` 列表。批量应答回来后逐条分发给各自的回调。
     */
    public function sendAsync(?callable $sendCallback = null): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $batch = $this->batch();
        $size = $this->messagesSize;

        $onException = function (\Throwable $e) use ($size): void {
            foreach ($this->sendCallbacks as $cb) {
                $cb(null, $e);
            }
            $this->owner->releaseHold($size);
        };

        $onSuccess = function (?SendResult $sendResult) use ($size, $onException): void {
            try {
                $this->splitSendResults($sendResult);
                $i = 0;
                foreach ($this->sendCallbacks as $cb) {
                    $cb($this->sendResults[$i], null);
                    $i++;
                }
                if ($i !== $this->count) {
                    throw new \InvalidArgumentException('sendResult is illegal');
                }
                $this->owner->releaseHold($size);
            } catch (\Throwable $e) {
                // 与 Java 一样：内部异常转给全体回调
                $onException($e);
            }
        };

        try {
            $this->owner->sendDirect(
                $batch,
                $this->aggregateKey->mq,
                new InternalSendCallback($onSuccess, $onException),
            );
        } catch (\Throwable $e) {
            // ⚠ Java 在这里**没有**归还 currentlyHoldSize（只有回调路径会还）——照抄，别修。
            foreach ($this->sendCallbacks as $cb) {
                $cb(null, $e);
            }
        }
    }

    /** Java `String.split(" ")` 的等价物（limit=0 时丢弃尾部空串，保留中间空段）。 */
    /** @return list<string> */
    private static function splitKeys(string $msgKeys): array
    {
        $parts = explode(MessageConst::KEY_SEPARATOR, $msgKeys);
        while ($parts !== [] && $parts[count($parts) - 1] === '') {
            array_pop($parts);
        }
        return $parts;
    }
}

/**
 * 守卫基类（对应蓝本 `_GuardService` / Java 的 `ServiceThread` 子类）。
 *
 * PHP 单线程下没有线程体，`doWork()` 由 `ProduceAccumulator::tick()` 显式驱动；本类保留
 * start/shutdown 与「同一 key 在途时不重复触发」的在途集合。
 */
class GuardService
{
    /** @var array<string,true> 正在发送的 key 集合（防重入） */
    private array $inFlight = [];
    private bool $stopped = false;

    public function __construct(
        protected ProduceAccumulator $owner,
        public readonly string $name,
    ) {
    }

    public function start(): void
    {
        $this->stopped = false;
    }

    public function shutdown(): void
    {
        $this->stopped = true;
        $this->inFlight = [];
    }

    public function isStopped(): bool
    {
        return $this->stopped;
    }

    /** Java：`Math.max(1, holdMs / 2)`（单线程下仅用于取证/对齐）。 */
    public function sleepTimeMs(): int
    {
        return max(1, intdiv($this->owner->getHoldMs(), 2));
    }

    /** 进入某 key 的发送临界区；已在途则返回 false（不重复触发）。 */
    public function tryEnter(string $keyString): bool
    {
        if (isset($this->inFlight[$keyString])) {
            return false;
        }
        $this->inFlight[$keyString] = true;
        return true;
    }

    public function leave(string $keyString): void
    {
        unset($this->inFlight[$keyString]);
    }

    public function isInFlight(string $keyString): bool
    {
        return isset($this->inFlight[$keyString]);
    }

    /** 一轮守卫主体（子类实现）。 */
    public function doWork(?int $nowMs = null): void
    {
        throw new \LogicException('doWork() must be implemented by a concrete guard');
    }
}

/**
 * 同步发送的守卫（Java `GuardForSyncSendService`）。
 *
 * 蓝本里同步守卫只 `wakeup()`（真正发送由被唤醒的 add 执行）；PHP 无阻塞 add，因此这里在
 * 到期时直接 `sendSync()`，并保留「messagesSize == 0 的空批次置 closed + 摘表」的清理口径。
 */
final class GuardForSyncSend extends GuardService
{
    public function __construct(ProduceAccumulator $owner)
    {
        parent::__construct($owner, 'Client_' . $owner->instanceName . '_GuardForSyncSend');
    }

    public function doWork(?int $nowMs = null): void
    {
        foreach ($this->owner->syncSendBatchesSnapshot() as $v) {
            $ks = $v->aggregateKey->keyString();
            $v->wakeup();
            if (!$v->closed && $v->readyToSend($nowMs) && $this->tryEnter($ks)) {
                try {
                    $v->sendSync();
                } catch (\Throwable $e) {
                    Logger::warning($this->name . ' service has exception. ' . $e->getMessage());
                } finally {
                    $this->leave($ks);
                }
            }
            // 摘表：只清理 messagesSize == 0 的空批次（发过的批次 size 仍 > 0，留在表里）。
            if ($v->messagesSize === 0) {
                $v->closed = true;
                $this->owner->removeSyncBatch($v->aggregateKey, $v);
            }
        }
    }
}

/** 异步发送的守卫（Java `GuardForAsyncSendService`）。 */
final class GuardForAsyncSend extends GuardService
{
    public function __construct(ProduceAccumulator $owner)
    {
        parent::__construct($owner, 'Client_' . $owner->instanceName . '_GuardForAsyncSend');
    }

    public function doWork(?int $nowMs = null): void
    {
        foreach ($this->owner->asyncSendBatchesSnapshot() as $v) {
            $ks = $v->aggregateKey->keyString();
            if (!$v->closed && $v->readyToSend($nowMs) && $this->tryEnter($ks)) {
                try {
                    $v->sendAsync(null);
                } catch (\Throwable $e) {
                    Logger::warning($this->name . ' service has exception. ' . $e->getMessage());
                } finally {
                    $this->leave($ks);
                }
            }
            if ($v->messagesSize === 0) {
                $v->closed = true;
                $this->owner->removeAsyncBatch($v->aggregateKey, $v);
            }
        }
    }
}

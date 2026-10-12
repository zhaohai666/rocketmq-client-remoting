<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageBatch;
use RocketMQ\Common\MessageClientIdSetter;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageQueue;

final class ProduceAccumulator
{
    public const DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024;
    public const DEFAULT_HOLD_SIZE = 32 * 1024;
    public const DEFAULT_HOLD_MS = 10;

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

final class MessageAccumulation
{
    /** @var list<Message> */
    public array $messages = [];
    /** @var list<callable(?SendResult, ?\Throwable): void> 每条消息各自的异步回调 */
    public array $sendCallbacks = [];
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

    public function wakeup(): void
    {
        // no-op：单线程下不存在阻塞在 add 里等待的调用方。
    }

    // ---------------- 组装 / 拆分 ----------------

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
            $this->owner->releaseHold($this->messagesSize);
        }
    }

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
            foreach ($this->sendCallbacks as $cb) {
                $cb(null, $e);
            }
        }
    }

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

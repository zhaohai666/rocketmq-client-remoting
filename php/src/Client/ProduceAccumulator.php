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

    /** @var array<string,MessageAccumulation> sync table, keyed by AggregateKey::keyString() */
    private array $syncSendBatches = [];
    /** @var array<string,MessageAccumulation> async table, keyed by AggregateKey::keyString() */
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

    // ================================================================ Lifecycle

    /** Idempotent and repeatable: `start -> shutdown -> start` (producer restart) is not blocked by a "can only start once" thread. */
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

    /** Current value of the injected clock (milliseconds). */
    public function nowMs(): int
    {
        return ($this->clock)();
    }

    // ================================================================ Global byte gate

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

    // ================================================================ Table operations

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

    // ================================================================ Send target

    public function sendDirect(MessageBatch $batch, ?MessageQueue $mq, ?InternalSendCallback $callback): ?SendResult
    {
        if ($this->sender === null) {
            throw new MQClientException('sender is not configured, can not send batch message');
        }
        return ($this->sender)($batch, $mq, $callback);
    }

    // ================================================================ Explicit pump

    /**
     * Replicates one round of the guard thread's `doWork`: runs both the sync and async
     * tables once each.
     *
     * @param int|null $nowMs injected "current time" (milliseconds); null uses the injected clock
     */
    public function tick(?int $nowMs = null): void
    {
        $this->guardSync->doWork($nowMs);
        $this->guardAsync->doWork($nowMs);
    }

    /** Flush one key immediately (tries both the sync and async tables). */
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

    /** Flush all batches in both tables immediately. */
    public function flushAll(): void
    {
        foreach ($this->syncSendBatchesSnapshot() as $batch) {
            $this->pumpOne($batch, true);
        }
        foreach ($this->asyncSendBatchesSnapshot() as $batch) {
            $this->pumpOne($batch, false);
        }
    }

    /** Sending of a single batch inside pump: wrap exceptions (the guard thread likewise relies on the outer catch to swallow and continue). */
    private function pumpOne(MessageAccumulation $batch, bool $sync): void
    {
        $guard = $sync ? $this->guardSync : $this->guardAsync;
        $ks = $batch->aggregateKey->keyString();
        if (!$guard->tryEnter($ks)) {
            return; // the same key is being sent, do not trigger again
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

    // ================================================================ External send entry points

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
                // This batch was closed before this add: detach it, re-fetch/create a new one and retry
                $this->removeSyncBatch($key, $batch);
                continue;
            }
            // Single-threaded adaptation: there is no blocking add waiter, so this key must be pumped immediately after enqueueing to get the result (exceptions propagate as usual).
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

    // ================================================================ Process-level reuse

    public static function getOrCreateProduceAccumulator(
        string $clientId,
        ?callable $sender = null,
        ?callable $clock = null,
    ): self {
        return self::$sharedInstances[$clientId] ??= new self($clientId, $sender, $clock);
    }

    /** Clear the process-level reuse table (for unit test isolation). */
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
    /** @var list<callable(?SendResult, ?\Throwable): void> per-message async callback */
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

    // ---------------- Threshold ----------------

    public function readyToSend(?int $nowMs = null): bool
    {
        if ($this->messagesSize > $this->owner->getHoldSize()) {
            return true;
        }
        $now = $nowMs ?? $this->owner->nowMs();
        return $now >= $this->createTime + $this->owner->getHoldMs();
    }

    // ---------------- Join ----------------

    /**
     * Join synchronously; returns this message's index in the batch, `-1` means the batch
     * is already closed (the caller must re-fetch/create).
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

    /** Join asynchronously; `false` means the batch is already closed (the caller must re-fetch/create). */
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
        // no-op: under a single thread there is no caller blocked inside add waiting.
    }

    // ---------------- Assembly / splitting ----------------

    public function batch(): MessageBatch
    {
        $batch = new MessageBatch($this->messages);
        $batch->setTopic($this->aggregateKey->topic);
        $batch->setWaitStoreMsgOk($this->aggregateKey->waitStoreMsgOk);
        // Write unconditionally (an empty collection means KEYS="")
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

    // ---------------- Send ----------------

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
    /** @var array<string,true> set of keys currently being sent (re-entrancy guard) */
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

    /** Enter a key's send critical section; returns false if already in flight (do not trigger again). */
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

    /** The main body of one guard round (implemented by subclasses). */
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
            // Table pruning: only clean up empty batches with messagesSize == 0 (sent batches still have size > 0 and stay in the table).
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

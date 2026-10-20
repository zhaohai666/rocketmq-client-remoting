<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageExt;

/**
 * Consume/pull result types: pull status, consume return type, POP result, and
 * consume context/listener contracts.
 */

/** Pull status; values come from the status field of the classic remoting protocol pull response. */
enum PullStatus: int
{
    case FOUND = 0;
    case NO_NEW_MSG = 1;
    case NO_MATCHED_MSG = 2;
    case OFFSET_ILLEGAL = 3;

    /** Build from a protocol status code; unknown codes fall back to FOUND. */
    public static function fromCode(int $code): self
    {
        return self::tryFrom($code) ?? self::FOUND;
    }
}

/**
 * Consume return type.
 *
 * WARNING: the order is the ordinal -- the trace SubAfter ``contextCode`` uses exactly
 * this ordinal; reordering breaks the console display.
 */
enum ConsumeReturnType: int
{
    case SUCCESS = 0;
    case TIME_OUT = 1;
    case EXCEPTION = 2;
    case RETURNNULL = 3;
    case FAILED = 4;
}

/** POP pull status; values come from the status field of the broker pop response. */
enum PopStatus: int
{
    case FOUND = 0;
    case NO_NEW_MSG = 1;
    case POLLING_FULL = 2;
    case POLLING_NOT_FOUND = 3;
}

/** Concurrent consume return status. */
enum ConsumeConcurrentlyStatus: int
{
    case CONSUME_SUCCESS = 0;
    case RECONSUME_LATER = 1;
}

/**
 * Orderly consume return status.
 *
 * ORDINAL itself is not an online value, but callers that "hard-code by ordinal" depend
 * on the declaration order; reordering shifts meaning. COMMIT/ROLLBACK belong to a
 * deprecated branch that only serves the binlog consumption scenario.
 */
enum ConsumeOrderlyStatus: int
{
    case SUCCESS = 0;
    case ROLLBACK = 1;
    case COMMIT = 2;
    case SUSPEND_CURRENT_QUEUE_A_MOMENT = 3;
}

/** Pull result, including the brokerId field the broker suggests for the next pull. */
final class PullResult
{
    /**
     * @param list<MessageExt> $msgFoundList
     * @param int|null $suggestWhichBrokerId which brokerId the broker suggests pulling from
     *        next (possibly a slave in a master-slave deployment); null = not present in
     *        the response header (old broker).
     */
    public function __construct(
        public PullStatus $status,
        public int $nextBeginOffset = 0,
        public int $minOffset = 0,
        public int $maxOffset = 0,
        public array $msgFoundList = [],
        public ?int $suggestWhichBrokerId = null,
    ) {
    }

    public function __toString(): string
    {
        return sprintf(
            'PullResult [status=%s, nextBeginOffset=%d, minOffset=%d, maxOffset=%d, msgFoundList.size=%d]',
            'PullStatus.' . $this->status->name,
            $this->nextBeginOffset,
            $this->minOffset,
            $this->maxOffset,
            count($this->msgFoundList),
        );
    }
}

/**
 * POP response.
 *
 * ``startOffsetInfo`` / ``msgOffsetInfo`` / ``orderCountInfo`` keep the broker's raw
 * strings as-is; parsing is left to ExtraInfoUtil. Every message in ``msgFoundList``
 * is already stamped with the ``POP_CK`` (client re-construction) and ``1ST_POP_TIME``
 * properties.
 */
final class PopResult
{
    /** @param list<MessageExt> $msgFoundList */
    public function __construct(
        public PopStatus $status,
        public array $msgFoundList = [],
        public int $restNum = 0,
        public int $popTime = 0,
        public int $invisibleTime = 0,
        public int $reviveQid = 0,
        public ?string $startOffsetInfo = null,
        public ?string $msgOffsetInfo = null,
        public ?string $orderCountInfo = null,
    ) {
    }

    public function __toString(): string
    {
        return sprintf(
            'PopResult [status=%s, restNum=%d, popTime=%d, invisibleTime=%d, reviveQid=%d, msgFoundList.size=%d]',
            'PopStatus.' . $this->status->name,
            $this->restNum,
            $this->popTime,
            $this->invisibleTime,
            $this->reviveQid,
            count($this->msgFoundList),
        );
    }
}

/**
 * Result of a CHANGE_INVISIBLE_TIME request.
 *
 * ``extraInfo`` is an 8-segment CK string rebuilt with the **new** popTime/invisibleTime/
 * reviveQid from the response; the subsequent ACK uses it (not the old string passed in
 * with the request).
 */
final class ChangeInvisibleTimeResult
{
    public bool $success;

    public function __construct(
        public int $responseCode,
        public int $popTime = 0,
        public int $invisibleTime = 0,
        public int $reviveQid = 0,
        public ?string $extraInfo = null,
    ) {
        $this->success = $responseCode === 0;
    }

    public function __toString(): string
    {
        return sprintf(
            'ChangeInvisibleTimeResult [responseCode=%d, popTime=%d, invisibleTime=%d, reviveQid=%d]',
            $this->responseCode,
            $this->popTime,
            $this->invisibleTime,
            $this->reviveQid,
        );
    }
}

/** Concurrent consume context. */
final class ConsumeConcurrentlyContext
{
    /**
     * Delay level for the next consume attempt, default 0.
     * When 0, the caller rewrites it to 3 + reconsumeTimes.
     */
    public int $delayLevelWhenNextConsume = 0;

    /**
     * ackIndex: "up to which message the listener acknowledges"; the index includes the
     * message itself, and messages after it are redelivered/dropped per status. The
     * default is "acknowledge the whole batch" (2^31-1); only when the listener actively
     * lowers it does a partial ack happen.
     */
    public int $ackIndex;

    public function __construct(public ?\RocketMQ\Common\MessageQueue $messageQueue = null)
    {
        $this->ackIndex = (1 << 31) - 1;
    }
}

/** Orderly consume context. */
final class ConsumeOrderlyContext
{
    /**
     * autoCommit: when true the client commits the offset per the listener's status
     * (the normal path); the listener sets false to take over the commit right.
     */
    public bool $autoCommit = true;

    /**
     * suspendCurrentQueueTimeMillis, **default -1**: -1 means "unspecified"; the suspend
     * duration falls back to the consumer's configured ``suspendCurrentQueueTimeMillis``
     * (default 1000); the parsed value is then clamped to [10, 30000] on the scheduler side.
     */
    public int $suspendCurrentQueueTimeMillis = -1;

    public function __construct(public ?\RocketMQ\Common\MessageQueue $messageQueue = null)
    {
    }
}

/**
 * Concurrent consume listener: receives a batch of messages at a time and returns the
 * disposition status of the whole batch.
 */
interface MessageListenerConcurrently
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus;
}

/** Orderly consume listener: serial callbacks within the same queue; the status decides suspend/commit. */
interface MessageListenerOrderly
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus;
}

/** Compatibility alias: order-agnostic; treat as a concurrent listener for now. */
interface MessageListener
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, mixed $context): ConsumeConcurrentlyStatus;
}

/**
 * Asynchronous pull outcome: exactly one of the two methods fires per request.
 *
 * ``onSuccess`` receives the decoded pull result (status, next begin offset and the message
 * list already read from the response); ``onException`` receives the reason the request could
 * not produce one -- a broker error code, a transport failure or a timeout.
 */
interface PullCallback
{
    public function onSuccess(PullResult $pullResult): void;

    public function onException(\Throwable $e): void;
}

/**
 * The ``status`` in the hook context: the conventional form is the **bare enum member name**.
 *
 * What the concurrent/orderly consume services write into the context ``status`` is the
 * member name of the normalized enum: ``CONSUME_SUCCESS`` / ``RECONSUME_LATER`` /
 * ``SUCCESS`` / ``SUSPEND_CURRENT_QUEUE_A_MOMENT`` / ``COMMIT`` / ``ROLLBACK``.
 * status is externally visible to hooks and trace logs, so it is uniformly taken by
 * ``name``, without the class-name prefix. Fallback for non-enum values: use
 * ``__toString`` if present, otherwise fall back to the class name; scalars are cast
 * to string directly.
 *
 * WARNING: this function is only defined when a class in this file is autoloaded (PHP's
 * autoloader does not load functions); callers usually `use` the enums from this file
 * first, so the file has already been required.
 */
function consumeStatusName(mixed $status): string
{
    if ($status instanceof \UnitEnum) {
        return $status->name;
    }
    if (is_object($status)) {
        if (method_exists($status, '__toString')) {
            return (string) $status;
        }
        return get_class($status);
    }
    if ($status === null) {
        return 'None';
    }
    if (is_bool($status)) {
        return $status ? 'True' : 'False';
    }
    return (string) $status;
}

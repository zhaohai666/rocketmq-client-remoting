<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageExt;

/**
 * 消费/拉取结果类型：拉取状态、消费返回类型、POP 结果与消费上下文/监听器契约。
 */

/** 拉取状态，取值来自经典 remoting 协议 pull 响应的 status 字段。 */
enum PullStatus: int
{
    case FOUND = 0;
    case NO_NEW_MSG = 1;
    case NO_MATCHED_MSG = 2;
    case OFFSET_ILLEGAL = 3;

    /** 由协议状态码构造：未知码兜底为 FOUND。 */
    public static function fromCode(int $code): self
    {
        return self::tryFrom($code) ?? self::FOUND;
    }
}

/**
 * 消费返回类型。
 *
 * ⚠ 顺序即 ordinal —— 轨迹 SubAfter 的 ``contextCode`` 用的正是 ordinal，
 * 改动顺序会让控制台显示错乱。
 */
enum ConsumeReturnType: int
{
    case SUCCESS = 0;
    case TIME_OUT = 1;
    case EXCEPTION = 2;
    case RETURNNULL = 3;
    case FAILED = 4;
}

/** POP 拉取状态，取值来自 broker pop 响应的 status 字段。 */
enum PopStatus: int
{
    case FOUND = 0;
    case NO_NEW_MSG = 1;
    case POLLING_FULL = 2;
    case POLLING_NOT_FOUND = 3;
}

/** 并发消费返回状态。 */
enum ConsumeConcurrentlyStatus: int
{
    case CONSUME_SUCCESS = 0;
    case RECONSUME_LATER = 1;
}

/**
 * 顺序消费返回状态。
 *
 * ORDINAL 本身不是线上值，但「按 ordinal 写死」的调用方依赖声明顺序，改动会错位；
 * COMMIT/ROLLBACK 属于已废弃分支，只服务于 binlog 消费场景。
 */
enum ConsumeOrderlyStatus: int
{
    case SUCCESS = 0;
    case ROLLBACK = 1;
    case COMMIT = 2;
    case SUSPEND_CURRENT_QUEUE_A_MOMENT = 3;
}

/** 拉取结果，含 broker 建议下次拉取的 brokerId 字段。 */
final class PullResult
{
    /**
     * @param list<MessageExt> $msgFoundList
     * @param int|null $suggestWhichBrokerId broker 建议下次从哪个 brokerId 拉
     *        （主从部署时可能是从节点）；null = 响应头没带（老 broker）。
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
 * POP 响应。
 *
 * ``startOffsetInfo`` / ``msgOffsetInfo`` / ``orderCountInfo`` 保留 broker 原样
 * 字符串，解析交给 ExtraInfoUtil；``msgFoundList`` 里的每条消息
 * 都已盖好 ``POP_CK``（客户端反构）与 ``1ST_POP_TIME`` 属性。
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
 * CHANGE_INVISIBLE_TIME 请求的结果。
 *
 * ``extraInfo`` 是用响应里**新的** popTime/invisibleTime/reviveQid 重建的 8 段 CK 串，
 * 后续 ACK 要用它（不是请求时传进去的那个旧串）。
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

/** 并发消费上下文。 */
final class ConsumeConcurrentlyContext
{
    /**
     * 下次消费的延迟等级，缺省 0。
     * 为 0 时由 caller 改写为 3 + reconsumeTimes。
     */
    public int $delayLevelWhenNextConsume = 0;

    /**
     * ackIndex：「listener 认可到第几条」，下标含自身，其后的消息按状态回投/丢弃。
     * 默认值是「全批认可」（2^31-1），只有 listener 主动调小才会部分 ack。
     */
    public int $ackIndex;

    public function __construct(public ?\RocketMQ\Common\MessageQueue $messageQueue = null)
    {
        $this->ackIndex = (1 << 31) - 1;
    }
}

/** 顺序消费上下文。 */
final class ConsumeOrderlyContext
{
    /**
     * autoCommit：true 时由客户端按 listener 的状态提交位点（正常路径）；
     * listener 置 false 拿走提交权。
     */
    public bool $autoCommit = true;

    /**
     * suspendCurrentQueueTimeMillis，**默认 -1**：-1 表示"没指定"，挂起时长回落到
     * 消费者配置的 ``suspendCurrentQueueTimeMillis``（默认 1000）；解析出的值再由
     * 调度侧钳到 [10, 30000]。
     */
    public int $suspendCurrentQueueTimeMillis = -1;

    public function __construct(public ?\RocketMQ\Common\MessageQueue $messageQueue = null)
    {
    }
}

/**
 * 并发消费监听器：一次收到一批消息，返回整批的处置状态。
 */
interface MessageListenerConcurrently
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus;
}

/** 顺序消费监听器：同一队列内串行回调，返回状态决定挂起/提交。 */
interface MessageListenerOrderly
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus;
}

/** 兼容别名：不区分顺序性，先按并发监听器对待。 */
interface MessageListener
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, mixed $context): ConsumeConcurrentlyStatus;
}

/**
 * 钩子上下文里的 ``status``：约定形态是**裸枚举成员名**。
 *
 * 并发/顺序消费服务写进上下文 ``status`` 的都是归一化后那个枚举的成员名：
 * ``CONSUME_SUCCESS`` / ``RECONSUME_LATER`` / ``SUCCESS`` /
 * ``SUSPEND_CURRENT_QUEUE_A_MOMENT`` / ``COMMIT`` / ``ROLLBACK``。
 * status 对钩子和轨迹日志都是对外可见的，所以统一按 ``name`` 取，不带类名前缀。
 * 非枚举值兜底：有 ``__toString`` 就用它，否则退到类名，标量直接转字符串。
 *
 * ⚠ 本文件里的类被 autoload 时该函数才会被定义（PHP 的 autoloader 不加载函数）；
 * 调用方通常先 `use` 了本文件的枚举，所以文件已被 require。
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

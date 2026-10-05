<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageExt;

/**
 * 消费/拉取结果类型（对应 org.apache.rocketmq.client.consumer.* 及 pull.*），
 * 移植自 consumer_result.py。
 */

/** 对应 org.apache.rocketmq.client.consumer.PullStatus。 */
enum PullStatus: int
{
    case FOUND = 0;
    case NO_NEW_MSG = 1;
    case NO_MATCHED_MSG = 2;
    case OFFSET_ILLEGAL = 3;

    /** 对应 ``PullStatus.from_code``：未知码兜底为 FOUND。 */
    public static function fromCode(int $code): self
    {
        return self::tryFrom($code) ?? self::FOUND;
    }
}

/**
 * 消费返回类型（对应 Java client.consumer.listener.ConsumeReturnType）。
 *
 * ⚠ 顺序即 ordinal —— 轨迹 SubAfter 的 ``contextCode`` 用的正是 ordinal
 * （Java ConsumeMessageTraceHookImpl:113），改动顺序会让控制台显示错乱。
 */
enum ConsumeReturnType: int
{
    case SUCCESS = 0;
    case TIME_OUT = 1;
    case EXCEPTION = 2;
    case RETURNNULL = 3;
    case FAILED = 4;
}

/** 对应 Java ``org.apache.rocketmq.client.consumer.PopStatus``。 */
enum PopStatus: int
{
    case FOUND = 0;
    case NO_NEW_MSG = 1;
    case POLLING_FULL = 2;
    case POLLING_NOT_FOUND = 3;
}

/** 对应 Java ``ConsumeConcurrentlyStatus``。 */
enum ConsumeConcurrentlyStatus: int
{
    case CONSUME_SUCCESS = 0;
    case RECONSUME_LATER = 1;
}

/**
 * 对应 Java ``ConsumeOrderlyStatus``。
 *
 * 声明顺序与 Java 逐字对齐（ORDINAL 不是线上值，但两侧一致才不会在"按 ordinal
 * 写死"的调用方手里错位）；COMMIT/ROLLBACK 在 Java 侧标注为 已废弃 + "only for
 * binlog consumption"。
 */
enum ConsumeOrderlyStatus: int
{
    case SUCCESS = 0;
    case ROLLBACK = 1;
    case COMMIT = 2;
    case SUSPEND_CURRENT_QUEUE_A_MOMENT = 3;
}

/** 对应 Java ``PullResult``（含 Java ``PullResultExt`` 的 suggestWhichBrokerId 字段）。 */
final class PullResult
{
    /**
     * @param list<MessageExt> $msgFoundList
     * @param int|null $suggestWhichBrokerId 对应 Java PullResultExt.suggestWhichBrokerId：
     *        broker 建议下次从哪个 brokerId 拉（主从部署时可能是从节点）。
     *        null = 响应头没带（老 broker）。
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
 * POP 响应（对应 Java ``PopResult``）。
 *
 * ``startOffsetInfo`` / ``msgOffsetInfo`` / ``orderCountInfo`` 保留 broker 原样
 * 字符串，解析交给 remoting.protocol.extra_info；``msgFoundList`` 里的每条消息
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
 * ``change_invisible_time`` 的结果。
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

/** 对应 Java ``ConsumeConcurrentlyContext``。 */
final class ConsumeConcurrentlyContext
{
    /**
     * 对应 Java ConsumeConcurrentlyContext.delayLevelWhenNextConsume（缺省 0）。
     * 为 0 时由 caller 改写为 3 + reconsumeTimes。
     */
    public int $delayLevelWhenNextConsume = 0;

    /**
     * 对应 Java ConsumeConcurrentlyContext.ackIndex（默认 Integer.MAX_VALUE）：
     * 「listener 认可到第几条」，下标含自身，其后的消息按状态回投/丢弃。
     * 默认值是「全批认可」，只有 listener 主动调小才会部分 ack。
     */
    public int $ackIndex;

    public function __construct(public ?\RocketMQ\Common\MessageQueue $messageQueue = null)
    {
        $this->ackIndex = (1 << 31) - 1;
    }
}

/** 对应 Java ``ConsumeOrderlyContext``。 */
final class ConsumeOrderlyContext
{
    /**
     * 对应 Java ConsumeOrderlyContext.autoCommit：true 时由客户端按 listener 的
     * 状态提交位点（正常路径）；listener 置 false 拿走提交权。
     */
    public bool $autoCommit = true;

    /**
     * 对应 Java ConsumeOrderlyContext.suspendCurrentQueueTimeMillis，**默认 -1**
     * （Java 就是这个默认值）：-1 表示"没指定"，挂起时长回落到消费者配置的
     * ``suspendCurrentQueueTimeMillis``（默认 1000）；解析出的值再由调度侧钳到
     * [10, 30000]。
     */
    public int $suspendCurrentQueueTimeMillis = -1;

    public function __construct(public ?\RocketMQ\Common\MessageQueue $messageQueue = null)
    {
    }
}

/**
 * 并发消费监听器（对应 Java ``MessageListenerConcurrently`` 接口）。
 *
 * Java 侧是接口，Python 侧是不可实例化语义的契约类；PHP 用 interface 表达。
 */
interface MessageListenerConcurrently
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus;
}

/** 顺序消费监听器（对应 Java ``MessageListenerOrderly`` 接口）。 */
interface MessageListenerOrderly
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus;
}

/** 兼容别名：先按并发监听器对待（对应 Python ``MessageListener``）。 */
interface MessageListener
{
    /** @param list<MessageExt> $msgs */
    public function consumeMessage(array $msgs, mixed $context): ConsumeConcurrentlyStatus;
}

/**
 * 钩子上下文里的 ``status``：Java ``status.toString()`` 的形态，即**裸枚举成员名**。
 *
 * Java 写进 ``ConsumeMessageContext.status`` 的是归一化后那个枚举的 ``toString()``
 * （并发 ConsumeMessageConcurrentlyService:408、顺序 ConsumeMessageOrderlyService:507），
 * 默认实现就是成员名：``CONSUME_SUCCESS`` / ``RECONSUME_LATER`` / ``SUCCESS`` /
 * ``SUSPEND_CURRENT_QUEUE_A_MOMENT`` / ``COMMIT`` / ``ROLLBACK``。Python 的
 * ``str(Enum member)`` 给的是带类名的形态，不是 Java 的；钩子 status 是公开可见的，
 * 所以统一按 ``name`` 取。非枚举保持 ``str()`` 兜底，与 Python 归一致。
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

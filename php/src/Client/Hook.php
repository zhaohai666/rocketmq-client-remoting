<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageType;

/**
 * 客户端钩子（对应 org.apache.rocketmq.client.hook 包），移植自 hook.py。
 *
 * Java 侧钩子是「业务无关的切面」：生产者在 sendKernelImpl 前后各调一次
 * SendMessageHook，消费者在投递 listener 前后各调一次 ConsumeMessageHook。
 *
 * 设计约定（与 Java 一致）：
 *  * 钩子抛出的异常**必须被吞掉并记 warn**，绝不能因为轨迹出错影响正常收发；
 *  * **唯二的例外**：``CheckForbiddenHook``（发送前拦截）——它的异常要向上传播；
 *  * ``mqTraceContext`` 是钩子自己的私有状态：before 写入、after 取出。
 *
 * Java 的钩子类型是接口，Python 侧是 raise NotImplementedError 的契约类；
 * PHP 用 interface 表达（由用户实现）。
 */

/** 对应 org.apache.rocketmq.client.hook.SendMessageContext。 */
final class SendMessageContext
{
    public mixed $producer = null;
    public string $producerGroup = '';
    public ?Message $message = null;
    public ?MessageQueue $mq = null;
    public string $brokerAddr = '';
    public string $bornHost = '';
    public ?string $communicationMode = null;
    public mixed $sendResult = null;
    public ?\Throwable $exception = null;
    public mixed $mqTraceContext = null;
    public mixed $props = null;
    public MessageType $msgType = MessageType::NORMAL_MSG;
    public string $namespace = '';
}

/** 对应 org.apache.rocketmq.client.hook.SendMessageHook。 */
interface SendMessageHook
{
    public function hookName(): string;

    public function sendMessageBefore(SendMessageContext $context): void;

    public function sendMessageAfter(SendMessageContext $context): void;
}

/** 对应 org.apache.rocketmq.client.hook.ConsumeMessageContext。 */
final class ConsumeMessageContext
{
    /** @var list<MessageExt> */
    public array $msgList;
    public ?MessageQueue $mq;
    public bool $success = true;
    public ?string $status = null;
    public mixed $mqTraceContext = null;
    public mixed $props = null;
    public mixed $accessChannel = null;

    /** @param list<MessageExt>|null $msgList */
    public function __construct(
        public string $consumerGroup = '',
        ?array $msgList = null,
        ?MessageQueue $mq = null,
    ) {
        $this->msgList = $msgList ?? [];
        $this->mq = $mq;
    }
}

/** 对应 org.apache.rocketmq.client.hook.ConsumeMessageHook。 */
interface ConsumeMessageHook
{
    public function hookName(): string;

    public function consumeMessageBefore(ConsumeMessageContext $context): void;

    public function consumeMessageAfter(ConsumeMessageContext $context): void;
}

/** 对应 org.apache.rocketmq.client.hook.EndTransactionContext。 */
final class EndTransactionContext
{
    public string $producerGroup = '';
    public ?Message $message = null;
    public string $brokerAddr = '';
    public ?string $msgId = null;
    public ?string $transactionId = null;
    public mixed $transactionState = null;
    public bool $fromTransactionCheck = false;
    public string $namespace = '';
}

/** 对应 org.apache.rocketmq.client.hook.EndTransactionHook。 */
interface EndTransactionHook
{
    public function hookName(): string;

    public function endTransaction(EndTransactionContext $context): void;
}

/**
 * 对应 org.apache.rocketmq.client.impl.CommunicationMode（Java 是枚举，三个常量）。
 *
 * Python 侧是带字符串常量的普通类（非 Enum），PHP 保持为常量类。
 */
final class CommunicationMode
{
    public const SYNC = 'SYNC';
    public const ASYNC = 'ASYNC';
    public const ONEWAY = 'ONEWAY';
}

/**
 * 对应 org.apache.rocketmq.client.hook.CheckForbiddenContext。
 *
 * 发送前拦截钩子的上下文。与 SendMessageContext 的关键差别：**没有 sendResult**
 * （此刻还没发），带上 ``arg``（send(msg, selector, arg) 里的业务参数）。
 */
final class CheckForbiddenContext
{
    public string $nameSrvAddr = '';
    public string $group = '';
    public ?Message $message = null;
    public ?MessageQueue $mq = null;
    public string $brokerAddr = '';
    public ?string $communicationMode = null;
    public mixed $sendResult = null;
    public ?\Throwable $exception = null;
    public mixed $arg = null;
    public bool $unitMode = false;
}

/**
 * 对应 org.apache.rocketmq.client.hook.CheckForbiddenHook。
 *
 * ⚠ 与 Send/Consume 钩子**相反**：``checkForbidden`` 抛出的异常**不会被吞掉**
 * （Java 签名就是 throws MQClientException），而是沿发送重试链向上传播 ——
 * 这正是"拦截"能力的实现方式。
 */
interface CheckForbiddenHook
{
    public function hookName(): string;

    public function checkForbidden(CheckForbiddenContext $context): void;
}

/**
 * 对应 org.apache.rocketmq.client.hook.FilterMessageContext。
 *
 * ``msgList`` 是**可变的**：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
 * （拉取路径 = 静默跳过；POP 路径 = 立刻 ack）。
 */
final class FilterMessageContext
{
    /** @var list<MessageExt> */
    public array $msgList;
    public ?MessageQueue $mq;
    public mixed $arg = null;
    public bool $unitMode = false;

    /** @param list<MessageExt>|null $msgList */
    public function __construct(
        public string $consumerGroup = '',
        ?array $msgList = null,
        ?MessageQueue $mq = null,
    ) {
        $this->msgList = $msgList ?? [];
        $this->mq = $mq;
    }
}

/** 对应 org.apache.rocketmq.client.hook.FilterMessageHook。 */
interface FilterMessageHook
{
    public function hookName(): string;

    public function filterMessage(FilterMessageContext $context): void;
}

<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\ClientErrorCode;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MixAll;

/**
 * Request-Reply（5.x）客户端侧支撑，移植自 request_reply.py。
 *
 * 对应 Java 的这几个类：
 * - org.apache.rocketmq.client.producer.RequestResponseFuture
 * - org.apache.rocketmq.client.producer.RequestFutureHolder
 * - org.apache.rocketmq.client.utils.MessageUtil#createReplyMessage
 * - org.apache.rocketmq.client.impl.ClientRemotingProcessor#receiveReplyMessage
 *
 * 协议回顾：
 *   请求方（producer.request）：msg.properties[CORRELATION_ID]=uuid /
 *   [REPLY_TO_CLIENT]=clientId / [TTL]=timeoutMillis；应答方 createReplyMessage 派生一条
 *   topic=<CLUSTER>_REPLY_TOPIC、MSG_TYPE="reply" 的消息，broker 按 REPLY_TO_CLIENT
 *   找到请求方连接把应答推回（PUSH_REPLY_MESSAGE_TO_CLIENT 326）。
 *
 * 两个关键点：应答必须带 MSG_TYPE=="reply"（发送据此换码为 SEND_REPLY_MESSAGE_V2(325)）；
 * REPLY_TO_CLIENT 是**请求方的 clientId**，所以请求方必须发过心跳注册为 producer。
 */

/** 对应 Java ``RequestCallback``。 */
interface RequestCallback
{
    public function onSuccess(?Message $responseMessage): void;

    public function onException(?\Throwable $cause): void;
}

/**
 * 对应 Java ``RequestResponseFuture``：一次 request 的等待槽。
 *
 * 与 Java 的差异（有意）：Java 额外起了一个 ``scanExpiredRequest`` 定时线程清理超时项；
 * 本实现由 ``request()`` 的 ``finally`` 保证移除，故不需要后台扫描线程。
 *
 * PHP 单线程适配：Python 的 ``threading.Event.wait`` 会阻塞到应答投递或超时。单线程里
 * 没有别的线程能在此调用内投递应答，因此 ``waitResponseMessage`` 只返回**当前**已到达的
 * 应答（可能为 null）——真实的"等待"由上层生产者驱动 remoting 层 pump 后再取。
 */
class RequestResponseFuture
{
    public const DEFAULT_REQUEST_TIMEOUT_MILLIS = 3000;

    public int $beginTimestamp;
    public ?Message $responseMsg = null;
    public bool $sendRequestOk = true;
    public ?\Throwable $cause = null;

    private bool $callbackFired = false;

    public function __construct(
        public string $correlationId,
        public int $timeoutMillis,
        public ?RequestCallback $requestCallback = null,
    ) {
        $this->beginTimestamp = (int) (microtime(true) * 1000.0);
    }

    /** 对应 Java ``waitResponseMessage``（单线程：返回当前应答，不阻塞）。 */
    public function waitResponseMessage(int $timeoutMillis): ?Message
    {
        return $this->responseMsg;
    }

    /** 对应 Java ``putResponseMessage``（会 countDown latch，允许多次调用）。 */
    public function putResponseMessage(?Message $responseMsg): void
    {
        $this->responseMsg = $responseMsg;
    }

    public function isTimeout(): bool
    {
        return (int) (microtime(true) * 1000.0) - $this->beginTimestamp > $this->timeoutMillis;
    }

    /** 对应 Java ``executeRequestCallback``：回调只允许触发一次。 */
    public function executeRequestCallback(): void
    {
        if ($this->requestCallback === null) {
            return;
        }
        if ($this->callbackFired) {
            return;
        }
        $this->callbackFired = true;

        if ($this->sendRequestOk && $this->cause === null) {
            $this->requestCallback->onSuccess($this->responseMsg);
        } else {
            $this->requestCallback->onException($this->cause);
        }
    }
}

/**
 * 对应 Java ``RequestFutureHolder``：correlationId → 等待槽 的全局表。
 *
 * Java 里是**跨 producer 共享的单例**（``RequestFutureHolder.getInstance()``），
 * 因为应答由 clientId 级别的 remoting 通道推回，与具体 producer 实例无关。
 */
class RequestFutureHolder
{
    /** @var array<string, RequestResponseFuture> */
    public array $requestFutureTable = [];

    private static ?self $instance = null;

    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function putRequest(string $correlationId, RequestResponseFuture $future): void
    {
        $this->requestFutureTable[$correlationId] = $future;
    }

    public function getRequest(string $correlationId): ?RequestResponseFuture
    {
        return $this->requestFutureTable[$correlationId] ?? null;
    }

    public function removeRequest(string $correlationId): ?RequestResponseFuture
    {
        $future = $this->requestFutureTable[$correlationId] ?? null;
        unset($this->requestFutureTable[$correlationId]);
        return $future;
    }

    /**
     * 接收侧入口（对齐 Java ``processReplyMessage``）：投递应答。
     *
     * Java 做的是 ``getRequestFutureTable().remove(correlationId)`` —— 用「谁摘到谁负责」
     * 保证「应答到达」与「超时清理」两条路径只会有一个生效。
     *
     * 返回被填充的 future；查不到（已超时/已移除）时返回 null。
     */
    public function putResponse(string $correlationId, ?Message $responseMsg): ?RequestResponseFuture
    {
        $future = $this->removeRequest($correlationId);
        if ($future === null) {
            return null;
        }
        $future->putResponseMessage($responseMsg);
        // 对齐 Java：成功路径也走 executeRequestCallback，让「只回调一次」的守卫生效
        $future->executeRequestCallback();
        return $future;
    }
}

/** 对应 Java ``CorrelationIdUtil``（随机 UUID 字符串）。 */
final class CorrelationIdUtil
{
    public static function createCorrelationId(): string
    {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40); // version 4
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80); // variant
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}

/**
 * 对应 Java ``MessageUtil``：由请求消息派生应答消息 / 判定是否为应答消息。
 */
final class MessageUtil
{
    /**
     * 对应 Java ``MessageUtil.createReplyMessage``。
     *
     * ``CLUSTER`` 属性由 broker 在投递时写入；拿不到就说明这条消息不是经 broker 转发的
     * （或 topic 配得不对），与 Java 一样直接抛错，而不是造一条投不出去的应答。
     *
     * Java 抛 ``MQClientException(CREATE_REPLY_MESSAGE_EXCEPTION, ...)``，应答方通常写在
     * 业务 listener 里，按 responseCode 分流才能把"这条请求不能回话"和别的本地错误分开。
     */
    public static function createReplyMessage(?Message $requestMessage, string $body): Message
    {
        if ($requestMessage === null) {
            throw new MQClientException(
                'create reply message fail, requestMessage cannot be null.',
                ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION
            );
        }
        if (!self::hasCluster($requestMessage)) {
            throw new MQClientException(
                sprintf(
                    'create reply message fail, requestMessage error, property[%s] is null.',
                    MessageConst::PROPERTY_CLUSTER
                ),
                ClientErrorCode::CREATE_REPLY_MESSAGE_EXCEPTION
            );
        }
        $cluster = (string) $requestMessage->getProperty(MessageConst::PROPERTY_CLUSTER);

        $reply = new Message();
        $reply->setTopic(MixAll::getReplyTopic($cluster));
        $reply->setBody($body);
        $reply->putProperty(MessageConst::PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
        self::copyProperty($requestMessage, $reply, MessageConst::PROPERTY_CORRELATION_ID);
        self::copyProperty($requestMessage, $reply, MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        self::copyProperty($requestMessage, $reply, MessageConst::PROPERTY_MESSAGE_TTL);
        return $reply;
    }

    /** MSG_TYPE == "reply" 的发送要走 SEND_REPLY_MESSAGE_V2(325)。 */
    public static function isReplyMessage(Message $msg): bool
    {
        return $msg->getProperty(MessageConst::PROPERTY_MESSAGE_TYPE) === MixAll::REPLY_MESSAGE_FLAG;
    }

    private static function hasCluster(Message $requestMessage): bool
    {
        $cluster = $requestMessage->getProperty(MessageConst::PROPERTY_CLUSTER);
        return $cluster !== null && $cluster !== '';
    }

    private static function copyProperty(Message $from, Message $to, string $key): void
    {
        // Java/Python 原样拷贝（可能为 null）；PHP 属性表为 array<string,string>，
        // null 无法落地，故缺省时跳过 —— 对 properties2String 的线格式等价（null 本就跳过）。
        $value = $from->getProperty($key);
        if ($value !== null) {
            $to->putProperty($key, $value);
        }
    }
}

/** Request-Reply 的若干便利函数（对应 Python 模块级 helper）。 */
final class RequestReply
{
    /**
     * （可选）把回调适配成 ``(response, cause)`` 形式，便于脚本使用。
     *
     * @param (callable(?Message, ?\Throwable): void)|null $callback
     */
    public static function reactCallback(?callable $callback, RequestResponseFuture $future): void
    {
        if ($callback === null) {
            return;
        }
        $callback($future->responseMsg, $future->cause);
    }
}

<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\ClientErrorCode;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\RequestTimeoutException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MixAll;

/**
 * Request-Reply（5.x）客户端侧支撑：请求等待槽、correlationId 全局表、
 * 应答消息派生与接收分发。
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

/** request 的异步回调接口。 */
interface RequestCallback
{
    public function onSuccess(?Message $responseMessage): void;

    public function onException(?\Throwable $cause): void;
}

/**
 * 一次 request 的等待槽。
 *
 * 超时清理（有意）：不设后台定时扫描线程；由 ``request()`` 的 ``finally`` 保证移除，
 * 兜底清扫走 {@see RequestFutureHolder::scanExpiredRequest()}。
 *
 * PHP 单线程适配：进程内没有别的线程能在此调用内投递应答，因此 ``waitResponseMessage``
 * 只返回**当前**已到达的应答（可能为 null）——真实的"等待"由上层生产者驱动 remoting 层
 * pump 后再取。
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

    /** 取应答（单线程：返回当前应答，不阻塞）。 */
    public function waitResponseMessage(int $timeoutMillis): ?Message
    {
        return $this->responseMsg;
    }

    /** 写入应答（允许多次调用，以最后一次为准）。 */
    public function putResponseMessage(?Message $responseMsg): void
    {
        $this->responseMsg = $responseMsg;
    }

    public function isTimeout(): bool
    {
        return (int) (microtime(true) * 1000.0) - $this->beginTimestamp > $this->timeoutMillis;
    }

    /** 记录失败原因。 */
    public function setCause(?\Throwable $cause): void
    {
        $this->cause = $cause;
    }

    /** 触发回调：只允许触发一次。 */
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

    public function putResponse(string $correlationId, ?Message $responseMsg): ?RequestResponseFuture
    {
        $future = $this->removeRequest($correlationId);
        if ($future === null) {
            return null;
        }
        $future->putResponseMessage($responseMsg);
        $future->executeRequestCallback();
        return $future;
    }

    public function scanExpiredRequest(): int
    {
        $expired = [];
        foreach ($this->requestFutureTable as $correlationId => $future) {
            if ($future->isTimeout()) {
                $expired[$correlationId] = $future;
                unset($this->requestFutureTable[$correlationId]);
            }
        }
        foreach ($expired as $future) {
            Logger::warning(sprintf('remove timeout request, CorrelationId=%s', $future->correlationId));
            try {
                $future->setCause(new RequestTimeoutException(
                    'request timeout, no reply message.',
                    ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION,
                ));
                $future->executeRequestCallback();
            } catch (\Throwable $e) {
                Logger::warning('scanExpiredRequest operationCallback exception: ' . $e->getMessage());
            }
        }
        return count($expired);
    }
}

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

final class MessageUtil
{
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

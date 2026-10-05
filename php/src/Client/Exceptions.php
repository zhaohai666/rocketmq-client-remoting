<?php

declare(strict_types=1);

namespace RocketMQ\Client\Exceptions;

/**
 * 客户端异常体系（对应 org.apache.rocketmq.client.exception.* 与 remoting exception，
 * 按 php/PORTING.md 约定集中在 src/Client/Exceptions.php 一个文件多类）。
 *
 * 命名空间取 `RocketMQ\Client\Exceptions`：与文件名 `Exceptions.php` 对应，且被
 * bootstrap.php 的 classmap fallback（token 扫描）自动加载，无需调用方 require。
 */

/**
 * 对应 org.apache.rocketmq.client.common.ClientErrorCode（七个常量，逐个齐全）。
 *
 * sendDefaultImpl 重试耗尽后用它给最终的 MQClientException 定性：
 * 连不上 broker→10001，等响应超时→10002，客户端自身问题→10003，
 * 地址服务器没给地址→10004，路由查不到→10005。
 * 另两个不在重试定性里，各有各的抛出点：
 * * 10006 —— request-reply 等到点没等到应答（``DefaultMQProducerImpl#waitResponse``，
 *   以及 Java ``RequestFutureHolder#scanExpiredRequest`` 的清理路径）。
 * * 10007 —— 由请求消息造应答消息失败（``MessageUtil#createReplyMessage``）。
 */
final class ClientErrorCode
{
    public const CONNECT_BROKER_EXCEPTION = 10001;
    public const ACCESS_BROKER_TIMEOUT = 10002;
    public const BROKER_NOT_EXIST_EXCEPTION = 10003;
    public const NO_NAME_SERVER_EXCEPTION = 10004;
    public const NOT_FOUND_TOPIC_EXCEPTION = 10005;
    public const REQUEST_TIMEOUT_EXCEPTION = 10006;
    public const CREATE_REPLY_MESSAGE_EXCEPTION = 10007;
}

/**
 * 对应 MQException 基类（RuntimeException）。response_code 非 0 时为 broker 侧错误码。
 */
class MQException extends \RuntimeException
{
    public function __construct(
        string $message = '',
        public readonly ?int $responseCode = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }
}

/** 对应 MQClientException。response_code 非 0 时为 broker 侧错误码。 */
class MQClientException extends MQException
{
}

/** 对应 MQBrokerException：broker 返回非 SUCCESS 码。 */
class MQBrokerException extends MQException
{
    public function __construct(int $responseCode, string $errorMessage = '', ?\Throwable $previous = null)
    {
        parent::__construct($errorMessage, $responseCode, $previous);
    }
}

class MQTimeOutException extends MQClientException
{
}

class MQQueueException extends MQClientException
{
}

/**
 * 对应 Java ``RequestTimeoutException extends MQClientException``。
 *
 * Request-Reply 专用语义：请求消息**已经发成功**，但在超时窗口内没等到应答。
 * 与「发送本身失败」（会抛 MQClientException / RemotingException）区分开 ——
 * 前者可能只是应答方没回，消息其实已经投递；后者连 broker 都没收到。
 */
class RequestTimeoutException extends MQClientException
{
}

// ---------------- Remoting 异常（对应 org.apache.rocketmq.remoting.exception）----------------

class RemotingException extends MQException
{
}

class RemotingCommandException extends RemotingException
{
}

class RemotingConnectException extends RemotingException
{
    public function __construct(string $addr = '', ?\Throwable $previous = null)
    {
        parent::__construct(sprintf('connect to %s failed', $addr), null, $previous);
        $this->addr = $addr;
    }

    public string $addr = '';
}

class RemotingSendRequestException extends RemotingException
{
    public string $addr = '';

    public function __construct(string $addr = '', string $msg = '', ?\Throwable $previous = null)
    {
        parent::__construct(sprintf('send request to %s failed: %s', $addr, $msg), null, $previous);
        $this->addr = $addr;
    }
}

class RemotingTimeoutException extends RemotingException
{
    public string $addr = '';
    public int $timeoutMillis = 0;

    public function __construct(string $addr = '', int $timeoutMillis = 0, string $msg = '', ?\Throwable $previous = null)
    {
        parent::__construct(sprintf('wait response on the channel %s timeout, %dms: %s', $addr, $timeoutMillis, $msg), null, $previous);
        $this->addr = $addr;
        $this->timeoutMillis = $timeoutMillis;
    }
}

class RemotingTooMuchRequestException extends RemotingException
{
}

class RemotingNoCodecException extends RemotingException
{
}

class RemotingServerException extends RemotingException
{
}

/** 对应 Java UnsupportedOperationException（客户端层的"不支持"信号）。 */
class UnsupportedOperationException extends MQException
{
}

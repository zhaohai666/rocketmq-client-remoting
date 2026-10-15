<?php

declare(strict_types=1);

namespace RocketMQ\Client\Exceptions;


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
 * Corresponds to the MQException base class (RuntimeException). A non-zero response_code
 * is a broker-side error code.
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

/** Corresponds to MQClientException. A non-zero response_code is a broker-side error code. */
class MQClientException extends MQException
{
}

/** Corresponds to MQBrokerException: the broker returned a non-SUCCESS code. */
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

class RequestTimeoutException extends MQClientException
{
}


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

class UnsupportedOperationException extends MQException
{
}

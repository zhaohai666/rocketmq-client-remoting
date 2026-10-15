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
 * Request-Reply (5.x) client-side support: request wait slots, correlationId global table,
 * reply message derivation and receive dispatch.
 *
 * Protocol recap:
 *   Requester (producer.request): msg.properties[CORRELATION_ID]=uuid /
 *   [REPLY_TO_CLIENT]=clientId / [TTL]=timeoutMillis; the responder's createReplyMessage
 *   derives a message with topic=<CLUSTER>_REPLY_TOPIC, MSG_TYPE="reply", and the broker
 *   finds the requester connection by REPLY_TO_CLIENT and pushes the reply back
 *   (PUSH_REPLY_MESSAGE_TO_CLIENT 326).
 *
 * Two key points: the reply must carry MSG_TYPE=="reply" (sending then swaps the code to
 * SEND_REPLY_MESSAGE_V2(325)); REPLY_TO_CLIENT is the **requester's clientId**, so the
 * requester must have sent a heartbeat registered as a producer.
 */

/** The async callback interface for a request. */
interface RequestCallback
{
    public function onSuccess(?Message $responseMessage): void;

    public function onException(?\Throwable $cause): void;
}

/**
 * A wait slot for one request.
 *
 * Timeout cleanup (deliberate): no background periodic scan thread; removal is guaranteed
 * by ``request()``'s ``finally``, with a fallback sweep via
 * {@see RequestFutureHolder::scanExpiredRequest()}.
 *
 * PHP single-threaded adaptation: no other thread in the process can deliver a reply
 * during this call, so ``waitResponseMessage`` only returns the reply **already** arrived
 * (possibly null) -- the real "waiting" is driven by the upper-layer producer pumping the
 * remoting layer before fetching again.
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

    /** Take the reply (single-threaded: returns the current reply, non-blocking). */
    public function waitResponseMessage(int $timeoutMillis): ?Message
    {
        return $this->responseMsg;
    }

    /** Write the reply (multiple calls allowed; the last one wins). */
    public function putResponseMessage(?Message $responseMsg): void
    {
        $this->responseMsg = $responseMsg;
    }

    public function isTimeout(): bool
    {
        return (int) (microtime(true) * 1000.0) - $this->beginTimestamp > $this->timeoutMillis;
    }

    /** Record the failure cause. */
    public function setCause(?\Throwable $cause): void
    {
        $this->cause = $cause;
    }

    /** Fire the callback: allowed to fire only once. */
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

    /** Sending with MSG_TYPE == "reply" must go through SEND_REPLY_MESSAGE_V2(325). */
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

/** A few convenience functions for Request-Reply (module-level helpers). */
final class RequestReply
{
    /**
     * (Optional) Adapt the callback into the ``(response, cause)`` form for script use.
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

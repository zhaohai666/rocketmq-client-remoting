<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MessageType;


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

interface SendMessageHook
{
    public function hookName(): string;

    public function sendMessageBefore(SendMessageContext $context): void;

    public function sendMessageAfter(SendMessageContext $context): void;
}

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

interface ConsumeMessageHook
{
    public function hookName(): string;

    public function consumeMessageBefore(ConsumeMessageContext $context): void;

    public function consumeMessageAfter(ConsumeMessageContext $context): void;
}

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

interface EndTransactionHook
{
    public function hookName(): string;

    public function endTransaction(EndTransactionContext $context): void;
}

final class CommunicationMode
{
    public const SYNC = 'SYNC';
    public const ASYNC = 'ASYNC';
    public const ONEWAY = 'ONEWAY';
}

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

interface CheckForbiddenHook
{
    public function hookName(): string;

    public function checkForbidden(CheckForbiddenContext $context): void;
}

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

interface FilterMessageHook
{
    public function hookName(): string;

    public function filterMessage(FilterMessageContext $context): void;
}

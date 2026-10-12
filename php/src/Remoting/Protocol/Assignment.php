<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;

enum MessageRequestMode: string
{
    case PULL = 'PULL';
    case POP = 'POP';
}

/**
 * broker 下发的单个指配项：队列 + 期望请求模式（PULL/POP）+ attachments。
 */
final class MessageQueueAssignment
{
    public ?MessageQueue $messageQueue = null;
    public MessageRequestMode $mode = MessageRequestMode::PULL;
    /** @var array<string, string> */
    public array $attachments = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $mq = $this->messageQueue;
        return [
            'messageQueue' => $mq === null ? null : [
                'topic' => $mq->topic,
                'brokerName' => $mq->brokerName,
                'queueId' => $mq->queueId,
            ],
            'mode' => $this->mode->value,
            'attachments' => $this->attachments === [] ? new \stdClass() : $this->attachments,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $a = new self();
        $mq = $d['messageQueue'] ?? null;
        if (is_array($mq)) {
            $a->messageQueue = new MessageQueue(
                (string) ($mq['topic'] ?? ''),
                (string) ($mq['brokerName'] ?? ''),
                (int) ($mq['queueId'] ?? 0),
            );
        }
        $mode = $d['mode'] ?? null;
        $a->mode = is_string($mode)
            ? (MessageRequestMode::tryFrom($mode) ?? MessageRequestMode::PULL)
            : MessageRequestMode::PULL;
        foreach ((array) ($d['attachments'] ?? []) as $k => $v) {
            $a->attachments[(string) $k] = (string) $v;
        }
        return $a;
    }
}

final class QueryAssignmentRequestBody
{
    public string $topic = '';
    public string $consumerGroup = '';
    public string $clientId = '';
    /** MessageModel（CLUSTERING / BROADCASTING），线格式是枚举 name。 */
    public string $messageModel = 'CLUSTERING';
    public string $strategyName = '';

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'topic' => $this->topic,
            'consumerGroup' => $this->consumerGroup,
            'clientId' => $this->clientId,
            'messageModel' => $this->messageModel,
            'strategyName' => $this->strategyName,
        ];
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(?string $data): self
    {
        $b = new self();
        $obj = ($data === null || $data === '') ? null : RemotingSerializable::fastjsonLoads($data);
        if (!is_array($obj)) {
            return $b;
        }
        $b->topic = (string) ($obj['topic'] ?? '');
        $b->consumerGroup = (string) ($obj['consumerGroup'] ?? '');
        $b->clientId = (string) ($obj['clientId'] ?? '');
        $b->messageModel = (string) ($obj['messageModel'] ?? 'CLUSTERING');
        $b->strategyName = (string) ($obj['strategyName'] ?? '');
        return $b;
    }
}

final class QueryAssignmentResponseBody
{
    public ?array $messageQueueAssignments = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        if ($this->messageQueueAssignments === null) {
            return [];
        }
        return [
            'messageQueueAssignments' => array_map(
                static fn (MessageQueueAssignment $a): array => $a->toDict(),
                $this->messageQueueAssignments,
            ),
        ];
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        if (!array_key_exists('messageQueueAssignments', $d) || $d['messageQueueAssignments'] === null) {
            return $b;
        }
        $out = [];
        foreach ((array) $d['messageQueueAssignments'] as $one) {
            if (!is_array($one)) {
                continue;
            }
            $out[] = MessageQueueAssignment::fromDict($one);
        }
        $b->messageQueueAssignments = $out;
        return $b;
    }

    public static function decode(?string $data): self
    {
        if ($data === null || $data === '') {
            return new self();
        }
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;

/**
 * POP broker 指配（QUERY_ASSIGNMENT=400）线格式：
 * QueryAssignmentRequestBody / QueryAssignmentResponseBody / MessageQueueAssignment。
 *
 * 用途：push 消费者 ``clientRebalance=false``（opt-in）时，rebalance 不在本地按
 * 分配策略算队列，而是把 QUERY_ASSIGNMENT(400) 发给 broker，**完全服从**返回的
 * (MessageQueue, MessageRequestMode) 列表——队列归属由 broker 单方决定。
 */
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
        // 回包没有 mode 时按 PULL 处理；有值时大小写保持线格式原样
        $a->mode = $mode === null ? MessageRequestMode::PULL : MessageRequestMode::from((string) $mode);
        foreach ((array) ($d['attachments'] ?? []) as $k => $v) {
            $a->attachments[(string) $k] = (string) $v;
        }
        return $a;
    }
}

/**
 * QUERY_ASSIGNMENT 请求体：上报本组订阅与期望策略，向 broker 索取指配结果。
 */
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
}

/**
 * QUERY_ASSIGNMENT 响应体：broker 下发的指配列表。
 */
final class QueryAssignmentResponseBody
{
    /** @var list<MessageQueueAssignment> */
    public array $messageQueueAssignments = [];

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        foreach ((array) ($d['messageQueueAssignments'] ?? []) as $one) {
            if (!is_array($one)) {
                continue;
            }
            $b->messageQueueAssignments[] = MessageQueueAssignment::fromDict($one);
        }
        return $b;
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

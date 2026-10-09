<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;

/**
 * POP broker 指配（QUERY_ASSIGNMENT=400）线格式，对应
 * org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody /
 * QueryAssignmentResponseBody 与 org.apache.rocketmq.common.message.MessageQueueAssignment。
 *
 * 用途：push 消费者 ``clientRebalance=false``（opt-in）时，rebalance 不在本地按
 * 分配策略算队列，而是把 QUERY_ASSIGNMENT(400) 发给 broker，**完全服从**返回的
 * (MessageQueue, MessageRequestMode) 列表（Java RebalanceImpl.getRebalanceResultFromBroker）。
 */
enum MessageRequestMode: string
{
    case PULL = 'PULL';
    case POP = 'POP';
}

/**
 * 对应 org.apache.rocketmq.common.message.MessageQueueAssignment。
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
        // broker 侧缺省 PULL（Java MessageRequestMode.PULL），大小写按线格式原样
        $a->mode = $mode === null ? MessageRequestMode::PULL : MessageRequestMode::from((string) $mode);
        foreach ((array) ($d['attachments'] ?? []) as $k => $v) {
            $a->attachments[(string) $k] = (string) $v;
        }
        return $a;
    }
}

/**
 * 对应 org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody。
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
 * 对应 org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody。
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

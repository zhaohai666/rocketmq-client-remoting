<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;

/**
 * POP broker 指配（QUERY_ASSIGNMENT=400）线格式：
 * QueryAssignmentRequestBody / QueryAssignmentResponseBody / MessageQueueAssignment。
 *
 * 用途：push 消费者 POP 路径 ``clientRebalance=false``（opt-in）时，rebalance 不在本地按
 * 分配策略算队列，而是把 QUERY_ASSIGNMENT(400) 发给 broker，**完全服从**返回的
 * (MessageQueue, MessageRequestMode) 列表（Java RebalanceImpl#getRebalanceResultFromBroker:345）。
 * 驱动方是 MQClientInstance::tick() 的分配轮（PHP 单线程：Java 的 RebalanceService
 * 20s 调度在这里收敛成到点检查）。
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
        // broker 侧缺省 PULL（Java MessageRequestMode.PULL），大小写按线格式原样。
        // 认不出的取值同样回落 PULL：Java 的 fastjson 在枚举反序列化上会抛异常、整条
        // 应答作废（调用方因此保留上一次分配）；本端口径与其余 body 一致，逐字段容错，
        // mode 只用于记账，队列集照单全收。
        $a->mode = is_string($mode)
            ? (MessageRequestMode::tryFrom($mode) ?? MessageRequestMode::PULL)
            : MessageRequestMode::PULL;
        foreach ((array) ($d['attachments'] ?? []) as $k => $v) {
            $a->attachments[(string) $k] = (string) $v;
        }
        return $a;
    }
}

/**
 * 对应 org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody。
 *
 * ⚠ wire 口径：400 和 401 一样**没有请求头**（Java MQClientAPIImpl#queryAssignment:416
 * 传的是 `createRequestCommand(QUERY_ASSIGNMENT, null)`），全部字段都进 body JSON，
 * broker 按 Java 属性名解 —— 下面的键名一个都不能改。
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

    /**
     * 解一个 400 请求体（假 broker / 服务端视角；线上缺字段一律按 Java 属性缺省值）。
     */
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

/**
 * 对应 org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody。
 *
 * **null / 空集是两种语义**（Java QueryAssignmentProcessor#doLoadBalance:148-151 的
 * javadoc，消费方是 RebalanceImpl#getRebalanceResultFromBroker:356-359）：
 *
 * - 键缺失 / 显式 null（本端口回 `null`）= "invalid result"，客户端**跳过更新**、
 *   保留上一次分配（真实 broker 对不存在的 topic、取不到 cid 列表、策略名不认识都走这条）；
 * - 空数组（本端口回 `[]`）= "clear all load assigned to it before"，该 topic 的队列
 *   全部撤销。
 *
 * 两者都**不能**退化成"这个 topic 的队列我全要"。
 */
final class QueryAssignmentResponseBody
{
    /** @var list<MessageQueueAssignment>|null null = broker 没给指配集（Java 的 null） */
    public ?array $messageQueueAssignments = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        if ($this->messageQueueAssignments === null) {
            // fastjson 对 null 字段不落键（Java RemotingSerializable.encode 同语义），
            // 解回来命中"键缺失"分支 = 无效结果。
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
            // Java 的 null：不是"没队列"，是"这次答复无效"。
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

    /** 空 body / 非对象 body 一律按 Java 的 null 结果处理（调用方保留上一次分配）。 */
    public static function decode(?string $data): self
    {
        if ($data === null || $data === '') {
            return new self();
        }
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

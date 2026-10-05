<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\SubscriptionData;

/**
 * 心跳数据（对应 org.apache.rocketmq.remoting.protocol.heartbeat.*，移植自 protocol/heartbeat.py）。
 */

/** 对应 Java ConsumeType 枚举名。 */
final class ConsumeType
{
    public const CONSUME_ACTIVELY = 'CONSUME_ACTIVELY';
    public const CONSUME_PASSIVELY = 'CONSUME_PASSIVELY';
}

/** 对应 Java MessageModel 枚举名。 */
final class MessageModel
{
    public const CLUSTERING = 'CLUSTERING';
    public const BROADCASTING = 'BROADCASTING';
}

/** 对应 Java ConsumeFromWhere 枚举名。 */
final class ConsumeFromWhere
{
    public const CONSUME_FROM_LAST_OFFSET = 'CONSUME_FROM_LAST_OFFSET';
    public const CONSUME_FROM_FIRST_OFFSET = 'CONSUME_FROM_FIRST_OFFSET';
    public const CONSUME_FROM_TIMESTAMP = 'CONSUME_FROM_TIMESTAMP';
}

final class ProducerData
{
    public function __construct(public string $groupName = '')
    {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['groupName' => $this->groupName];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self((string)($d['groupName'] ?? ''));
    }
}

final class ConsumerData
{
    /** @var list<SubscriptionData> 对应 Python set（无重复）；JSON 输出顺序按插入序 */
    public array $subscriptionDataSet = [];
    public bool $unitMode = false;

    public function __construct(
        public string $groupName = '',
        public string $consumeType = ConsumeType::CONSUME_PASSIVELY,
        public string $messageModel = MessageModel::CLUSTERING,
        public string $consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
    ) {
    }

    /**
     * 对齐 Java 5.x ConsumerData 字段（**没有** 4.x 的 consumeTimestamp /
     * maxReconsumeTimes）；subscriptionDataSet 用 SubscriptionData.toDict()。
     *
     * @return array<string, mixed>
     */
    public function toDict(): array
    {
        return [
            'groupName' => $this->groupName,
            'consumeType' => $this->consumeType,
            'messageModel' => $this->messageModel,
            'consumeFromWhere' => $this->consumeFromWhere,
            'subscriptionDataSet' => array_map(static fn(SubscriptionData $s): array => $s->toDict(), $this->subscriptionDataSet),
            'unitMode' => $this->unitMode,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $cd = new self(
            (string)($d['groupName'] ?? ''),
            (string)($d['consumeType'] ?? ConsumeType::CONSUME_PASSIVELY),
            (string)($d['messageModel'] ?? MessageModel::CLUSTERING),
            (string)($d['consumeFromWhere'] ?? ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET),
        );
        $cd->unitMode = (bool)($d['unitMode'] ?? false);
        foreach ($d['subscriptionDataSet'] ?? [] as $sd) {
            $sub = new SubscriptionData($sd['topic'] ?? null, $sd['subString'] ?? null);
            $sub->subVersion = (int)($sd['subVersion'] ?? 0);
            $sub->expressionType = (string)($sd['expressionType'] ?? 'TAG');
            $sub->classFilterMode = (bool)($sd['classFilterMode'] ?? false);
            $sub->tagsSet = array_values((array)($sd['tagsSet'] ?? []));
            $sub->codeSet = array_map(intval(...), array_values((array)($sd['codeSet'] ?? [])));
            $cd->subscriptionDataSet[] = $sub;
        }
        return $cd;
    }
}

final class HeartbeatData
{
    /** @var list<ProducerData> */
    public array $producerDataSet = [];
    /** @var list<ConsumerData> */
    public array $consumerDataSet = [];

    public function __construct(public string $clientId = '')
    {
    }

    /**
     * heartbeatFingerprint 故意留 0：broker 见到 0 走 V1 注册路径（用完整
     * subscriptionDataSet 注册），最稳妥；非 0 才会进 heartBeatV2 优化。
     * withoutSub 同理（Java 字段 isWithoutSub，fastjson2 名是 withoutSub）。
     *
     * @return array<string, mixed>
     */
    public function toDict(): array
    {
        return [
            'clientID' => $this->clientId,
            'producerDataSet' => array_map(static fn(ProducerData $p): array => $p->toDict(), $this->producerDataSet),
            'consumerDataSet' => array_map(static fn(ConsumerData $c): array => $c->toDict(), $this->consumerDataSet),
            'heartbeatFingerprint' => 0,
            'withoutSub' => false,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $hb = new self((string)($d['clientID'] ?? ''));
        foreach ($d['producerDataSet'] ?? [] as $p) {
            $hb->producerDataSet[] = ProducerData::fromDict($p);
        }
        foreach ($d['consumerDataSet'] ?? [] as $c) {
            $hb->consumerDataSet[] = ConsumerData::fromDict($c);
        }
        return $hb;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::decodeJson($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

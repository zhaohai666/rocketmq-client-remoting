<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\SubscriptionData;

/**
 * Heartbeat data: the wire format with which the client registers producers / consumers and their
 * subscription sets with the broker.
 */

/** The consume type (the enum name of the protocol field consumeType). */
final class ConsumeType
{
    public const CONSUME_ACTIVELY = 'CONSUME_ACTIVELY';
    public const CONSUME_PASSIVELY = 'CONSUME_PASSIVELY';
}

/** The message model (the enum name of the protocol field messageModel). */
final class MessageModel
{
    public const CLUSTERING = 'CLUSTERING';
    public const BROADCASTING = 'BROADCASTING';
}

/** The starting consumption position (the enum name of the protocol field consumeFromWhere). */
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
    /** @var list<SubscriptionData> set semantics: one entry per topic, filled in by the caller after sorting; the JSON output order is the insertion order */
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
     * Only emits the ConsumerData fields that a 5.x broker recognizes (**without** the 4.x
     * consumeTimestamp / maxReconsumeTimes); subscriptionDataSet uses SubscriptionData.toDict().
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
     * heartbeatFingerprint is deliberately left at 0: when the broker sees 0 it takes the V1 registration
     * path (registering with the complete subscriptionDataSet), which is the safest; only a non-zero value
     * enters the heartBeatV2 optimization.
     * withoutSub for the same reason: the boolean field name in the protocol really is withoutSub, and
     * false means the subscription set is reported as usual.
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

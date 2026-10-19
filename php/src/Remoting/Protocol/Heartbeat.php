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
    /**
     * Fingerprint of the subscription set this beat reports. 0 (the default) is what makes the
     * broker take its legacy registration path with the complete subscriptionDataSet; only a
     * non-zero value lets it answer the fingerprint contract at all.
     */
    public int $heartbeatFingerprint = 0;
    /// True when this body deliberately omits the subscription sets. The protocol field name is
    /// `withoutSub`; false means the subscriptions are reported as usual.
    public bool $withoutSub = false;

    public function __construct(public string $clientId = '')
    {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'clientID' => $this->clientId,
            'producerDataSet' => array_map(static fn(ProducerData $p): array => $p->toDict(), $this->producerDataSet),
            'consumerDataSet' => array_map(static fn(ConsumerData $c): array => $c->toDict(), $this->consumerDataSet),
            'heartbeatFingerprint' => $this->heartbeatFingerprint,
            'withoutSub' => $this->withoutSub,
        ];
    }

    /**
     * The normalized content the fingerprint is taken over: everything that legitimately churns
     * between two beats is blanked out -- every `subVersion` (a wall-clock timestamp), the client
     * id, the `withoutSub` marker and the fingerprint field itself. Two beats that report the
     * same groups and the same subscriptions therefore hash equal no matter which facade sent
     * them, which is exactly the question the broker answers with IS_SUB_CHANGE.
     *
     * @return array<string, mixed>
     */
    public function fingerprintPayload(): array
    {
        $consumers = [];
        foreach ($this->consumerDataSet as $cd) {
            $dict = $cd->toDict();
            $subs = [];
            foreach ($dict['subscriptionDataSet'] ?? [] as $sd) {
                $sd['subVersion'] = 0;
                $subs[] = self::sortedKeys($sd);
            }
            $dict['subscriptionDataSet'] = $subs;
            $consumers[] = self::sortedKeys($dict);
        }
        return self::sortedKeys([
            'clientID' => '',
            'producerDataSet' => array_map(
                static fn(ProducerData $p): array => self::sortedKeys($p->toDict()),
                $this->producerDataSet,
            ),
            'consumerDataSet' => $consumers,
            'heartbeatFingerprint' => 0,
            'withoutSub' => false,
        ]);
    }

    /**
     * A 32-bit hash of the subscription content. Only ever compared with **its own history** (the
     * broker keeps group -> fingerprint for this channel, the client keeps broker address ->
     * fingerprint), so the value has to be stable inside this process, not equal to what another
     * client would produce.
     *
     * Always compute it on the **with-subscription** body: one value per round is stamped onto
     * both bodies, so the subscription-free variant's own recomputation (its sets are empty) is
     * meaningless by construction.
     */
    public function computeFingerprint(): int
    {
        return \RocketMQ\Common\UtilAll::wireStringHash(
            json_encode($this->fingerprintPayload(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)
        );
    }

    /**
     * The cheap body of a fingerprint round: same client, same producer and consumer groups,
     * **empty** subscription sets and `withoutSub = true`, carrying the fingerprint of the full
     * body it was derived from. The broker re-registers the groups against what it already holds
     * instead of re-parsing the subscriptions.
     */
    public function withoutSubCopy(): self
    {
        $copy = new self($this->clientId);
        $copy->producerDataSet = array_map(
            static fn(ProducerData $p): ProducerData => new ProducerData($p->groupName),
            $this->producerDataSet,
        );
        foreach ($this->consumerDataSet as $cd) {
            $light = new ConsumerData(
                $cd->groupName,
                $cd->consumeType,
                $cd->messageModel,
                $cd->consumeFromWhere,
            );
            $light->unitMode = $cd->unitMode;
            $copy->consumerDataSet[] = $light;
        }
        $copy->heartbeatFingerprint = $this->heartbeatFingerprint;
        $copy->withoutSub = true;
        return $copy;
    }

    /** @param array<string, mixed> $a */
    private static function sortedKeys(array $a): array
    {
        ksort($a);
        foreach ($a as $k => $v) {
            if (is_array($v)) {
                $a[$k] = self::sortedKeys($v);
            }
        }
        return $a;
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
        $hb->heartbeatFingerprint = (int)($d['heartbeatFingerprint'] ?? 0);
        $hb->withoutSub = (bool)($d['withoutSub'] ?? false);
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

/**
 * What one fingerprint heartbeat answers.
 *
 * A broker that predates the two body fields answers HEART_BEAT(34) with plain SUCCESS and **no**
 * ext fields, which reads as `supportV2 = false` / `subChange = false`: the client then keeps
 * sending that address the full subscription set, so the pre-existing registration path is never
 * disturbed.
 */
final class HeartbeatV2Result
{
    public function __construct(
        /// The broker's protocol version, read off the response header like the plain path does.
        public int $version = 0,
        public bool $subChange = false,
        public bool $supportV2 = false,
    ) {
    }

    /** @param array<string, mixed> $extFields */
    public static function fromResponse(int $version, array $extFields): self
    {
        // Lenient parse, the way the field is defined: anything but "true" (case-insensitive) is
        // false, and a missing key is false too.
        $isTrue = static fn(string $key): bool =>
            strtolower(trim((string)($extFields[$key] ?? ''))) === 'true';
        return new self(
            $version,
            $isTrue(\RocketMQ\Common\MixAll::IS_SUB_CHANGE),
            $isTrue(\RocketMQ\Common\MixAll::IS_SUPPORT_HEART_BEAT_V2),
        );
    }
}

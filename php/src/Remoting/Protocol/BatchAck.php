<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * The body of BATCH_ACK_MESSAGE (200151): the acknowledge bit set, one merged entry and the
 * whole request body.
 *
 * A batch acknowledge carries **no custom header**: the consumer group, topic, queue, checkpoint
 * offset, revive queue, pop time and invisible time all travel inside the JSON body entries, so the
 * only thing the header can add is the broker name the body already names.
 */

/**
 * The bit set carried by the ``b`` field of a batch acknowledge entry.
 *
 * Byte layout is the one the broker reads back as a bit set of absolute positions: **little-endian**,
 * so bit index ``i`` lives at bit ``i % 8`` of byte ``i / 8``, and the byte array is
 * **BYTE-MINIMAL**: exactly ``(highestSetBit / 8) + 1`` bytes, i.e. trailing zero bytes are dropped.
 * Padding up to a whole word changes the base64 the broker receives, so it is not allowed:
 *  - ``{0, 3}`` -> one byte ``0x09`` -> ``"CQ=="``
 *  - ``{0}``    -> one byte ``0x01`` -> ``"AQ=="``
 *  - ``{8}``    -> two bytes ``0x00 0x01`` -> ``"AAE="``
 *  - the empty set -> no bytes at all -> ``""``
 */
final class BatchAckBitSet
{
    /** @var array<int, true> the set bit indexes */
    private array $bits = [];

    /** @param iterable<int> $bits */
    public function __construct(iterable $bits = [])
    {
        foreach ($bits as $index) {
            $this->set((int)$index);
        }
    }

    /** Marks bit ``$index``; a negative index is a client-side bug, never data. */
    public function set(int $index): void
    {
        if ($index < 0) {
            throw new \InvalidArgumentException('bitIndex < 0: ' . $index);
        }
        $this->bits[$index] = true;
    }

    public function isSet(int $index): bool
    {
        return isset($this->bits[$index]);
    }

    public function isEmpty(): bool
    {
        return $this->bits === [];
    }

    /** @return list<int> every set index, ascending */
    public function setIndexes(): array
    {
        $out = array_map(intval(...), array_keys($this->bits));
        sort($out, SORT_NUMERIC);
        return $out;
    }

    /** The byte-minimal little-endian byte array; the empty set yields an empty string. */
    public function toBytes(): string
    {
        if ($this->bits === []) {
            return '';
        }
        // Byte-minimal: the length is decided by the highest set bit alone, never rounded up to a word.
        $bytes = str_repeat("\0", intdiv(max(array_keys($this->bits)), 8) + 1);
        foreach ($this->bits as $index => $_) {
            $at = intdiv($index, 8);
            $bytes[$at] = chr(ord($bytes[$at]) | (1 << ($index % 8)));
        }
        return $bytes;
    }

    /** The wire form: standard base64 of the byte array; the empty set is the empty string. */
    public function toBase64(): string
    {
        $bytes = $this->toBytes();
        return $bytes === '' ? '' : base64_encode($bytes);
    }

    /** The inverse of toBytes(): every byte position contributes eight bit indexes. */
    public static function fromBytes(string $bytes): self
    {
        $set = new self();
        $length = strlen($bytes);
        for ($byteIndex = 0; $byteIndex < $length; $byteIndex++) {
            $byte = ord($bytes[$byteIndex]);
            if ($byte === 0) {
                continue;
            }
            for ($bitInByte = 0; $bitInByte < 8; $bitInByte++) {
                if (($byte & (1 << $bitInByte)) !== 0) {
                    $set->set($byteIndex * 8 + $bitInByte);
                }
            }
        }
        return $set;
    }

    /** Reads the ``b`` field back; an absent or empty value is the empty set. */
    public static function fromBase64(?string $encoded): self
    {
        if ($encoded === null || $encoded === '') {
            return new self();
        }
        $decoded = base64_decode($encoded, true);
        return self::fromBytes($decoded === false ? '' : $decoded);
    }
}

/**
 * One entry of the batch acknowledge body: every acknowledged offset of a single pop of a single
 * queue.
 *
 * ``startOffset`` is the checkpoint offset of that pop and the bit set marks the acknowledged offsets
 * **relative to it**: bit ``i`` stands for queue offset ``startOffset + i``, which the broker expands
 * back into absolute offsets.
 *
 * The JSON keys are the compact family ``c t r so q rq pt it b``. ``r`` is a **String** on the wire
 * (``"0"``, ``"1"``, ``"2"``) even though it only ever holds a digit: writing it as a number makes the
 * broker-side reader reject the entry. On the reading side the long names are also accepted, because a
 * broker-side tool may emit those instead.
 */
final class BatchAck
{
    /** compact wire key => the long name a reader must also accept */
    private const KEY_ALIASES = [
        'c' => 'consumerGroup',
        't' => 'topic',
        'r' => 'retry',
        'so' => 'startOffset',
        'q' => 'queueId',
        'rq' => 'reviveQueueId',
        'pt' => 'popTime',
        'it' => 'invisibleTime',
        'b' => 'bitSet',
    ];

    public ?string $consumerGroup = null;
    public ?string $topic = null;
    /** "1"/"2" when the message was revived from a retry topic; travels as a String. */
    public string $retry = ExtraInfoUtil::NORMAL_TOPIC;
    public int $startOffset = 0;
    public int $queueId = 0;
    public int $reviveQueueId = 0;
    public int $popTime = 0;
    public int $invisibleTime = 0;
    public BatchAckBitSet $bitSet;

    public function __construct()
    {
        $this->bitSet = new BatchAckBitSet();
    }

    /** The absolute queue offsets this entry acknowledges: checkpoint offset plus each bit index. */
    public function ackedOffsets(): array
    {
        return array_map(
            fn(int $index): int => $this->startOffset + $index,
            $this->bitSet->setIndexes(),
        );
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'c' => $this->consumerGroup,
            't' => $this->topic,
            'r' => $this->retry,
            'so' => $this->startOffset,
            'q' => $this->queueId,
            'rq' => $this->reviveQueueId,
            'pt' => $this->popTime,
            'it' => $this->invisibleTime,
            'b' => $this->bitSet->toBase64(),
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $ack = new self();
        $ack->consumerGroup = self::pickString($d, 'c');
        $ack->topic = self::pickString($d, 't');
        $retry = self::pick($d, 'r');
        $ack->retry = $retry === null ? ExtraInfoUtil::NORMAL_TOPIC : (string)$retry;
        $ack->startOffset = (int)(self::pick($d, 'so') ?? 0);
        $ack->queueId = (int)(self::pick($d, 'q') ?? 0);
        $ack->reviveQueueId = (int)(self::pick($d, 'rq') ?? 0);
        $ack->popTime = (int)(self::pick($d, 'pt') ?? 0);
        $ack->invisibleTime = (int)(self::pick($d, 'it') ?? 0);
        $ack->bitSet = BatchAckBitSet::fromBase64(self::pickString($d, 'b'));
        return $ack;
    }

    /** Compact key first, long name as the fallback. */
    private static function pick(array $d, string $compact): mixed
    {
        if (array_key_exists($compact, $d)) {
            return $d[$compact];
        }
        return $d[self::KEY_ALIASES[$compact]] ?? null;
    }

    private static function pickString(array $d, string $compact): ?string
    {
        $v = self::pick($d, $compact);
        return $v === null ? null : (string)$v;
    }
}

/**
 * The body of BATCH_ACK_MESSAGE (200151): one broker plus the merged entries.
 *
 * Two checkpoints belong to the same pop -- and therefore to the same entry -- when their retry flag,
 * queueId, checkpoint offset **and** pop time all match; that quadruple is the merge key. The first
 * checkpoint of a group fixes the entry's fields, and every checkpoint of the group (the first
 * included) contributes exactly one bit at index ``queueOffset - ckQueueOffset``. Entries are emitted
 * in first-seen order.
 *
 * The list spans **one** broker: the first checkpoint decides the body's broker name (and the address
 * the request goes to), so checkpoints of other brokers must be acknowledged by a separate call.
 */
final class BatchAckMessageRequestBody
{
    public ?string $brokerName = null;
    /** @var list<BatchAck> */
    public array $acks = [];

    /** @param list<string> $extraInfoList */
    public static function fromExtraInfos(string $consumerGroup, string $topic, array $extraInfoList): self
    {
        $body = new self();
        /** @var array<string, BatchAck> $merged keyed by merge key, insertion order preserved */
        $merged = [];
        foreach ($extraInfoList as $extraInfo) {
            $segments = ExtraInfoUtil::split($extraInfo);
            if ($body->brokerName === null) {
                $body->brokerName = ExtraInfoUtil::getBrokerName($segments);
            }
            $retry = ExtraInfoUtil::getRetry($segments);
            $queueId = ExtraInfoUtil::getQueueId($segments);
            $ckQueueOffset = ExtraInfoUtil::getCkQueueOffset($segments);
            $popTime = ExtraInfoUtil::getPopTime($segments);
            $mergeKey = $retry . '@' . $queueId . '@' . $ckQueueOffset . '@' . $popTime;

            $ack = $merged[$mergeKey] ?? null;
            if ($ack === null) {
                $ack = new BatchAck();
                $ack->consumerGroup = $consumerGroup;
                $ack->topic = $topic;
                $ack->retry = $retry;
                $ack->startOffset = $ckQueueOffset;
                $ack->queueId = $queueId;
                $ack->reviveQueueId = ExtraInfoUtil::getReviveQid($segments);
                $ack->popTime = $popTime;
                $ack->invisibleTime = ExtraInfoUtil::getInvisibleTime($segments);
                $merged[$mergeKey] = $ack;
            }
            // The bit index is relative to the checkpoint of this very pop, never the absolute offset.
            $ack->bitSet->set(ExtraInfoUtil::getQueueOffset($segments) - $ckQueueOffset);
        }
        $body->acks = array_values($merged);
        return $body;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'brokerName' => $this->brokerName,
            'acks' => array_map(static fn(BatchAck $ack): array => $ack->toDict(), $this->acks),
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $body = new self();
        $body->brokerName = isset($d['brokerName']) ? (string)$d['brokerName'] : null;
        foreach ((array)($d['acks'] ?? []) as $entry) {
            if (is_array($entry)) {
                $body->acks[] = BatchAck::fromDict($entry);
            }
        }
        return $body;
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

<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

// The classes in Bodies.php depend on the decode_message_queue_map / message_queue_key helper functions
// in AdminBodies.php (functions cannot be pulled in by the autoloader, so the file has to be loaded
// explicitly once).
require_once __DIR__ . '/AdminBodies.php';

/**
 * Common bodies: the JSON body structures of requests/responses, one request code maps to one shape.
 *
 * ⚠ For maps keyed by MessageQueue: the broker-side serializer **inlines the keys as JSON objects**
 * (``{{"brokerName":...,"queueId":...,"topic":...}:{...}}``) — that shape is not legal JSON, but the
 * broker's parser accepts it, so it has to be handled with messageQueueKey()/decodeMessageQueueMap()
 * rather than as an ordinary string key. This port stores it uniformly as list<array{mq, value}>.
 */
final class Bodies
{
    // Placeholder class: it only exists so that the autoloader hits this file (see the classmap in
    // bootstrap.php).
}

final class KVTable
{
    /** @var array<string, string> */
    public array $table = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['table' => (object)$this->table];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $kv = new self();
        $kv->table = (array)($d['table'] ?? []);
        return $kv;
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

final class TopicList
{
    /** @var list<string> */
    public array $topicList = [];
    public ?string $brokerAddr = null;

    /** @return list<string> */
    public function getTopicList(): array
    {
        return $this->topicList;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = ['topicList' => $this->topicList];
        if ($this->brokerAddr !== null) {
            $d['brokerAddr'] = $this->brokerAddr;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $tl = new self();
        $tl->topicList = array_map(strval(...), array_values((array)($d['topicList'] ?? [])));
        $tl->brokerAddr = isset($d['brokerAddr']) ? (string)$d['brokerAddr'] : null;
        return $tl;
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

final class LockBatchRequestBody
{
    public ?string $consumerGroup = null;
    public ?string $clientId = null;
    /** @var list<array<string, mixed>> */
    public array $mqSet = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'consumerGroup' => $this->consumerGroup,
            'clientId' => $this->clientId,
            'mqSet' => $this->mqSet,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->consumerGroup = isset($d['consumerGroup']) ? (string)$d['consumerGroup'] : null;
        $b->clientId = isset($d['clientId']) ? (string)$d['clientId'] : null;
        $b->mqSet = array_values((array)($d['mqSet'] ?? []));
        return $b;
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

final class LockBatchResponseBody
{
    /** @var list<array<string, mixed>> */
    public array $lockOkMqSet = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['lockOKMQSet' => $this->lockOkMqSet];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->lockOkMqSet = array_values((array)($d['lockOKMQSet'] ?? []));
        return $b;
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

final class UnlockBatchRequestBody
{
    public ?string $consumerGroup = null;
    public ?string $clientId = null;
    /** @var list<array<string, mixed>> */
    public array $mqSet = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'consumerGroup' => $this->consumerGroup,
            'clientId' => $this->clientId,
            'mqSet' => $this->mqSet,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->consumerGroup = isset($d['consumerGroup']) ? (string)$d['consumerGroup'] : null;
        $b->clientId = isset($d['clientId']) ? (string)$d['clientId'] : null;
        $b->mqSet = array_values((array)($d['mqSet'] ?? []));
        return $b;
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

final class GetConsumerListByGroupResponseBody
{
    /** @var list<string> */
    public array $consumerIdList = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['consumerIdList' => $this->consumerIdList];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->consumerIdList = array_map(strval(...), array_values((array)($d['consumerIdList'] ?? [])));
        return $b;
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
 * The body pushed down when the broker queries back the client configuration.
 *
 * It is only used by ``CHECK_CLIENT_CONFIG(46)``: the broker logs the clientId/group, and what is really
 * validated is only the expressionType and subString of ``subscriptionData``. The namespace field exists
 * in the protocol but the sender does not fill it; here the field is likewise kept without a value (the
 * broker side does not serialize null).
 */
final class CheckClientRequestBody
{
    public ?string $clientId = null;
    public ?string $group = null;
    public ?SubscriptionData $subscriptionData = null;
    public ?string $namespace = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = ['clientId' => $this->clientId, 'group' => $this->group];
        if ($this->subscriptionData !== null) {
            $d['subscriptionData'] = $this->subscriptionData->toDict();
        }
        if ($this->namespace !== null) {
            $d['namespace'] = $this->namespace;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->clientId = isset($d['clientId']) ? (string)$d['clientId'] : null;
        $b->group = isset($d['group']) ? (string)$d['group'] : null;
        $b->namespace = isset($d['namespace']) ? (string)$d['namespace'] : null;
        $sd = $d['subscriptionData'] ?? null;
        if (is_array($sd)) {
            $sub = new SubscriptionData($sd['topic'] ?? null, $sd['subString'] ?? null);
            $sub->classFilterMode = (bool)($sd['classFilterMode'] ?? false);
            $sub->tagsSet = array_values((array)($sd['tagsSet'] ?? []));
            $sub->codeSet = array_map(intval(...), array_values((array)($sd['codeSet'] ?? [])));
            $sub->subVersion = (int)($sd['subVersion'] ?? 0);
            $sub->expressionType = (string)($sd['expressionType'] ?? 'TAG');
            $b->subscriptionData = $sub;
        }
        return $b;
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

final class ClusterInfo
{
    /** @var array<string, array<int, string>> brokerName → {brokerId: addr} */
    public array $brokerAddrTable = [];
    /** @var array<string, list<string>> */
    public array $clusterAddrTable = [];

    /**
     * Collects the addresses of all brokers (deduplicated, ordered by broker name then brokerId).
     *
     * @return list<string>
     */
    public function getBrokerAddrs(): array
    {
        $addrs = [];
        $seen = [];
        $names = array_keys($this->brokerAddrTable);
        sort($names, SORT_STRING);
        foreach ($names as $brokerName) {
            $ids = array_keys($this->brokerAddrTable[$brokerName]);
            sort($ids);
            foreach ($ids as $brokerId) {
                $addr = $this->brokerAddrTable[$brokerName][$brokerId];
                if ($addr !== '' && !isset($seen[$addr])) {
                    $seen[$addr] = true;
                    $addrs[] = $addr;
                }
            }
        }
        return $addrs;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $table = [];
        foreach ($this->brokerAddrTable as $name => $ids) {
            $table[$name] = [
                'cluster' => '',
                'brokerName' => $name,
                'brokerAddrs' => (object)$ids,
                'enableActingMaster' => false,
            ];
        }
        return [
            'brokerAddrTable' => (object)$table,
            'clusterAddrTable' => (object)$this->clusterAddrTable,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $ci = new self();
        // In real RocketMQ, brokerAddrTable[name] is a BrokerData object, and the actual
        // {brokerId: addr} mapping sits under its brokerAddrs field.
        foreach ((array)($d['brokerAddrTable'] ?? []) as $k => $vv) {
            $ids = [];
            foreach ((array)(is_array($vv) ? ($vv['brokerAddrs'] ?? []) : []) as $kk => $v) {
                $ids[(int)$kk] = (string)$v;
            }
            $ci->brokerAddrTable[(string)$k] = $ids;
        }
        $ci->clusterAddrTable = (array)($d['clusterAddrTable'] ?? []);
        return $ci;
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
 * The consumer running information: the main structure for diagnostics and statistics reporting.
 *
 * Both sides need it: the **admin side** parses the running information aggregated by the broker, and
 * **this client** encodes its own running information when answering
 * GET_CONSUMER_RUNNING_INFO(307).
 *
 * ⚠ The keys of ``mqTable`` / ``mqPopTable`` are ``MessageQueue`` (inline object keys once serialized);
 * this port accesses them with message_queue_key() string keys.
 */
final class ConsumerRunningInfo
{
    public const PROP_NAMESERVER_ADDR = 'PROP_NAMESERVER_ADDR';
    public const PROP_THREADPOOL_CORE_SIZE = 'PROP_THREADPOOL_CORE_SIZE';
    public const PROP_CONSUME_ORDERLY = 'PROP_CONSUMEORDERLY';   // the protocol key really is CONSUMEORDERLY, without an underscore
    public const PROP_CONSUME_TYPE = 'PROP_CONSUME_TYPE';
    public const PROP_CLIENT_VERSION = 'PROP_CLIENT_VERSION';
    public const PROP_CONSUMER_START_TIMESTAMP = 'PROP_CONSUMER_START_TIMESTAMP';

    /** @var array<string, string> */
    public array $properties = [];
    /** @var list<array<string, mixed>> */
    public array $subscriptionSet = [];
    /** @var array<string, array<string, mixed>> key = message_queue_key() */
    public array $mqTable = [];
    /** @var array<string, array<string, mixed>> key = message_queue_key() */
    public array $mqPopTable = [];
    /** @var array<string, array<string, mixed>> */
    public array $statusTable = [];
    /** @var array<string, string> */
    public array $userConsumerInfo = [];
    public ?string $jstack = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'properties' => (object)$this->properties,
            'subscriptionSet' => $this->subscriptionSet,
            'mqTable' => (object)$this->mqTable,
            'mqPopTable' => (object)$this->mqPopTable,
            'statusTable' => (object)$this->statusTable,
            'userConsumerInfo' => (object)$this->userConsumerInfo,
            'jstack' => $this->jstack,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $ri = new self();
        $ri->properties = (array)($d['properties'] ?? []);
        $ri->subscriptionSet = array_values((array)($d['subscriptionSet'] ?? []));
        $ri->mqTable = self::decodeMqTable($d['mqTable'] ?? null);
        $ri->mqPopTable = self::decodeMqTable($d['mqPopTable'] ?? null);
        $ri->statusTable = (array)($d['statusTable'] ?? []);
        $ri->userConsumerInfo = (array)($d['userConsumerInfo'] ?? []);
        $ri->jstack = isset($d['jstack']) ? (string)$d['jstack'] : null;
        return $ri;
    }

    /** @return array<string, array<string, mixed>> */
    private static function decodeMqTable(?array $raw): array
    {
        $out = [];
        foreach ($raw ?? [] as $k => $v) {
            if (parse_message_queue_key((string)$k) !== null) {
                $out[(string)$k] = (array)$v;
            }
        }
        return $out;
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

final class Connection
{
    public ?string $clientId = null;
    public ?string $clientAddr = null;
    public ?string $language = null;
    public ?int $version = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'clientId' => $this->clientId,
            'clientAddr' => $this->clientAddr,
            'language' => $this->language,
            'version' => $this->version,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $c = new self();
        $c->clientId = isset($d['clientId']) ? (string)$d['clientId'] : null;
        $c->clientAddr = isset($d['clientAddr']) ? (string)$d['clientAddr'] : null;
        $c->language = isset($d['language']) ? (string)$d['language'] : null;
        $c->version = isset($d['version']) ? (int)$d['version'] : null;
        return $c;
    }
}

final class ConsumerConnection
{
    /** @var list<Connection> */
    public array $connectionSet = [];
    /** @var array<string, array<string, mixed>> */
    public array $subscriptionTable = [];
    public ?string $consumeType = null;
    public ?string $messageModel = null;
    public ?string $consumeFromWhere = null;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'connectionSet' => array_map(static fn(Connection $c): array => $c->toDict(), $this->connectionSet),
            'subscriptionTable' => (object)$this->subscriptionTable,
            'consumeType' => $this->consumeType,
            'messageModel' => $this->messageModel,
            'consumeFromWhere' => $this->consumeFromWhere,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $cc = new self();
        $cc->connectionSet = array_map(static fn(array $c): Connection => Connection::fromDict($c), (array)($d['connectionSet'] ?? []));
        $cc->subscriptionTable = (array)($d['subscriptionTable'] ?? []);
        $cc->consumeType = isset($d['consumeType']) ? (string)$d['consumeType'] : null;
        $cc->messageModel = isset($d['messageModel']) ? (string)$d['messageModel'] : null;
        $cc->consumeFromWhere = isset($d['consumeFromWhere']) ? (string)$d['consumeFromWhere'] : null;
        return $cc;
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

final class ProducerConnection
{
    /** @var list<Connection> */
    public array $connectionSet = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['connectionSet' => array_map(static fn(Connection $c): array => $c->toDict(), $this->connectionSet)];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $pc = new self();
        $pc->connectionSet = array_map(static fn(array $c): Connection => Connection::fromDict($c), (array)($d['connectionSet'] ?? []));
        return $pc;
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

final class QueryConsumeTimeSpanBody
{
    /** @var list<array<string, mixed>> */
    public array $consumeTimeSpanSet = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['consumeTimeSpanSet' => $this->consumeTimeSpanSet];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        $b->consumeTimeSpanSet = array_values((array)($d['consumeTimeSpanSet'] ?? []));
        return $b;
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

final class ConsumeStatus
{
    public float $pullRt = 0;
    public float $pullTps = 0;
    public float $consumeRt = 0;
    public float $consumeOkTps = 0;
    public float $consumeFailedTps = 0;
    public int $consumeFailedMsgs = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'pullRT' => $this->pullRt,
            'pullTPS' => $this->pullTps,
            'consumeRT' => $this->consumeRt,
            'consumeOKTPS' => $this->consumeOkTps,
            'consumeFailedTPS' => $this->consumeFailedTps,
            'consumeFailedMsgs' => $this->consumeFailedMsgs,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $cs = new self();
        $cs->pullRt = (float)($d['pullRT'] ?? 0);
        $cs->pullTps = (float)($d['pullTPS'] ?? 0);
        $cs->consumeRt = (float)($d['consumeRT'] ?? 0);
        $cs->consumeOkTps = (float)($d['consumeOKTPS'] ?? 0);
        $cs->consumeFailedTps = (float)($d['consumeFailedTPS'] ?? 0);
        $cs->consumeFailedMsgs = (int)($d['consumeFailedMsgs'] ?? 0);
        return $cs;
    }
}

/**
 * The aggregated consumption statistics (the response body of `GET_BROKER_CONSUME_STATS`).
 *
 * ⚠ The JSON key is the protocol field name `consumeStatsList`, not the property name `statsList`: when
 * the key name is wrong, a real response parses into an empty list, which looks like "this broker has no
 * backlog".
 */
final class ConsumeStatsList
{
    /** @var list<array<string, mixed>> */
    public array $statsList = [];
    public ?string $brokerAddr = null;
    public int $totalDiff = 0;
    public int $totalInflightDiff = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = ['consumeStatsList' => $this->statsList];
        if ($this->brokerAddr !== null) {
            $d['brokerAddr'] = $this->brokerAddr;
        }
        $d['totalDiff'] = $this->totalDiff;
        $d['totalInflightDiff'] = $this->totalInflightDiff;
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $sl = new self();
        $sl->statsList = array_values((array)($d['consumeStatsList'] ?? []));
        $sl->brokerAddr = isset($d['brokerAddr']) ? (string)$d['brokerAddr'] : null;
        $sl->totalDiff = (int)(($d['totalDiff'] ?? 0) ?: 0);
        $sl->totalInflightDiff = (int)(($d['totalInflightDiff'] ?? 0) ?: 0);
        return $sl;
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
 * The map form of the offset reset (222) body.
 *
 * ⚠ In the protocol, ``offsetTable`` is a flat MessageQueue → Long table, **not** a nested
 * topic→queueId→offset map (writing it as a nested map makes it impossible to parse real broker
 * responses).
 * The MessageQueue keys get inlined as JSON objects, so they are parsed with the key helpers from
 * AdminBodies.
 */
final class ResetOffsetBody
{
    /** @var list<array{mq: MessageQueue, offset: int}> */
    public array $offsetTable = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $table = [];
        foreach ($this->offsetTable as ['mq' => $mq, 'offset' => $v]) {
            $table[message_queue_key($mq)] = $v;
        }
        return ['offsetTable' => (object)$table];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        foreach (decode_message_queue_map($d['offsetTable'] ?? null) as ['mq' => $mq, 'value' => $v]) {
            $b->offsetTable[] = ['mq' => $mq, 'offset' => (int)$v];
        }
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

/** The queue element in the ForC array form: the three queue elements plus an optional offset. */
final class MessageQueueForC
{
    public function __construct(
        public string $topic = '',
        public string $brokerName = '',
        public int $queueId = 0,
        public int $offset = 0,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['topic' => $this->topic, 'brokerName' => $this->brokerName, 'queueId' => $this->queueId, 'offset' => $this->offset];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            (string)($d['topic'] ?? ''),
            (string)($d['brokerName'] ?? ''),
            (int)(($d['queueId'] ?? 0) ?: 0),
            (int)(($d['offset'] ?? 0) ?: 0),
        );
    }
}

/**
 * The array form of the offset reset (222) body: ``offsetTable`` is an **array** (each entry carries its
 * own offset), not a map.
 *
 * The broker picks the shape by the language code reported by the initiator: only language=CPP pushes this
 * array form, and every other language code pushes the map form. The initiator of a reset may well be an
 * admin tool reporting CPP, so both have to be decodable — receiving something that cannot be parsed
 * amounts to silently discarding the whole reset.
 */
final class ResetOffsetBodyForC
{
    /** @var list<MessageQueueForC> */
    public array $offsetTable = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return ['offsetTable' => array_map(static fn(MessageQueueForC $e): array => $e->toDict(), $this->offsetTable)];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        foreach ((array)($d['offsetTable'] ?? []) as $item) {
            if (is_array($item)) {
                $b->offsetTable[] = MessageQueueForC::fromDict($item);
            }
        }
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- 42 GET_CONSUMER_STATUS_FROM_CLIENT

/**
 * The body of fetching the client consumption status (42).
 *
 * The keys of both maps are ``MessageQueue`` (inline object keys).
 */
final class GetConsumerStatusBody
{
    /** @var array<string, array{mq: MessageQueue, offset: int}> */
    public array $messageQueueTable = [];
    // Deprecated in the protocol but the structure is still there (consumerTable: clientId -> offset
    // table); parsing keeps supporting it as before
    /** @var array<string, list<array{mq: MessageQueue, offset: int}>> */
    public array $consumerTable = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $mqt = [];
        foreach ($this->messageQueueTable as ['mq' => $mq, 'offset' => $v]) {
            $mqt[message_queue_key($mq)] = $v;
        }
        $ct = [];
        foreach ($this->consumerTable as $cid => $tbl) {
            $inner = [];
            foreach ($tbl as ['mq' => $mq, 'offset' => $v]) {
                $inner[message_queue_key($mq)] = $v;
            }
            $ct[$cid] = (object)$inner;
        }
        return [
            'messageQueueTable' => (object)$mqt,
            'consumerTable' => (object)$ct,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $b = new self();
        foreach (decode_message_queue_map($d['messageQueueTable'] ?? null) as ['mq' => $mq, 'value' => $v]) {
            $b->messageQueueTable[] = ['mq' => $mq, 'offset' => (int)$v];
        }
        foreach ((array)($d['consumerTable'] ?? []) as $cid => $tbl) {
            $list = [];
            foreach (decode_message_queue_map(is_array($tbl) ? $tbl : null) as ['mq' => $mq, 'value' => $v]) {
                $list[] = ['mq' => $mq, 'offset' => (int)$v];
            }
            $b->consumerTable[(string)$cid] = $list;
        }
        return $b;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

// ---------------------------------------------------------------- 307 / 309

/** A snapshot of a single process queue (reported queue by queue when answering 307). */
final class ProcessQueueInfo
{
    public int $commitOffset = 0;
    public int $cachedMsgMinOffset = 0;
    public int $cachedMsgMaxOffset = 0;
    public int $cachedMsgCount = 0;
    public int $cachedMsgSizeInMiB = 0;
    public int $transactionMsgMinOffset = 0;
    public int $transactionMsgMaxOffset = 0;
    public int $transactionMsgCount = 0;
    public bool $locked = false;
    public int $tryUnlockTimes = 0;
    public int $lastLockTimestamp = 0;
    public bool $droped = false;          // the protocol field name really is droped (spelled this way, changing it loses the field)
    public int $lastPullTimestamp = 0;
    public int $lastConsumeTimestamp = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'commitOffset' => $this->commitOffset,
            'cachedMsgMinOffset' => $this->cachedMsgMinOffset,
            'cachedMsgMaxOffset' => $this->cachedMsgMaxOffset,
            'cachedMsgCount' => $this->cachedMsgCount,
            'cachedMsgSizeInMiB' => $this->cachedMsgSizeInMiB,
            'transactionMsgMinOffset' => $this->transactionMsgMinOffset,
            'transactionMsgMaxOffset' => $this->transactionMsgMaxOffset,
            'transactionMsgCount' => $this->transactionMsgCount,
            'locked' => $this->locked,
            'tryUnlockTimes' => $this->tryUnlockTimes,
            'lastLockTimestamp' => $this->lastLockTimestamp,
            'droped' => $this->droped,
            'lastPullTimestamp' => $this->lastPullTimestamp,
            'lastConsumeTimestamp' => $this->lastConsumeTimestamp,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $p = new self();
        $p->commitOffset = (int)($d['commitOffset'] ?? 0);
        $p->cachedMsgMinOffset = (int)($d['cachedMsgMinOffset'] ?? 0);
        $p->cachedMsgMaxOffset = (int)($d['cachedMsgMaxOffset'] ?? 0);
        $p->cachedMsgCount = (int)($d['cachedMsgCount'] ?? 0);
        $p->cachedMsgSizeInMiB = (int)($d['cachedMsgSizeInMiB'] ?? 0);
        $p->transactionMsgMinOffset = (int)($d['transactionMsgMinOffset'] ?? 0);
        $p->transactionMsgMaxOffset = (int)($d['transactionMsgMaxOffset'] ?? 0);
        $p->transactionMsgCount = (int)($d['transactionMsgCount'] ?? 0);
        $p->locked = (bool)($d['locked'] ?? false);
        $p->tryUnlockTimes = (int)($d['tryUnlockTimes'] ?? 0);
        $p->lastLockTimestamp = (int)($d['lastLockTimestamp'] ?? 0);
        $p->droped = (bool)($d['droped'] ?? false);
        $p->lastPullTimestamp = (int)($d['lastPullTimestamp'] ?? 0);
        $p->lastConsumeTimestamp = (int)($d['lastConsumeTimestamp'] ?? 0);
        return $p;
    }
}

/** The result codes of a direct consumption. */
final class CMResult
{
    public const CR_SUCCESS = 'CR_SUCCESS';
    public const CR_LATER = 'CR_LATER';
    public const CR_ROLLBACK = 'CR_ROLLBACK';
    public const CR_COMMIT = 'CR_COMMIT';
    public const CR_THROW_EXCEPTION = 'CR_THROW_EXCEPTION';
    public const CR_RETURN_NULL = 'CR_RETURN_NULL';
}

/**
 * The response body of consuming a single message directly (309).
 *
 * All fields are scalars — it is the only one in this group of bodies that does not need to deal with
 * MessageQueue inline keys.
 */
final class ConsumeMessageDirectlyResult
{
    public bool $order = false;
    public bool $autoCommit = true;
    public ?string $consumeResult = null;
    public ?string $remark = null;
    public int $spentTimeMills = 0;

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'order' => $this->order,
            'autoCommit' => $this->autoCommit,
            'consumeResult' => $this->consumeResult,
            'remark' => $this->remark,
            'spentTimeMills' => $this->spentTimeMills,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $r = new self();
        $r->order = (bool)($d['order'] ?? false);
        $r->autoCommit = (bool)($d['autoCommit'] ?? true);
        $r->consumeResult = isset($d['consumeResult']) ? (string)$d['consumeResult'] : null;
        $r->remark = isset($d['remark']) ? (string)$d['remark'] : null;
        $r->spentTimeMills = (int)($d['spentTimeMills'] ?? 0);
        return $r;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

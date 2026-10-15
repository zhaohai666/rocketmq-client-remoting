<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;

/**
 * Topic route data: the queueDatas / brokerDatas / filterServerTable three tables from the
 * namesrv response; both the publish-side and subscribe-side queue sets are assembled from it.
 */

final class QueueData
{
    public function __construct(
        public string $brokerName = '',
        public int $readQueueNums = 0,
        public int $writeQueueNums = 0,
        public int $perm = 0,
        public int $topicSysFlag = 0,
    ) {
    }

    public function clone(): self
    {
        return new self($this->brokerName, $this->readQueueNums, $this->writeQueueNums, $this->perm, $this->topicSysFlag);
    }

    public function compareTo(self $o): int
    {
        if ($this->brokerName === $o->brokerName) {
            return 0;
        }
        return $this->brokerName < $o->brokerName ? -1 : 1;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'brokerName' => $this->brokerName,
            'readQueueNums' => $this->readQueueNums,
            'writeQueueNums' => $this->writeQueueNums,
            'perm' => $this->perm,
            'topicSysFlag' => $this->topicSysFlag,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            (string)($d['brokerName'] ?? ''),
            (int)($d['readQueueNums'] ?? 0),
            (int)($d['writeQueueNums'] ?? 0),
            (int)($d['perm'] ?? 0),
            (int)($d['topicSysFlag'] ?? 0),
        );
    }

    public function equals(self $other): bool
    {
        return $this->brokerName === $other->brokerName
            && $this->readQueueNums === $other->readQueueNums
            && $this->writeQueueNums === $other->writeQueueNums
            && $this->perm === $other->perm
            && $this->topicSysFlag === $other->topicSysFlag;
    }

    public function __toString(): string
    {
        return sprintf('QueueData [brokerName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%d, topicSysFlag=%d]', $this->brokerName, $this->readQueueNums, $this->writeQueueNums, $this->perm, $this->topicSysFlag);
    }
}

final class BrokerData
{
    /** @var array<int, string> brokerId → addr */
    public array $brokerAddrs;
    public bool $enableActingMaster = false;

    /** @param array<int, string>|null $brokerAddrs */
    public function __construct(
        public string $cluster = '',
        public string $brokerName = '',
        ?array $brokerAddrs = null,
        public string $zoneName = '',
    ) {
        $this->brokerAddrs = $brokerAddrs ?? [];
    }

    public function selectBrokerAddr(): ?string
    {
        if ($this->brokerAddrs === []) {
            return null;
        }
        $masterAddr = $this->brokerAddrs[MixAll::MASTER_ID] ?? null;
        if ($masterAddr !== null) {
            return $masterAddr;
        }
        $addrs = array_values($this->brokerAddrs);
        return $addrs[mt_rand(0, count($addrs) - 1)];
    }

    public function clone(): self
    {
        $bd = new self($this->cluster, $this->brokerName, $this->brokerAddrs, $this->zoneName);
        $bd->enableActingMaster = $this->enableActingMaster;
        return $bd;
    }

    public function compareTo(self $o): int
    {
        if ($this->brokerName === $o->brokerName) {
            return 0;
        }
        return $this->brokerName < $o->brokerName ? -1 : 1;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'cluster' => $this->cluster,
            'brokerName' => $this->brokerName,
            // fastjson2 writes numeric keys as an object: {"0":"addr"}; cast to object here to force object output
            'brokerAddrs' => (object)$this->brokerAddrs,
            'zoneName' => $this->zoneName,
            'enableActingMaster' => $this->enableActingMaster,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $addrs = [];
        foreach ((array)($d['brokerAddrs'] ?? []) as $k => $v) {
            $addrs[(int)$k] = (string)$v;
        }
        $bd = new self(
            (string)($d['cluster'] ?? ''),
            (string)($d['brokerName'] ?? ''),
            $addrs,
            (string)($d['zoneName'] ?? ''),
        );
        $bd->enableActingMaster = (bool)($d['enableActingMaster'] ?? false);
        return $bd;
    }

    public function equals(self $other): bool
    {
        return $this->cluster === $other->cluster
            && $this->brokerName === $other->brokerName
            && $this->brokerAddrs === $other->brokerAddrs;
    }

    public function __toString(): string
    {
        return sprintf('BrokerData [brokerName=%s, brokerAddrs=%s]', $this->brokerName, json_encode($this->brokerAddrs));
    }
}

final class TopicRouteData
{
    public ?string $orderTopicConf = null;
    /** @var list<QueueData> */
    public array $queueDatas = [];
    /** @var list<BrokerData> */
    public array $brokerDatas = [];
    /** @var array<string, list<string>> */
    public array $filterServerTable = [];
    /** @var array<string, mixed>|null */
    public ?array $topicQueueMappingByBroker = null;

    public function getBrokerDatas(): array
    {
        return $this->brokerDatas;
    }

    /**
     * Assembles all writable MessageQueues from queueDatas + brokerDatas.
     *
     * The topic must be backfilled into every MessageQueue (it must be the real topic, never an
     * empty string), otherwise a later send/pull that re-resolves the route via mq.topic will
     * find nothing.
     *
     * The two skip conditions are hard requirements of the route semantics: the broker is not
     * found in the route, **or** the broker's brokerAddrs lacks ``MASTER_ID``; either match
     * skips the whole broker.
     * The latter is not a redundant check: slaves also register themselves into the namesrv, and
     * under the default configuration they still carry the write permission (the namesrv only
     * strips WRITE for "prime slave with enableActingMaster") — once the master goes offline,
     * the route for the same brokerName is left with only brokerId=1 and the write permission
     * remains.
     * Missing this check makes the producer pick such a queue and send the message to the slave,
     * which answers send requests with ``rejectRequest`` => SYSTEM_BUSY — and a retryable code at
     * that — burning a whole round of timeouts for nothing;
     * the right fix is to keep such queues out of the publish info entirely and treat the send as
     * having no route.
     *
     * @return list<MessageQueue>
     */
    public function getAllMessageQueue(string $topic = ''): array
    {
        $mqs = [];
        foreach ($this->queueDatas as $qd) {
            if (!PermName::checkPerm($qd->perm, PermName::PERM_WRITE)) {
                continue;
            }
            $brokerData = null;
            foreach ($this->brokerDatas as $bd) {
                if ($bd->brokerName === $qd->brokerName) {
                    $brokerData = $bd;
                    break;
                }
            }
            if ($brokerData === null) {
                continue;
            }
            if (!array_key_exists(MixAll::MASTER_ID, $brokerData->brokerAddrs)) {
                continue;
            }
            for ($i = 0; $i < $qd->writeQueueNums; $i++) {
                $mqs[] = new MessageQueue($topic, $qd->brokerName, $i);
            }
        }
        return $mqs;
    }

    /**
     * Assembles all **readable** MessageQueues from queueDatas.
     *
     * Two deliberate differences from getAllMessageQueue() (publish side):
     * 1. Only the **read** permission and ``readQueueNums`` matter (a read-only topic with
     *    perm=4 can still be consumed);
     * 2. The broker is **not required to have a master** — after the master dies, slaves must
     *    still be pullable, which is exactly why the subscribe-side broker address selection
     *    carries the slave fallback. Only the publish info needs a master.
     *
     * @return list<MessageQueue>
     */
    public function getAllSubscribeMessageQueue(string $topic = ''): array
    {
        $mqs = [];
        foreach ($this->queueDatas as $qd) {
            if (!PermName::checkPerm($qd->perm, PermName::PERM_READ)) {
                continue;
            }
            for ($i = 0; $i < $qd->readQueueNums; $i++) {
                $mqs[] = new MessageQueue($topic, $qd->brokerName, $i);
            }
        }
        return $mqs;
    }

    public function cloneTopicRouteData(): self
    {
        $trd = new self();
        $trd->orderTopicConf = $this->orderTopicConf;
        $trd->queueDatas = $this->queueDatas;
        $trd->brokerDatas = $this->brokerDatas;
        $trd->filterServerTable = array_map(static fn(array $v): array => array_values($v), $this->filterServerTable);
        if ($this->topicQueueMappingByBroker !== null) {
            $trd->topicQueueMappingByBroker = $this->topicQueueMappingByBroker;
        }
        return $trd;
    }

    public function topicRouteDataChanged(?self $old): bool
    {
        if ($old === null) {
            return true;
        }
        $cmpQd = static fn(QueueData $a, QueueData $b): int =>
            [$a->brokerName, $a->readQueueNums, $a->writeQueueNums, $a->perm]
            <=> [$b->brokerName, $b->readQueueNums, $b->writeQueueNums, $b->perm];
        $cmpBd = static fn(BrokerData $a, BrokerData $b): int => $a->brokerName <=> $b->brokerName;

        $oldQ = $this->queueDatas;
        usort($oldQ, $cmpQd);
        $newQ = $old->queueDatas;
        usort($newQ, $cmpQd);
        $oldB = $this->brokerDatas;
        usort($oldB, $cmpBd);
        $newB = $old->brokerDatas;
        usort($newB, $cmpBd);

        if (count($oldQ) !== count($newQ) || count($oldB) !== count($newB)) {
            return true;
        }
        for ($i = 0, $n = count($oldQ); $i < $n; $i++) {
            if (!$oldQ[$i]->equals($newQ[$i])) {
                return true;
            }
        }
        for ($i = 0, $n = count($oldB); $i < $n; $i++) {
            if (!$oldB[$i]->equals($newB[$i])) {
                return true;
            }
        }
        return false;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'orderTopicConf' => $this->orderTopicConf,
            'queueDatas' => array_map(static fn(QueueData $q): array => $q->toDict(), $this->queueDatas),
            'brokerDatas' => array_map(static fn(BrokerData $b): array => $b->toDict(), $this->brokerDatas),
            'filterServerTable' => (object)$this->filterServerTable,
        ];
        if ($this->topicQueueMappingByBroker !== null) {
            $d['topicQueueMappingByBroker'] = $this->topicQueueMappingByBroker;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $trd = new self();
        $trd->orderTopicConf = isset($d['orderTopicConf']) ? (string)$d['orderTopicConf'] : null;
        $trd->queueDatas = array_map(static fn(array $q): QueueData => QueueData::fromDict($q), $d['queueDatas'] ?? []);
        $trd->brokerDatas = array_map(static fn(array $b): BrokerData => BrokerData::fromDict($b), $d['brokerDatas'] ?? []);
        $trd->filterServerTable = $d['filterServerTable'] ?? [];
        $trd->topicQueueMappingByBroker = isset($d['topicQueueMappingByBroker']) && is_array($d['topicQueueMappingByBroker'])
            ? $d['topicQueueMappingByBroker'] : null;
        return $trd;
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

    public function __toString(): string
    {
        return sprintf('TopicRouteData[orderTopicConf=%s, queueDatas=%d, brokerDatas=%d]', $this->orderTopicConf === null ? 'None' : $this->orderTopicConf, count($this->queueDatas), count($this->brokerDatas));
    }
}

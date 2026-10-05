<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;

/**
 * Topic 路由数据（对应 org.apache.rocketmq.remoting.protocol.route.*，移植自 protocol/route.py）。
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
            // fastjson2 把数字键写成对象：{"0":"addr"}，这里用 object 强制输出对象
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

    /** 返回 broker 数据列表（与 Java topicRouteData.getBrokerDatas() 对齐）。 */
    public function getBrokerDatas(): array
    {
        return $this->brokerDatas;
    }

    /**
     * 根据 queueDatas + brokerDatas 组装全部可写 MessageQueue（对应 Java 的 topicRouteData2TopicPublishInfo 组装逻辑）。
     *
     * topic 用于回填到每个 MessageQueue（Java 用真实 topic，而不是空串），否则后续
     * send/pull 用 mq.topic 回查路由时会查不到。
     *
     * 两条跳过条件逐字对应 Java ``MQClientInstance.topicRouteData2TopicPublishInfo:294-303``：
     * 路由里查不到同名 broker，**或者**该 broker 的 brokerAddrs 里没有 ``MASTER_ID``。
     * 后者不是冗余判断：从节点自己也会注册进 namesrv，且默认配置下它照样带写位
     * （``RouteInfoManager:344-346`` 只在「prime slave 且 enableActingMaster」时才抹掉
     * WRITE）——master 一旦掉线，路由里同一个 brokerName 就只剩 brokerId=1，写位还在。
     * 漏判这条，生产者会挑中这台队列并把消息发到从节点上，而从节点对发送请求是
     * ``rejectRequest``（``SendMessageProcessor:131`` ⇒ SYSTEM_BUSY，还是个可重试码），
     * 于是白烧一轮超时；Java 那边这种队列压根进不了发布信息，发送直接当无路由处理。
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
     * 按 queueDatas 组装全部**可读** MessageQueue（对应 Java ``MQClientInstance.topicRouteData2TopicSubscribeInfo:318-332``）。
     *
     * 与 getAllMessageQueue()（发布侧）有两处刻意的不同，都是 Java 的语义：
     * ①只看**读**位与 ``readQueueNums``（perm=4 的只读 topic 在 Java 里照样能被消费）；
     * ②**不要求 broker 有 master** —— 主挂掉后从节点仍要能被拉取，客户端侧
     * ``findBrokerAddressInSubscribe`` 正是为此才带从节点回退。只有发布信息需要 master。
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

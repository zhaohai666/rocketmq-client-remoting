<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageQueue;

/**
 * 发送延迟故障容错（对应 org.apache.rocketmq.client.latency.*），移植自 latency.py。
 *
 * 实现 MQFaultStrategy + LatencyFaultToleranceImpl（带 FaultItem）：追踪每个 broker 的
 * 发送延迟，延迟过高或发生异常时**隔离**一段时间（不分配给新消息），默认关闭。
 *
 * 与 Java 关键点逐条对齐：
 *   * latencyMax / notAvailableDuration 两套阈值表；
 *   * updateFaultItem 的 notAvailableDuration 取 ``computeNotAvailableDuration``，
 *     隔离（异常）场景固定按 10000ms 算档位；
 *   * FaultItem.isAvailable() = now >= startTimestamp（隔离期未过则不可用）；
 *   * LatencyFaultToleranceImpl.isAvailable/isReachable 在没有记录时返回 true。
 */
final class FaultItem
{
    public float $currentLatency = 0.0;
    public float $startTimestamp = 0.0;
    public float $checkStamp = 0.0;
    public bool $reachableFlag = true;

    public function __construct(public string $name)
    {
    }

    /** Java：only when now + dur > startTimestamp 才更新（保持最长隔离期）。 */
    public function updateNotAvailableDuration(float $notAvailableDuration): void
    {
        $now = self::nowMillis();
        if ($notAvailableDuration > 0 && $now + $notAvailableDuration > $this->startTimestamp) {
            $this->startTimestamp = $now + $notAvailableDuration;
        }
    }

    public function isAvailable(): bool
    {
        return self::nowMillis() >= $this->startTimestamp;
    }

    public function isReachable(): bool
    {
        return $this->reachableFlag;
    }

    public function __toString(): string
    {
        return sprintf(
            'FaultItem{name=%s, latency=%.0f, startTs=%.0f, reachable=%s}',
            $this->name,
            $this->currentLatency,
            $this->startTimestamp,
            $this->reachableFlag ? 'True' : 'False',
        );
    }

    /** 当前毫秒时间戳（对应 Python ``time.time() * 1000.0``）。 */
    public static function nowMillis(): float
    {
        return microtime(true) * 1000.0;
    }
}

/**
 * 对应 Java client.latency.LatencyFaultToleranceImpl（简化为纯内存版，无探测线程）。
 *
 * 省略 Java 的"后台可达性探测线程"（startDetector）；本地保留 reachableFlag 语义：
 * updateFaultItem(..., reachable) 时写 reachableFlag。
 *
 * PHP 单线程：Python 的 threading.RLock 只为保护 _faultItemTable 读写的一致性，
 * 单线程下无需加锁；这里保留"同一时刻只有一处读写该表"的注释性约束。
 */
final class LatencyFaultToleranceImpl
{
    /** @var array<string, FaultItem> */
    private array $faultItemTable = [];

    public function updateFaultItem(string $name, float $currentLatency, float $notAvailableDuration, bool $reachable): void
    {
        $item = $this->faultItemTable[$name] ?? null;
        if ($item === null) {
            $item = new FaultItem($name);
            $item->currentLatency = $currentLatency;
            $item->updateNotAvailableDuration($notAvailableDuration);
            $item->reachableFlag = $reachable;
            $this->faultItemTable[$name] = $item;
            return;
        }
        $item->currentLatency = $currentLatency;
        $item->updateNotAvailableDuration($notAvailableDuration);
        $item->reachableFlag = $reachable;
    }

    public function isAvailable(string $name): bool
    {
        $item = $this->faultItemTable[$name] ?? null;
        if ($item !== null) {
            return $item->isAvailable();
        }
        return true;
    }

    public function isReachable(string $name): bool
    {
        $item = $this->faultItemTable[$name] ?? null;
        if ($item !== null) {
            return $item->isReachable();
        }
        return true;
    }

    public function remove(string $name): void
    {
        unset($this->faultItemTable[$name]);
    }

    public function getFaultItem(string $name): ?FaultItem
    {
        return $this->faultItemTable[$name] ?? null;
    }
}

/**
 * 对应 Java client.latency.MQFaultStrategy。
 *
 * 仅当 ``sendLatencyFaultEnable`` 为 true 时，发送选队列阶段会：
 *   1) 优先选 available（隔离期已过）的 broker；
 *   2) 否则选 reachable 的 broker；
 *   3) 否则退化为普通轮询。
 * 发送结果/异常会回调 ``updateFaultItem`` 写延迟与隔离信息。
 */
final class MQFaultStrategy
{
    /** @var list<int> */
    public const LATENCY_MAX = [50, 100, 550, 1800, 3000, 5000, 15000];
    /** @var list<int> */
    public const NOT_AVAILABLE_DURATION = [0, 0, 2000, 5000, 6000, 10000, 30000];

    private bool $sendLatencyFaultEnable;
    private LatencyFaultToleranceImpl $latencyFaultTolerance;

    /** @var list<int> */
    public array $latencyMax;
    /** @var list<int> */
    public array $notAvailableDuration;

    public function __construct(bool $sendLatencyFaultEnable = false)
    {
        $this->sendLatencyFaultEnable = $sendLatencyFaultEnable;
        $this->latencyFaultTolerance = new LatencyFaultToleranceImpl();
        $this->latencyMax = self::LATENCY_MAX;
        $this->notAvailableDuration = self::NOT_AVAILABLE_DURATION;
    }

    // ---- 配置 ----
    public function isSendLatencyFaultEnable(): bool
    {
        return $this->sendLatencyFaultEnable;
    }

    public function setSendLatencyFaultEnable(bool $enable): void
    {
        $this->sendLatencyFaultEnable = $enable;
    }

    // ---- 队列选择 ----

    private function availableFilter(MessageQueue $mq): bool
    {
        return $this->latencyFaultTolerance->isAvailable($mq->getBrokerName());
    }

    private function reachableFilter(MessageQueue $mq): bool
    {
        return $this->latencyFaultTolerance->isReachable($mq->getBrokerName());
    }

    /**
     * 对应 Java ``MQFaultStrategy.selectOneMessageQueue``。
     *
     * `$tpInfo` 是 TopicPublishInfo 的等价对象，需提供
     * ``selectOneMessageQueue(callable ...$filters)`` 与 ``resetIndex()``。
     * 用 object 松散类型接收（该模块不在本移植子任务范围内）。
     */
    public function selectOneMessageQueue(object $tpInfo, ?string $lastBrokerName, bool $resetIndex = false): ?MessageQueue
    {
        $brokerFilter = static fn (MessageQueue $mq): bool => $lastBrokerName === null || $mq->getBrokerName() !== $lastBrokerName;

        if ($this->sendLatencyFaultEnable) {
            if ($resetIndex) {
                $tpInfo->resetIndex();
            }
            $mq = $tpInfo->selectOneMessageQueue(fn (MessageQueue $m): bool => $this->availableFilter($m), $brokerFilter);
            if ($mq !== null) {
                return $mq;
            }
            $mq = $tpInfo->selectOneMessageQueue(fn (MessageQueue $m): bool => $this->reachableFilter($m), $brokerFilter);
            if ($mq !== null) {
                return $mq;
            }
            return $tpInfo->selectOneMessageQueue();
        }
        $mq = $tpInfo->selectOneMessageQueue($brokerFilter);
        if ($mq !== null) {
            return $mq;
        }
        return $tpInfo->selectOneMessageQueue();
    }

    // ---- 故障记录 ----

    public function updateFaultItem(string $brokerName, float $currentLatency, bool $isolation, bool $reachable): void
    {
        if (!$this->sendLatencyFaultEnable) {
            return;
        }
        $latency = $isolation ? 10000 : $currentLatency;
        $duration = $this->computeNotAvailableDuration($latency);
        $this->latencyFaultTolerance->updateFaultItem($brokerName, $currentLatency, $duration, $reachable);
    }

    private function computeNotAvailableDuration(float $currentLatency): float
    {
        for ($i = count($this->latencyMax) - 1; $i >= 0; $i--) {
            if ($currentLatency >= $this->latencyMax[$i]) {
                return (float) $this->notAvailableDuration[$i];
            }
        }
        return 0.0;
    }

    public function getLatencyFaultTolerance(): LatencyFaultToleranceImpl
    {
        return $this->latencyFaultTolerance;
    }
}

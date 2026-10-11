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
 * 对应 Java client.latency.LatencyFaultToleranceImpl。
 *
 * 主动探活（5.x startDetector）收敛进调用方的 ``tick()``：Java 用
 * LatencyFaultToleranceScheduledThread 每 3s 跑一轮 ``detectByOneRound``，PHP 无线程，
 * 由 MQClientInstance::tick() 按到期判定驱动同一循环体（见 {@see detectByOneRound()}）。
 * 本地保留 reachableFlag 语义：updateFaultItem(..., reachable) 时写 reachableFlag；
 * 探测成功把 false 翻回 true（Java 只翻回、从不置 false——置 false 是发送路径的职责）。
 *
 * PHP 单线程：Python 的 threading.RLock 只为保护 _faultItemTable 读写的一致性，
 * 单线程下无需加锁；这里保留"同一时刻只有一处读写该表"的注释性约束。
 */
final class LatencyFaultToleranceImpl
{
    /** @var array<string, FaultItem> */
    private array $faultItemTable = [];

    /** 对应 Java detectTimeout（ClientConfig，默认 200ms）：单次探测的超时预算。 */
    public int $detectTimeout = 200;

    /** 对应 Java detectInterval（ClientConfig，默认 2000ms）：**每个** broker 的探测周期。 */
    public int $detectInterval = 2000;

    private bool $startDetectorEnable = false;

    public function isStartDetectorEnable(): bool
    {
        return $this->startDetectorEnable;
    }

    public function setStartDetectorEnable(bool $enable): void
    {
        $this->startDetectorEnable = $enable;
    }

    public function updateFaultItem(string $name, float $currentLatency, float $notAvailableDuration, bool $reachable): void
    {
        if (!$reachable) {
            Logger::info($name . ' is unreachable, it will not be used until it\'s reachable');
        }
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

    /**
     * 对应 Java ``LatencyFaultToleranceImpl.detectByOneRound``（后台探测线程的单轮循环体）。
     *
     * 逐项检查故障表：``$nowMillis - checkStamp >= 0`` 的 broker 视为到期——
     *   1) 先把 checkStamp 推到 now + detectInterval（无论探测成败，周期照排）；
     *   2) resolver 把 brokerName 解析成 master 发布地址；解析不到（broker 已离开路由）
     *      直接**摘除**故障记录（Java 同：记录不能比路由活得久）；
     *   3) serviceDetector 为 null 时跳过（Java 同：装了探测线程但没装探测器）；
     *   4) 探测成功且当前不可达 → 翻回 reachable（打日志）。**失败不动 flag**——
     *      置不可达永远是发送路径的职责，这里只负责恢复。
     *
     * `$resolver` 是 ``fn(string $brokerName): ?string``（Java Resolver.resolve）；
     * `$serviceDetector` 是 ``fn(string $brokerAddr, int $timeoutMillis): bool``
     * （Java ServiceDetector.detect），可为 null。探测是同步调用，单个到期 broker
     * 最多阻塞 detectTimeout 毫秒——调用方（tick）按此预算安排节奏。
     *
     * 注意 Java 的开关判定（startDetectorEnable）在调度线程里做、不在本方法里，
     * PHP 侧同理由 tick() 判定后再进来。
     */
    public function detectByOneRound(int $nowMillis, callable $resolver, ?callable $serviceDetector): void
    {
        foreach ($this->faultItemTable as $key => $item) {
            if ($nowMillis - $item->checkStamp < 0) {
                continue;
            }
            $item->checkStamp = $nowMillis + $this->detectInterval;
            $brokerAddr = $resolver($item->name);
            if ($brokerAddr === null) {
                unset($this->faultItemTable[$key]);
                continue;
            }
            if ($serviceDetector === null) {
                continue;
            }
            $serviceOK = $serviceDetector($brokerAddr, $this->detectTimeout);
            if ($serviceOK && !$item->reachableFlag) {
                Logger::info($item->name . ' is reachable now, then it can be used.');
                $item->reachableFlag = true;
            }
        }
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
 *
 * 主动探活（5.x）：``startDetectorEnable`` 打开后，探测轮由 MQClientInstance::tick()
 * 泵 {@see MQFaultStrategy::detectByOneRound()}，探测成功把不可达 broker 放回。
 */
final class MQFaultStrategy
{
    /** @var list<int> */
    public const LATENCY_MAX = [50, 100, 550, 1800, 3000, 5000, 15000];
    /** @var list<int> */
    public const NOT_AVAILABLE_DURATION = [0, 0, 2000, 5000, 6000, 10000, 30000];

    private bool $sendLatencyFaultEnable;
    private bool $startDetectorEnable = false;
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

    /** 对应 Java ClientConfig.startDetectorEnable（默认 false）。开关每轮探测现读，可运行中翻转。 */
    public function isStartDetectorEnable(): bool
    {
        return $this->startDetectorEnable;
    }

    public function setStartDetectorEnable(bool $enable): void
    {
        $this->startDetectorEnable = $enable;
        $this->latencyFaultTolerance->setStartDetectorEnable($enable);
    }

    /** 对应 Java ClientConfig.setDetectTimeout（默认 200ms）：单次探测超时预算。 */
    public function setDetectTimeout(int $timeoutMillis): void
    {
        $this->latencyFaultTolerance->detectTimeout = $timeoutMillis;
    }

    public function getDetectTimeout(): int
    {
        return $this->latencyFaultTolerance->detectTimeout;
    }

    /** 对应 Java ClientConfig.setDetectInterval（默认 2000ms）：每个 broker 的探测周期。 */
    public function setDetectInterval(int $intervalMillis): void
    {
        $this->latencyFaultTolerance->detectInterval = $intervalMillis;
    }

    public function getDetectInterval(): int
    {
        return $this->latencyFaultTolerance->detectInterval;
    }

    /**
     * 探测轮转发（Java MQFaultStrategy.startDetector → 调度线程 → detectByOneRound；
     * PHP 由 MQClientInstance::tick() 直接驱动，开关判定在 tick 侧，与 Java 的
     * 「开关在调度线程体里判」同位）。
     *
     * @param callable(string): ?string $resolver
     * @param (callable(string, int): bool)|null $serviceDetector
     */
    public function detectByOneRound(int $nowMillis, callable $resolver, ?callable $serviceDetector): void
    {
        $this->latencyFaultTolerance->detectByOneRound($nowMillis, $resolver, $serviceDetector);
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

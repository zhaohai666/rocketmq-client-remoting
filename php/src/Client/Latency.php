<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageQueue;

final class FaultItem
{
    public float $currentLatency = 0.0;
    public float $startTimestamp = 0.0;
    public float $checkStamp = 0.0;
    public bool $reachableFlag = true;

    public function __construct(public string $name)
    {
    }

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

    /** 当前毫秒时间戳（浮点，微秒精度）。 */
    public static function nowMillis(): float
    {
        return microtime(true) * 1000.0;
    }
}

final class LatencyFaultToleranceImpl
{
    /** @var array<string, FaultItem> */
    private array $faultItemTable = [];

    public int $detectTimeout = 200;

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

    public function isStartDetectorEnable(): bool
    {
        return $this->startDetectorEnable;
    }

    public function setStartDetectorEnable(bool $enable): void
    {
        $this->startDetectorEnable = $enable;
        $this->latencyFaultTolerance->setStartDetectorEnable($enable);
    }

    public function setDetectTimeout(int $timeoutMillis): void
    {
        $this->latencyFaultTolerance->detectTimeout = $timeoutMillis;
    }

    public function getDetectTimeout(): int
    {
        return $this->latencyFaultTolerance->detectTimeout;
    }

    public function setDetectInterval(int $intervalMillis): void
    {
        $this->latencyFaultTolerance->detectInterval = $intervalMillis;
    }

    public function getDetectInterval(): int
    {
        return $this->latencyFaultTolerance->detectInterval;
    }

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

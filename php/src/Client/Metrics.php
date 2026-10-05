<?php

declare(strict_types=1);

namespace RocketMQ\Client;

/**
 * 客户端基础指标（对应 Java 的 RT/计数统计：send/consume 耗时与成功失败数），
 * 移植自 metrics.py。
 *
 * 发送维度统计 sendRT/sendCount/sendFailureCount；消费维度统计
 * consumeRT/consumeCount/consumeFailureCount。PHP 单线程，Python 的 threading.Lock
 * 只为计数一致性，这里无需加锁（保留同一时刻单一读写的约束语义）。
 */
final class ClientMetrics
{
    // 发送
    private int $sendCount = 0;
    private int $sendFailureCount = 0;
    private float $sendRtSum = 0.0;
    private float $sendRtMax = 0.0;
    private float $sendRtMin = 0.0;
    private bool $sendStarted = false;

    // 消费
    private int $consumeCount = 0;
    private int $consumeFailureCount = 0;
    private float $consumeRtSum = 0.0;
    private float $consumeRtMax = 0.0;
    private float $consumeRtMin = 0.0;
    private bool $consumeStarted = false;

    // ---------------- 发送 ----------------
    public function recordSendStart(): float
    {
        return microtime(true) * 1000.0;
    }

    public function recordSendSuccess(float $startMs): void
    {
        $rt = microtime(true) * 1000.0 - $startMs;
        $this->sendCount++;
        $this->sendRtSum += $rt;
        if (!$this->sendStarted || $rt > $this->sendRtMax) {
            $this->sendRtMax = $rt;
        }
        if (!$this->sendStarted || $rt < $this->sendRtMin) {
            $this->sendRtMin = $rt;
        }
        $this->sendStarted = true;
    }

    public function recordSendFailure(float $startMs): void
    {
        $rt = microtime(true) * 1000.0 - $startMs;
        $this->sendFailureCount++;
        $this->sendRtSum += $rt;
        if (!$this->sendStarted || $rt > $this->sendRtMax) {
            $this->sendRtMax = $rt;
        }
        if (!$this->sendStarted || $rt < $this->sendRtMin) {
            $this->sendRtMin = $rt;
        }
        $this->sendStarted = true;
    }

    // ---------------- 消费 ----------------
    public function recordConsumeStart(): float
    {
        return microtime(true) * 1000.0;
    }

    public function recordConsumeSuccess(float $startMs): void
    {
        $rt = microtime(true) * 1000.0 - $startMs;
        $this->consumeCount++;
        $this->consumeRtSum += $rt;
        if (!$this->consumeStarted || $rt > $this->consumeRtMax) {
            $this->consumeRtMax = $rt;
        }
        if (!$this->consumeStarted || $rt < $this->consumeRtMin) {
            $this->consumeRtMin = $rt;
        }
        $this->consumeStarted = true;
    }

    public function recordConsumeFailure(float $startMs): void
    {
        $rt = microtime(true) * 1000.0 - $startMs;
        $this->consumeFailureCount++;
        $this->consumeRtSum += $rt;
        if (!$this->consumeStarted || $rt > $this->consumeRtMax) {
            $this->consumeRtMax = $rt;
        }
        if (!$this->consumeStarted || $rt < $this->consumeRtMin) {
            $this->consumeRtMin = $rt;
        }
        $this->consumeStarted = true;
    }

    // ---------------- 快照 ----------------

    /** @return array<string, float|int> */
    public function snapshot(): array
    {
        return [
            'sendCount' => $this->sendCount,
            'sendFailureCount' => $this->sendFailureCount,
            'sendRTSum' => round($this->sendRtSum, 3),
            'sendRTMax' => round($this->sendRtMax, 3),
            'sendRTMin' => round($this->sendRtMin, 3),
            'sendRTAvg' => $this->sendCount !== 0 ? round($this->sendRtSum / $this->sendCount, 3) : 0.0,
            'consumeCount' => $this->consumeCount,
            'consumeFailureCount' => $this->consumeFailureCount,
            'consumeRTSum' => round($this->consumeRtSum, 3),
            'consumeRTMax' => round($this->consumeRtMax, 3),
            'consumeRTMin' => round($this->consumeRtMin, 3),
            'consumeRTAvg' => $this->consumeCount !== 0 ? round($this->consumeRtSum / $this->consumeCount, 3) : 0.0,
        ];
    }

    public function __toString(): string
    {
        return 'ClientMetrics' . json_encode($this->snapshot());
    }
}

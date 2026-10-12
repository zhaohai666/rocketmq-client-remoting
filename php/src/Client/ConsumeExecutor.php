<?php

declare(strict_types=1);

namespace RocketMQ\Client;

final class RejectedExecutionError extends \RuntimeException
{
}

class ConsumeExecutor
{
    private int $core;
    private int $max;
    private float $keepAlive;
    private string $prefix;
    private string $nameSep;

    /** @var list<array{0: callable, 1: array<int, mixed>}> */
    private array $queue = [];

    private int $maxQueueSize;

    private int $workers = 0;
    private int $seq;
    private bool $shutdown = false;
    private int $handlerExceptions = 0;

    public function __construct(
        int $corePoolSize,
        int $maximumPoolSize,
        float $keepAliveSeconds = 60.0,
        string $threadNamePrefix = 'rmq-consume',
        int $maxQueueSize = 0,
        string $threadNameSep = '-',
        int $threadIndexFrom = 0,
    ) {
        $core = max(0, $corePoolSize);
        $this->core = $core;
        $this->max = max($core, $maximumPoolSize);
        $this->keepAlive = $keepAliveSeconds;
        $this->prefix = $threadNamePrefix;
        $this->nameSep = $threadNameSep;
        $this->maxQueueSize = max(0, $maxQueueSize);
        $this->seq = $threadIndexFrom;
    }

    // ---------------- 对外 API ----------------

    public function submit(callable $fn, mixed ...$args): void
    {
        if ($this->shutdown) {
            throw new RejectedExecutionError('ConsumeExecutor has been shut down');
        }
        $queueFull = $this->maxQueueSize !== 0 && count($this->queue) >= $this->maxQueueSize;
        $canGrow = $this->workers < $this->max;
        if ($queueFull && !$canGrow) {
            throw new RejectedExecutionError(sprintf('ConsumeExecutor queue is full (%d)', $this->maxQueueSize));
        }
        $this->queue[] = [$fn, $args];
        if ($this->workers < $this->core || ($queueFull && $canGrow) || $this->workers === 0) {
            $this->spawnLocked();
        }
    }

    public function setCorePoolSize(int $n): void
    {
        if ($n < 0) {
            throw new \ValueError('core pool size must be >= 0');
        }
        $delta = $n - $this->core;
        $this->core = $n;
        if ($n > $this->max) {
            $this->max = $n;
        }
        if ($delta > 0 && !$this->shutdown) {
            $k = min($delta, count($this->queue));
            while ($k > 0 && $this->workers < $this->max) {
                $this->spawnLocked();
                $k--;
                if ($this->queue === []) {
                    break;
                }
            }
        }
    }

    public function getCorePoolSize(): int
    {
        return $this->core;
    }

    public function getMaxPoolSize(): int
    {
        return $this->max;
    }

    public function workerCount(): int
    {
        return $this->workers;
    }

    public function queuedCount(): int
    {
        return count($this->queue);
    }

    /** 被吞掉的任务异常计数（仅供观测/单测）。 */
    public function handlerExceptionCount(): int
    {
        return $this->handlerExceptions;
    }

    public function pumpOnce(): bool
    {
        if ($this->queue !== []) {
            $task = array_shift($this->queue);
            [$fn, $args] = $task;
            try {
                $fn(...$args);
            } catch (\Throwable $e) {
                $this->handlerExceptions++;
                Logger::error('consume executor task raised, worker kept alive: ' . $e->getMessage());
            }
            return true;
        }
        // 空闲：单线程下把超编（> core）的 logical worker 退掉（对应 keep-alive 退出）
        if (!$this->shutdown && $this->workers > $this->core) {
            $this->workers = $this->core;
        }
        return false;
    }

    /** 排空并执行队列里的全部任务。 */
    public function pumpAll(): void
    {
        while ($this->pumpOnce()) {
        }
    }

    public function shutdown(bool $wait = false): void
    {
        if ($this->shutdown) {
            return;
        }
        $this->shutdown = true;
        if ($wait) {
            $this->pumpAll();
            $this->workers = 0;
        }
    }

    // ---------------- 内部 ----------------

    private function spawnLocked(): void
    {
        $this->workers++;
        $this->seq++;
    }
}

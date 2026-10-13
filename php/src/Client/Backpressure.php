<?php

declare(strict_types=1);

namespace RocketMQ\Client;


final class FairSemaphore
{
    public const MIN_ASYNC_SEND_NUM = 10;
    public const MIN_ASYNC_SEND_SIZE = 1024 * 1024;

    private int $total;
    private int $free;

    /** @var list<Pending> 一个还在排队的许可申请（只为让队首能被稳定识别） */
    private array $queue = [];

    public function __construct(int $permits)
    {
        $this->total = $permits;
        $this->free = $permits;
    }

    public function tryAcquire(int $permits, int $timeoutMillis): bool
    {
        $request = new Pending($permits);
        $this->queue[] = $request;

        $head = $this->queue[0];
        if ($head === $request && $this->free >= $permits) {
            array_shift($this->queue);
            $this->free -= $permits;
            return true;
        }

        // 超时/拿不到：把自己从队列里摘掉，别挡后面的人
        $this->removeRequest($request);
        return false;
    }

    public function release(int $permits): void
    {
        if ($permits <= 0) {
            return;
        }
        $this->free += $permits;
    }

    public function availablePermits(): int
    {
        return $this->free;
    }

    public function setTotalPermits(int $total): void
    {
        $this->free += $total - $this->total;
        $this->total = $total;
    }

    public function totalPermits(): int
    {
        return $this->total;
    }

    private function removeRequest(Pending $request): void
    {
        foreach ($this->queue as $i => $p) {
            if ($p === $request) {
                array_splice($this->queue, $i, 1);
                return;
            }
        }
    }
}

/** 一个还在排队的许可申请。 */
final class Pending
{
    public function __construct(public int $permits)
    {
    }
}

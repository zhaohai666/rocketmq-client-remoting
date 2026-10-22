<?php

declare(strict_types=1);

namespace RocketMQ\Client;


final class FairSemaphore
{
    public const MIN_ASYNC_SEND_NUM = 10;
    public const MIN_ASYNC_SEND_SIZE = 1024 * 1024;

    private int $total;
    private int $free;

    /** @var list<Pending> a permit request still waiting in line (only so the head can be stably identified) */
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

        // Timeout / no permit: remove yourself from the queue so you don't block those behind you
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

    /**
     * Requests still in line. The gate polls once and removes a request it cannot satisfy in
     * that same poll, so a refusal never leaves a ticket behind waiting for a release that
     * would arrive too late for its own budget.
     */
    public function waitingCount(): int
    {
        return count($this->queue);
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

/** A permit request still waiting in line. */
final class Pending
{
    public function __construct(public int $permits)
    {
    }
}

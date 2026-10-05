<?php

declare(strict_types=1);

namespace RocketMQ\Client;

/**
 * 异步发送背压（对应 Java ``DefaultMQProducerImpl`` 的两个公平信号量），
 * 移植自 backpressure.py。
 *
 * Java 的开关默认是**关**的。开了之后，异步发送在把任务投进 ``AsyncSenderExecutor``
 * **之前**按两个维度限流：
 *
 * * ``semaphoreAsyncSendNum`` —— 在途**条数**（默认 1024，地板 10）；
 * * ``semaphoreAsyncSendSize`` —— 在途**字节数**（默认 100M，地板 1M），
 *   一笔消息扣掉 ``body.length`` 个许可（body 为空按 1 算）。
 *
 * 两个许可都用**整个剩余预算**去等，等不到就直接回调
 * ``RemotingTooMuchRequestException(...)``，一次请求都不会发出去。
 *
 * PHP 单线程适配：Python 的 ``try_acquire`` 会阻塞等 ``timeout_millis`` 直到别的线程
 * release。单线程里没有并发释放者能在本次调用内改动空闲许可，所以"等到超时"等价于
 * "立即返回 false"；队首优先的**公平**结构仍按原样保留（见 tryAcquire）。
 */

/** Java DefaultMQProducerImpl:141-153 的两个地板值 */
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

    /**
     * 对应 Java ``tryAcquire(permits, timeout, MILLIS)``：拿不到就返回 false，不抛异常。
     *
     * 公平是这套背压的全部意义：只有**队首**能拿许可，后面的请求即使空闲许可够它也不许
     * 插队。单线程下队列在调用返回前必然清空（拿到就出队、拿不到就摘除自己），
     * 因此队首恒为本次申请；这里仍保留队首判定以维持与 Python 相同的数据结构语义。
     */
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

    /**
     * 对应 Java ``release(permits)``：**可以超过总量**（Java 同样不做校验），
     * 所以一次改小容量的窗口里多还几次不会丢计数。
     */
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

    /**
     * 把总量平移到 ``total``，在途份数原样保留（可能算出负的空闲许可 ——
     * Java ``new Semaphore(负数)`` 同样接受，归还许可会把它拉回正数）。
     */
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

/** 一个还在排队的许可申请（对应 Python 内部类 ``_Pending``）。 */
final class Pending
{
    public function __construct(public int $permits)
    {
    }
}

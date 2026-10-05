<?php

declare(strict_types=1);

namespace RocketMQ\Client;

/**
 * 消费执行器：Java ``ThreadPoolExecutor`` 的最小等价物，移植自 consume_executor.py。
 *
 * Java 的语义全部挂在 core 上：无界队列下真实并发度 == corePoolSize，
 * ``setCorePoolSize`` 是运行时改并发度的入口。对齐点：
 * 1. 任务到来时，仅当 ``workers < core`` 才新建 worker，否则入队；
 * 2. ``> core`` 的 worker 空闲后退出；``<= core`` 的 worker 永不退出；
 * 3. ``setCorePoolSize(n)``：core 变大且队列非空时按 ``min(delta, 队列长度)`` 补足 worker；
 * 4. 任务抛异常**不杀 worker**（这里直接吞掉并记日志 + 计数）。
 *
 * PHP 单线程适配（对应 PORTING.md 的「异步模型」）：Python 的 worker 是后台线程死循环，
 * 这里改成由调用方驱动的 ``pumpOnce()`` / ``pumpAll()`` —— 每调用一次执行一条排队任务。
 * 「空闲 keep-alive 退出超编线程」在此模型下等价为：一次空转的 ``pumpOnce`` 会把
 * ``workers > core`` 的多余 logical worker 退掉。
 */
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

    /** 0 = 无界（Java 的 LinkedBlockingQueue() 无参构造） */
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

    /**
     * 投递任务（Java ``execute``）。线程池已关闭时抛 ``RejectedExecutionError``，
     * 有界队列已满且 worker 数已到 max 时同样抛它（对应 Java offer 失败 -> reject）。
     */
    public function submit(callable $fn, mixed ...$args): void
    {
        if ($this->shutdown) {
            throw new RejectedExecutionError('ConsumeExecutor has been shut down');
        }
        // Java：入队失败（队列满）才考虑开一个非 core 线程，再不行就 reject
        $queueFull = $this->maxQueueSize !== 0 && count($this->queue) >= $this->maxQueueSize;
        $canGrow = $this->workers < $this->max;
        if ($queueFull && !$canGrow) {
            throw new RejectedExecutionError(sprintf('ConsumeExecutor queue is full (%d)', $this->maxQueueSize));
        }
        $this->queue[] = [$fn, $args];
        // Java 无界队列语义：只有 poolSize < corePoolSize 才新建线程；
        // workers == 0 是 core=0 配置下的兜底，否则任务永远没人跑。
        if ($this->workers < $this->core || ($queueFull && $canGrow) || $this->workers === 0) {
            $this->spawnLocked();
        }
    }

    /**
     * 对应 Java ``ThreadPoolExecutor.setCorePoolSize``：core 变大时按
     * ``min(delta, 队列长度)`` 补齐线程；core 变小时标量本身即并发上限，多余 worker 由
     * 空转 pump 的 keep-alive 逻辑退掉。
     */
    public function setCorePoolSize(int $n): void
    {
        if ($n < 0) {
            throw new \ValueError('core pool size must be >= 0');
        }
        $delta = $n - $this->core;
        $this->core = $n;
        if ($n > $this->max) {
            // Java 允许 core > max（会把 max 抬到 core）
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

    /** 当前存活 worker 数（Java ``getPoolSize``）。仅供观测/单测。 */
    public function workerCount(): int
    {
        return $this->workers;
    }

    /** 队列中待执行任务数（Java ``getQueue().size()``）。仅供观测/单测。 */
    public function queuedCount(): int
    {
        return count($this->queue);
    }

    /** 被吞掉的任务异常计数（仅供观测/单测）。 */
    public function handlerExceptionCount(): int
    {
        return $this->handlerExceptions;
    }

    /**
     * 执行一条排队任务（单线程驱动的 worker 主体）；队列为空时返回 false。
     * 任务异常不杀 worker（Java 同理），只计数 + 记日志。
     */
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

    /**
     * 对应 Java ``shutdown()``：停止接收新任务，把手上的队列跑完。
     *
     * ``wait=true`` 即"优雅等待"（跑完队列后把 logical worker 归零），不等价于
     * ``shutdownNow``（不中断在跑的任务）。
     */
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
        // 线程名 = <prefix><sep><序号>（Java ThreadFactoryImpl 序号从 1 开始，
        // 这里由 threadIndexFrom 决定起始，默认 0，与 Python 一致）。
        // 单线程模型下 worker 无真实线程名，seq 仅保留自增语义。
        $this->seq++;
    }
}

<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Remoting\Protocol\ConsumeStatus;

/**
 * 消费侧统计（对应 org.apache.rocketmq.client.stat.ConsumerStatsManager 与
 * org.apache.rocketmq.common.stats.{StatsItem,StatsItemSet,StatsSnapshot}），
 * 移植自 consumer_stats.py。
 *
 * Java 真实模型（5.5.1 源码逐条核对）：
 * * ``StatsItem`` 持有**累计值** value / times（只增不减），以及两个采样快照链：
 *   ``csListMinute``（每 10s 采一个累计点）与 ``csListHour``（每 10 分钟采一个累计点）；
 * * 快照计算 ``computeStatsData``：sum = last.value - first.value（窗口增量），
 *   tps = sum*1000/(last.ts-first.ts)（**每秒**，不是每分钟），
 *   avgpt = sum / (last.times - first.times)。
 *
 * PHP 单线程适配：Python 给每个 StatsItem 排 10s/10min 的调度任务 + 一个统一采样
 * 线程；这里改成由调用方驱动的 ``ConsumerStatsManager::sampleOnce()``（每次 = 一轮
 * 10s 采样，每 60 轮补一次小时级采样）。
 */

/** 对应 Java StatsSnapshot：sum / tps / avgpt / times。 */
final class StatsSnapshot
{
    public int $sum = 0;
    public float $tps = 0.0;
    public float $avgpt = 0.0;
    public int $times = 0;

    public function __toString(): string
    {
        return sprintf('StatsSnapshot(sum=%d, tps=%.2f, avgpt=%.2f, times=%d)', $this->sum, $this->tps, $this->avgpt, $this->times);
    }
}

/** 单项统计：累计 value/times + 分钟/小时两级采样链。 */
final class StatsItem
{
    private int $value = 0;
    private int $times = 0;

    /** @var list<array{0:int,1:int,2:int}> 元素 = (timestamp_ms, 累计 value, 累计 times) */
    private array $minute = [];

    /** @var list<array{0:int,1:int,2:int}> */
    private array $hour = [];

    public function __construct(
        public string $statsName,
        public string $statsKey,
    ) {
    }

    /**
     * Java ``StatsItem.computeStatsData`` 逐条照抄（StatsItem.java:53-79）。
     *
     * 元素结构 [timestamp_ms, 累计 value, 累计 times]。
     *
     * @param list<array{0:int,1:int,2:int}> $csList
     */
    public static function computeStatsData(array $csList): StatsSnapshot
    {
        $ss = new StatsSnapshot();
        if ($csList === []) {
            return $ss;
        }
        $first = $csList[0];
        $last = $csList[count($csList) - 1];
        $ss->sum = (int) ($last[1] - $first[1]);
        $spanMs = $last[0] - $first[0];
        if ($spanMs > 0) {
            $ss->tps = ($ss->sum * 1000.0) / $spanMs;
        }
        $timesDiff = (int) ($last[2] - $first[2]);
        $ss->times = $timesDiff;
        if ($timesDiff > 0) {
            $ss->avgpt = ($ss->sum * 1.0) / $timesDiff;
        }
        return $ss;
    }

    public function addValue(int $incValue, int $incTimes): void
    {
        $this->value += $incValue;
        $this->times += $incTimes;
    }

    public function getValue(): int
    {
        return $this->value;
    }

    public function getTimes(): int
    {
        return $this->times;
    }

    private function sampleData(): array
    {
        return [(int) (microtime(true) * 1000.0), $this->value, $this->times];
    }

    /** 追加分钟级采样点（每 10s 由采样线程调用）。 */
    public function sample(): void
    {
        $this->minute[] = $this->sampleData();
        while (count($this->minute) > ConsumerStatsManager::MINUTE_LIST_MAX) {
            array_shift($this->minute);
        }
    }

    /** 追加小时级采样点（每 10 分钟由采样线程调用）。 */
    public function sampleHour(): void
    {
        $this->hour[] = $this->sampleData();
        while (count($this->hour) > ConsumerStatsManager::HOUR_LIST_MAX) {
            array_shift($this->hour);
        }
    }

    public function getStatsDataInMinute(): StatsSnapshot
    {
        return self::computeStatsData($this->minute);
    }

    public function getStatsDataInHour(): StatsSnapshot
    {
        return self::computeStatsData($this->hour);
    }

    /** 分钟级采样链长度（Java ``csListMinute.size()``）。仅供观测/单测。 */
    public function getMinuteSampleCount(): int
    {
        return count($this->minute);
    }

    /** 小时级采样链长度（Java ``csListHour.size()``）。仅供观测/单测。 */
    public function getHourSampleCount(): int
    {
        return count($this->hour);
    }
}

/** key -> StatsItem（对应 Java StatsItemSet；key = topic@group）。 */
final class StatsItemSet
{
    /** @var array<string, StatsItem> */
    private array $items = [];

    public function __construct(public string $statsName)
    {
    }

    public function getAndCreate(string $key): StatsItem
    {
        $item = $this->items[$key] ?? null;
        if ($item === null) {
            $item = new StatsItem($this->statsName, $key);
            $this->items[$key] = $item;
        }
        return $item;
    }

    public function find(string $key): ?StatsItem
    {
        return $this->items[$key] ?? null;
    }

    public function addValue(string $key, int $incValue, int $incTimes): void
    {
        $this->getAndCreate($key)->addValue($incValue, $incTimes);
    }

    /** @return list<string> */
    public function keys(): array
    {
        return array_keys($this->items);
    }

    public function sampleAll(): void
    {
        foreach ($this->keys() as $key) {
            $this->find($key)?->sample();
        }
    }

    public function sampleHourAll(): void
    {
        foreach ($this->keys() as $key) {
            $this->find($key)?->sampleHour();
        }
    }
}

/**
 * 消费统计管理器（Java ConsumerStatsManager）。
 *
 * 五个 StatsItemSet，key 一律是 ``topic@group``：
 * PULL_RT / PULL_TPS / CONSUME_RT / CONSUME_OK_TPS / CONSUME_FAILED_TPS。
 */
final class ConsumerStatsManager
{
    // 采样参数（Java StatsItem.init 的 scheduleAtFixedRate 参数）
    public const SAMPLING_INTERVAL_SECONDS = 10.0;
    public const HOUR_SAMPLING_INTERVAL_SECONDS = 600.0;
    // 快照链长度（Java csListMinute 最多约 60 个点 ≈ 10 分钟窗口）
    public const MINUTE_LIST_MAX = 60;
    public const HOUR_LIST_MAX = 60;

    public StatsItemSet $topicAndGroupPullRt;
    public StatsItemSet $topicAndGroupPullTps;
    public StatsItemSet $topicAndGroupConsumeRt;
    public StatsItemSet $topicAndGroupConsumeOkTps;
    public StatsItemSet $topicAndGroupConsumeFailedTps;

    /** @var list<StatsItemSet> */
    private array $sets;

    private bool $started = false;
    private bool $stopped = false;
    private int $rounds = 0;

    public function __construct()
    {
        $this->topicAndGroupPullRt = new StatsItemSet('PULL_RT');
        $this->topicAndGroupPullTps = new StatsItemSet('PULL_TPS');
        $this->topicAndGroupConsumeRt = new StatsItemSet('CONSUME_RT');
        $this->topicAndGroupConsumeOkTps = new StatsItemSet('CONSUME_OK_TPS');
        $this->topicAndGroupConsumeFailedTps = new StatsItemSet('CONSUME_FAILED_TPS');
        $this->sets = [
            $this->topicAndGroupPullRt,
            $this->topicAndGroupPullTps,
            $this->topicAndGroupConsumeRt,
            $this->topicAndGroupConsumeOkTps,
            $this->topicAndGroupConsumeFailedTps,
        ];
    }

    // ---------------- 生命周期 ----------------

    /**
     * 对应 Java ``start()``（Java 侧也是空实现，采样挂在各 StatsItem 的调度器上）。
     * PHP 单线程不启动后台线程；采样由调用方驱动 ``sampleOnce()``。幂等。
     */
    public function start(): void
    {
        if ($this->started) {
            return;
        }
        $this->started = true;
        $this->stopped = false;
    }

    public function shutdown(): void
    {
        $this->stopped = true;
        $this->started = false;
    }

    /**
     * 采样线程一轮：分钟级采样全部 StatsItemSet；每 60 轮（60 × 10s = 10 分钟）
     * 追加一次小时级采样。对应 Python ``_sample_loop`` 的循环体。
     */
    public function sampleOnce(): void
    {
        $this->rounds++;
        foreach ($this->sets as $s) {
            $s->sampleAll();
        }
        if ($this->rounds % 60 === 0) {
            foreach ($this->sets as $s) {
                $s->sampleHourAll();
            }
        }
    }

    // ---------------- 记数（Java ConsumerStatsManager 同名方法）----------------

    private static function key(string $topic, string $group): string
    {
        return sprintf('%s@%s', $topic, $group);
    }

    public function incPullRt(string $group, string $topic, int $rt): void
    {
        $this->topicAndGroupPullRt->addValue(self::key($topic, $group), $rt, 1);
    }

    public function incPullTps(string $group, string $topic, int $msgs): void
    {
        $this->topicAndGroupPullTps->addValue(self::key($topic, $group), $msgs, 1);
    }

    public function incConsumeRt(string $group, string $topic, int $rt): void
    {
        $this->topicAndGroupConsumeRt->addValue(self::key($topic, $group), $rt, 1);
    }

    public function incConsumeOkTps(string $group, string $topic, int $msgs): void
    {
        $this->topicAndGroupConsumeOkTps->addValue(self::key($topic, $group), $msgs, 1);
    }

    public function incConsumeFailedTps(string $group, string $topic, int $msgs): void
    {
        $this->topicAndGroupConsumeFailedTps->addValue(self::key($topic, $group), $msgs, 1);
    }

    // ---------------- 查询 ----------------

    /**
     * Java ``ConsumerStatsManager.consumeStatus``：全部取 minute 快照；
     * consumeFailedMsgs 取 failed 的 **hour** 窗口 sum（Java 特意跨窗口，照抄）。
     */
    public function consumeStatus(string $group, string $topic): ConsumeStatus
    {
        $cs = new ConsumeStatus();
        $key = self::key($topic, $group);

        $ss = $this->topicAndGroupPullRt->find($key);
        if ($ss !== null) {
            $cs->pullRt = $ss->getStatsDataInMinute()->avgpt;
        }
        $ss = $this->topicAndGroupPullTps->find($key);
        if ($ss !== null) {
            $cs->pullTps = $ss->getStatsDataInMinute()->tps;
        }
        $ss = $this->topicAndGroupConsumeRt->find($key);
        if ($ss !== null) {
            $cs->consumeRt = $ss->getStatsDataInMinute()->avgpt;
        }
        $ss = $this->topicAndGroupConsumeOkTps->find($key);
        if ($ss !== null) {
            $cs->consumeOkTps = $ss->getStatsDataInMinute()->tps;
        }
        $ss = $this->topicAndGroupConsumeFailedTps->find($key);
        if ($ss !== null) {
            $cs->consumeFailedTps = $ss->getStatsDataInMinute()->tps;
            $cs->consumeFailedMsgs = $ss->getStatsDataInHour()->sum;
        }

        return $cs;
    }
}

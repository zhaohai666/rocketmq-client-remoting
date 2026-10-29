<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Remoting\Protocol\ConsumeStatus;


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

/** Single stat item: cumulative value/times + minute/hour two-tier sampling chain. */
final class StatsItem
{
    private int $value = 0;
    private int $times = 0;

    /** @var list<array{0:int,1:int,2:int}> element = (timestamp_ms, cumulative value, cumulative times) */
    private array $minute = [];

    /** @var list<array{0:int,1:int,2:int}> */
    private array $hour = [];

    /** @var list<array{0:int,1:int,2:int}> */
    private array $day = [];

    public function __construct(
        public string $statsName,
        public string $statsKey,
    ) {
    }

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

    /** Append a minute-level sample point (called by the sampling thread every 10s). */
    public function sample(): void
    {
        if ($this->minute === []) {
            // Seed a zero snapshot one interval back, so the first window covers the values
            // accumulated since startup instead of reporting a zero delta.
            $this->minute[] = [(int) (microtime(true) * 1000.0) - 10000, 0, 0];
        }
        $this->minute[] = $this->sampleData();
        while (count($this->minute) > ConsumerStatsManager::MINUTE_LIST_MAX) {
            array_shift($this->minute);
        }
    }

    /** Append an hour-level sample point (called by the sampling thread every 10 minutes). */
    public function sampleHour(): void
    {
        if ($this->hour === []) {
            $this->hour[] = [(int) (microtime(true) * 1000.0) - 600000, 0, 0];
        }
        $this->hour[] = $this->sampleData();
        while (count($this->hour) > ConsumerStatsManager::HOUR_LIST_MAX) {
            array_shift($this->hour);
        }
    }

    /** Append a day-level sample point (called by the sampling thread every hour). */
    public function sampleDay(): void
    {
        if ($this->day === []) {
            $this->day[] = [(int) (microtime(true) * 1000.0) - 3600000, 0, 0];
        }
        $this->day[] = $this->sampleData();
        while (count($this->day) > ConsumerStatsManager::DAY_LIST_MAX) {
            array_shift($this->day);
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

    public function getStatsDataInDay(): StatsSnapshot
    {
        return self::computeStatsData($this->day);
    }

    public function getMinuteSampleCount(): int
    {
        return count($this->minute);
    }

    public function getHourSampleCount(): int
    {
        return count($this->hour);
    }

    public function getDaySampleCount(): int
    {
        return count($this->day);
    }
}

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

    public function sampleDayAll(): void
    {
        foreach ($this->keys() as $key) {
            $this->find($key)?->sampleDay();
        }
    }
}

final class ConsumerStatsManager
{
    public const SAMPLING_INTERVAL_SECONDS = 10.0;
    public const HOUR_SAMPLING_INTERVAL_SECONDS = 600.0;
    public const DAY_SAMPLING_INTERVAL_SECONDS = 3600.0;
    /** Minute window: one sample every 10s, capped at 7 points (6 intervals plus the seed). */
    public const MINUTE_LIST_MAX = 7;
    /** Hour window: one sample every 10min, capped at 7 points. */
    public const HOUR_LIST_MAX = 7;
    /** Day window: one sample every hour, capped at 25 points. */
    public const DAY_LIST_MAX = 25;

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

    // ---------------- Lifecycle ----------------

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
     * One round of the sampling thread: minute-level sampling of all StatsItemSets; every
     * 60 rounds (60 x 10s = 10 minutes) an hour-level sample is appended, and every 360
     * rounds (1 hour) a day-level sample (corresponding to the loop bodies of the
     * sampling schedule).
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
        if ($this->rounds % 360 === 0) {
            foreach ($this->sets as $s) {
                $s->sampleDayAll();
            }
        }
    }


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

    // ---------------- Query ----------------

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

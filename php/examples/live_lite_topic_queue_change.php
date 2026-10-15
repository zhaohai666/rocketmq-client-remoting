<?php

declare(strict_types=1);

// PHP-side LitePull consumer live verification of topic queue-set change listening.
//
//   php examples/live_lite_topic_queue_change.php [namesrv]
//
// Why this must be a live run: the comparison pass asks the nameserver for the subscribed queue
// set fresh on every pass, while the ordinary route cache refreshes only every 30s. A fake
// nameserver can prove the comparison logic; only a real cluster can prove the "fresh lookup".
// Here the check interval is pushed down to the 1s floor, the route polling keeps the default
// 30s, and the topic is actually scaled: from the nameserver reporting the new queue count to
// the listener's callback only one or two check passes should pass (<=5s); reading the 30s cache
// would drag this window past half a minute.
//
// PHP adaptation: no scheduler thread, the comparison is caller-driven — the main loop ticks()
// first then polls() each round; only the pass due inside tick() performs the comparison.
//
//   L1  queues untouched => the listener is not disturbed
//   L2  scale up 2->4: the callback follows within a few passes once the nameserver reports it
//   L3  after the callback the snapshot advances => the same queue set does not re-callback
//   L4  scale down 4->2: likewise observed via the fresh lookup
//   L5  a never-created topic: failing to fetch queues counts as "not found", not disguised as
//       scaling to 0 queues
//
// Closing line `PASS=<n> FAIL=<n>`; exits non-zero on any FAIL.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQProducer;

// Keep the log out of the user's $HOME: the default is logs/ under the current working
// directory; pin it to the temp directory here.
if (getenv('ROCKETMQ_CLIENT_LOG_DIR') === false) {
    putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_qc_live_logs');
}

$pass = 0;
$fail = 0;
$check = function (bool $ok, string $name, string $detail = '') use (&$pass, &$fail): void {
    echo ($ok ? '  [PASS] ' : '  [FAIL] ') . $name . ($detail !== '' ? '  ' . $detail : '') . PHP_EOL;
    if ($ok) {
        $pass++;
    } else {
        $fail++;
    }
};

$ns = $argv[1] ?? '127.0.0.1:9876';
$stamp = (int)(microtime(true) * 1000);
$topic = 'PhpLiteQcLive_' . $stamp;
$group = 'GID_PhpLiteQcLive_' . $stamp;
$ghost = 'PhpLiteQcGhost_' . $stamp;

const BASE_QUEUES = 2;
const SCALED_QUEUES = 4;
// Push the check interval to the floor so "fresh lookup per pass" and "eating the 30s cache"
// are separated in time.
const CHECK_INTERVAL_MS = 1000;
// Maximum allowed callback gap after the nameserver sees the change.
const FRESH_WINDOW_MS = 5000;

echo "======================================================================" . PHP_EOL;
echo "LitePull queue-change live (PHP): ns=$ns topic=$topic group=$group" . PHP_EOL;
echo "======================================================================" . PHP_EOL;

/** Issues the create-topic request once more for the same topic, changing the read/write queue counts to $queueNum. */
$scale = static function (int $queueNum) use ($ns, $topic): void {
    $prod = new DefaultMQProducer('PG_PhpQc_' . bin2hex(random_bytes(4)));
    $prod->setNamesrvAddr($ns);
    $prod->start();
    try {
        $prod->createTopic('TBW102', $topic, $queueNum);
    } catch (\Throwable $e) {
        echo "!! createTopic($queueNum) failed: " . $e->getMessage() . PHP_EOL;
    }
    $prod->shutdown();
};

/** Records callbacks only; the comparison runs in the same thread, so no locking is needed. */
final class QcRecorder
{
    /** @var list<array{0:string,1:list<int>}> */
    public array $events = [];

    public function onChanged(string $topic, array $queues): void
    {
        $ids = array_map(static fn ($mq): int => $mq->queueId, $queues);
        sort($ids);
        $this->events[] = [$topic, $ids];
    }

    public function count(): int
    {
        return count($this->events);
    }

    public function idsAt(int $i): string
    {
        return isset($this->events[$i]) ? implode(',', $this->events[$i][1]) : '';
    }
}

/**
 * Drives the consumer for a while: each round ticks() first (only the due pass compares) then
 * polls(). Returns the milliseconds actually driven; returns early if cond becomes true.
 */
$drive = static function (DefaultLitePullConsumer $c, int $millis, ?callable $cond = null): int {
    $waited = 0;
    while ($waited < $millis) {
        $c->tick();
        $c->poll(200);
        $waited += 200;
        if ($cond !== null && $cond() === true) {
            return $waited;
        }
    }
    return $waited;
};

/** Fresh-lookups the queue set until it reports $want; returns the waited milliseconds, -1 on timeout. */
$waitQueueNum = static function (DefaultLitePullConsumer $c, string $t, int $want, int $timeoutMs): int {
    $waited = 0;
    while ($waited < $timeoutMs) {
        try {
            if (count($c->fetchMessageQueues($t)) === $want) {
                return $waited;
            }
        } catch (\Throwable) {
            // Route not updated yet, keep waiting
        }
        usleep(250000);
        $waited += 250;
    }
    return -1;
};

$scale(BASE_QUEUES);

$c = new DefaultLitePullConsumer($group);
$c->setNamesrvAddr($ns);
$c->instanceName = 'php-lite-qc-live';
$c->setTopicMetadataCheckIntervalMillis(CHECK_INTERVAL_MS);
$check($c->topicMetadataCheckIntervalMillis === CHECK_INTERVAL_MS,
    'L0 check interval pushed down to the 1s floor', (string)$c->topicMetadataCheckIntervalMillis);
$c->subscribe($topic, '*');
$c->start();

$visible = $waitQueueNum($c, $topic, BASE_QUEUES, 30000);
$check($visible >= 0, 'L0 route visible (2 queues)', $visible . 'ms');

// Registering while running => snapshot taken immediately => quiet as long as queues are
// untouched (the tick()-driven pass is equally undisturbed)
$rec = new QcRecorder();
$c->registerTopicMessageQueueChangeListener($topic, $rec);
$drive($c, 3000);
$check($rec->count() === 0, 'L1 queues untouched => silent (the snapshot eats every pass)', 'events=' . $rec->count());

// Schedule the first check for now: PHP has no threads, the first-check delay is just a
// timestamp on nextMetadataCheckAt.
$c->nextMetadataCheckAt = microtime(true);
$drive($c, 1000);
$check($rec->count() === 0, 'L1b the due pass really ran => still silent', 'events=' . $rec->count());

// ---- L2 scale up 2->4
$scale(SCALED_QUEUES);
$nsMs = $waitQueueNum($c, $topic, SCALED_QUEUES, 45000);
$check($nsMs >= 0, 'L2a the nameserver reports 4 queues', $nsMs . 'ms');
if ($nsMs >= 0) {
    $driven = $drive($c, FRESH_WINDOW_MS, static fn (): bool => $rec->count() >= 1);
    $check($rec->count() >= 1 && $rec->idsAt(0) === '0,1,2,3',
        'L2b the comparison pass fresh-lookups the route (the callback follows the nameserver, not the 30s cache)',
        $driven . 'ms, ids=' . $rec->idsAt(0));
}

// ---- L3 snapshot advance
$drive($c, 3000);
$check($rec->count() === 1, 'L3 the snapshot advances after the callback => no repeated callback', 'events=' . $rec->count());

// ---- L4 scale down 4->2
$scale(BASE_QUEUES);
$nsMs = $waitQueueNum($c, $topic, BASE_QUEUES, 45000);
$check($nsMs >= 0, 'L4a the nameserver reports back 2 queues', $nsMs . 'ms');
if ($nsMs >= 0) {
    $driven = $drive($c, FRESH_WINDOW_MS, static fn (): bool => $rec->count() >= 2);
    $check($rec->count() >= 2 && $rec->idsAt(1) === '0,1',
        'L4b the scale-down is likewise seen via the fresh route lookup', $driven . 'ms, ids=' . $rec->idsAt(1));
}

// ---- L5 unknown topic: an empty queue set counts as "not found"
$raised = '';
try {
    $raised = 'no exception, queues=' . count($c->fetchMessageQueues($ghost));
} catch (\Throwable $e) {
    $raised = $e->getMessage();
}
$check(str_contains($raised, 'Namesrv return empty') || str_contains($raised, 'Can not find'),
    'L5 fetching queues of an unknown topic throws "not found" instead of returning an empty set', $raised);

$c->shutdown();

echo PHP_EOL . 'LitePull queue-change live (PHP): PASS=' . $pass . ' FAIL=' . $fail . PHP_EOL;
exit($fail === 0 ? 0 : 1);

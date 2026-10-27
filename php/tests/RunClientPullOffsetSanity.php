<?php

declare(strict_types=1);

/**
 * Pull-result offset sanity warning suite (no phpunit, no real cluster).
 *
 * Run: php tests/RunClientPullOffsetSanity.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * On a FOUND round the push-consumer pull path remembers the cursor it requested with
 * (``prevRequestOffset``), then advances the cursor to ``nextBeginOffset``. At the end of handling that
 * round -- after the cursor advanced and after the batch was handed to the consume side -- the client
 * logs one WARN line when either leg holds:
 *
 *   - ``nextBeginOffset < prevRequestOffset``, or
 *   - ``firstMsgOffset < prevRequestOffset``,
 *
 * where ``firstMsgOffset`` is the queue offset of the first row of the list the round actually hands
 * over, after this port's own pre-delivery filter; a round whose list is empty carries ``PHP_INT_MAX``
 * there, so the second leg cannot fire without a row.
 *
 * The warning is purely diagnostic: it changes no offset, no pacing and no stats. The last leg pins
 * that by driving the warning round beside an identical consistent round and asserting the stored
 * cursor and the cadence verdict come out the same on both.
 *
 * As in the cadence suite, each leg drives one round against an instance double that answers as
 * scripted; the warning itself is captured through the client logger's test hook instead of read off a
 * log file.
 */

require_once __DIR__ . '/../bootstrap.php';
// message_queue_key() lives in a functions-only include: require it rather than depend on some
// earlier class load pulling the file in by accident.
require_once __DIR__ . '/../src/Remoting/Protocol/AdminBodies.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');
// Belt for the brace: no round here should log outside the captured handler, but keep any stray
// default-tier line out of the repository all the same.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_pos_logs_' . getmypid());

use RocketMQ\Client\ConsumerStatsManager;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Logger;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\PullResult;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\UtilAll;
use function RocketMQ\Remoting\Protocol\message_queue_key;

/** Instance double: one scripted pull answer, and a record of every request that left the client. */
final class PosInstance extends MQClientInstance
{
    /** @var list<int> one entry per pull request that went out */
    public array $sent = [];

    public ?PullResult $answer = null;

    public function pullMessage(
        string $consumerGroup,
        MessageQueue $mq,
        int $queueOffset,
        int $maxMsgNums,
        int $sysFlag,
        int $commitOffset,
        ?string $subscription,
        int $subVersion,
        ?string $expressionType,
        int $timeoutMillis = 30000,
        int $maxMsgBytes = -1,
        int $suspendTimeoutMillis = 15000,
        ?string $addr = null,
        int $requestSource = 0,
        ?int $brokerId = null,
    ): PullResult {
        $this->sent[] = $queueOffset;
        return $this->answer ?? new PullResult(PullStatus::NO_NEW_MSG, nextBeginOffset: $queueOffset);
    }
}

final class RunClientPullOffsetSanity
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    public function check(bool $condition, string $label): void
    {
        if ($condition) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: $label\n");
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, sprintf(
            "FAIL: %s\n  expected: %s\n  actual:   %s\n",
            $label,
            var_export($expected, true),
            var_export($actual, true),
        ));
    }

    public function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }

    // ---------------- reflection helpers ----------------

    private function prop(object $o, string $name): mixed
    {
        $p = new ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        return $p->getValue($o);
    }

    private function setProp(object $o, string $name, mixed $value): void
    {
        $p = new ReflectionProperty(DefaultMQPushConsumer::class, $name);
        $p->setAccessible(true);
        $p->setValue($o, $value);
    }

    private function call(object $o, string $name, array $args): mixed
    {
        $m = new ReflectionMethod(DefaultMQPushConsumer::class, $name);
        $m->setAccessible(true);
        return $m->invoke($o, ...$args);
    }

    private function key(MessageQueue $mq): string
    {
        return (string) message_queue_key($mq);
    }

    // ---------------- consumers wired for one round ----------------

    /**
     * One pull queue wired for a round, its cursor parked at ``$startOffset`` (the request point every
     * leg below judges the reply against). No cadence: a delivered round answers its verdict directly.
     *
     * @return array{0:DefaultMQPushConsumer,1:MessageQueue,2:string,3:PosInstance}
     */
    private function pullQueue(string $group, string $topic, int $startOffset): array
    {
        $inst = new PosInstance('pos-' . uniqid(), []);
        $c = new DefaultMQPushConsumer($group);
        $c->subscribe($topic, '*');
        $mq = new MessageQueue($topic, 'b-pos', 0);
        $key = $this->key($mq);
        $c->mqClient = $inst;
        $this->setProp($c, 'mqMap', [$key => $mq]);
        $this->setProp($c, 'offsetTable', [$key => $startOffset]);
        $this->setProp($c, 'statsManager', new ConsumerStatsManager());
        $this->setProp($c, 'started', true);
        return [$c, $mq, $key, $inst];
    }

    /** A row as the pull path hands it over: the pull confirms by offset, so no ack bookkeeping. */
    private function row(string $topic, int $offset): MessageExt
    {
        $m = new MessageExt(topic: $topic, body: 'pos-' . $offset);
        $m->queueId = 0;
        $m->queueOffset = $offset;
        $m->brokerName = 'b-pos';
        $m->msgId = sprintf('POS-%d', $offset);
        $m->bornTimestamp = UtilAll::currentTimeMillis();
        return $m;
    }

    /** @param list<MessageExt> $rows */
    private function found(string $topic, int $nextOffset, array $rows): PullResult
    {
        return new PullResult(
            PullStatus::FOUND,
            nextBeginOffset: $nextOffset,
            minOffset: 0,
            maxOffset: $nextOffset,
            msgFoundList: $rows,
        );
    }

    /** @return list<int> the queue offsets the buffered rows carry, in order */
    private function pendingOffsets(DefaultMQPushConsumer $c, string $key): array
    {
        return array_map(
            static fn(MessageExt $m): int => $m->queueOffset,
            (array) (($this->prop($c, 'pending')[$key] ?? [])),
        );
    }

    /**
     * Drive exactly one round whose request the double answers with ``$answer``, capturing every WARN
     * line the round logged through the client logger's test hook.
     *
     * @return array{0:bool,1:?int,2:list<int>,3:int,4:list<string>}
     *   verdict, stored cursor, buffered row offsets, requests sent, warning lines
     */
    private function round(DefaultMQPushConsumer $c, MessageQueue $mq, string $key, PullResult $answer): array
    {
        /** @var PosInstance $inst */
        $inst = $c->mqClient;
        $this->setProp($c, 'idleUntil', []);
        $this->setProp($c, 'flowControlUntil', []);
        $this->setProp($c, 'suspendedUntil', []);
        $inst->answer = $answer;
        $inst->sent = [];

        /** @var list<string> $captured */
        $captured = [];
        Logger::setHandler(static function (string $line) use (&$captured): void {
            $captured[] = $line;
        });
        try {
            $verdict = (bool) $this->call($c, 'pullOnce', [$mq, $key, false, microtime(true)]);
        } finally {
            Logger::setHandler(null);
        }
        // Logger renders a warning as "[<date>] WARNING: <message>".
        $warnings = array_values(array_filter(
            $captured,
            static fn(string $l): bool => str_contains($l, '] WARNING: '),
        ));
        $stored = (array) $this->prop($c, 'offsetTable');
        return [$verdict, $stored[$key] ?? null, $this->pendingOffsets($c, $key), count($inst->sent), $warnings];
    }

    /** Does this warning line carry all three numbers of the round? */
    private function carries(string $line, int $prev, int $next, int $first): bool
    {
        return str_contains($line, "requested at $prev")
            && str_contains($line, "nextBeginOffset=$next")
            && str_contains($line, "firstMsgOffset=$first");
    }

    // ================================================================ 1. the pure predicate, both legs

    private function testThePredicateFlagsExactlyTheTwoLegs(): void
    {
        $this->checkSame(true, DefaultMQPushConsumer::pullOffsetsLookWrong(9, PHP_INT_MAX, 10),
            'a cursor below the request point is the first leg');
        $this->checkSame(true, DefaultMQPushConsumer::pullOffsetsLookWrong(12, 11, 12),
            'a first row below the request point is the second leg even when the cursor is consistent');
        $this->checkSame(false, DefaultMQPushConsumer::pullOffsetsLookWrong(12, 10, 10),
            'a cursor and a first row at or above the request point look fine');
        $this->checkSame(false, DefaultMQPushConsumer::pullOffsetsLookWrong(10, 10, 10),
            'CONTROL: exactly at the request point is not "below" it');
        $this->checkSame(false, DefaultMQPushConsumer::pullOffsetsLookWrong(10, PHP_INT_MAX, 10),
            'the empty-list sentinel never fires the row leg, whatever the cursor is');
    }

    // ================================================================ 2. a FOUND round warns, once, in a line

    /** nextBeginOffset below the request point: one warning, carrying all three numbers. */
    private function testBackwardCursorWarnsOnce(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-POS-CURSOR', 'PosCursor', 10);
        [$verdict, $stored, $pending, $requests, $warnings] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PosCursor', 9, [$this->row('PosCursor', 10)]),
        );
        $this->checkSame(1, count($warnings), 'a cursor behind the request point logs exactly one warning');
        $this->check(isset($warnings[0]) && $this->carries($warnings[0], 10, 9, 10),
            'and the single line carries all three numbers (' . ($warnings[0] ?? 'none') . ')');
        $this->checkSame(true, $verdict, 'CONTROL: the warned round is still a delivered round');
        $this->checkSame(9, $stored, 'CONTROL: the cursor advanced to the reply as always');
        $this->checkSame([10], $pending, 'CONTROL: the row was handed over all the same');
        $this->checkSame(1, $requests, 'CONTROL: the round sent its one request');
    }

    /** firstMsgOffset below the request point while the cursor is consistent: the second leg warns. */
    private function testBackwardFirstRowWarnsOnce(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-POS-ROW', 'PosRow', 10);
        [, , , , $warnings] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PosRow', 12, [$this->row('PosRow', 9), $this->row('PosRow', 10)]),
        );
        $this->checkSame(1, count($warnings), 'a first row behind the request point logs exactly one warning');
        $this->check(isset($warnings[0]) && $this->carries($warnings[0], 10, 12, 9),
            'and the line reads the consistent cursor against the row that is not ('
            . ($warnings[0] ?? 'none') . ')');
    }

    // ================================================================ 3. the rounds that look fine

    /** A consistent FOUND round logs nothing at all. */
    private function testConsistentRoundLogsNothing(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-POS-CONSISTENT', 'PosConsistent', 10);
        [$verdict, $stored, $pending, $requests, $warnings] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PosConsistent', 12, [$this->row('PosConsistent', 10), $this->row('PosConsistent', 11)]),
        );
        $this->checkSame(0, count($warnings), 'a cursor and a first row at or above the request point log nothing');
        $this->checkSame(true, $verdict, 'CONTROL: the consistent round is a delivered round');
        $this->checkSame(12, $stored, 'CONTROL: and it stored its cursor');
        $this->checkSame([10, 11], $pending, 'CONTROL: and handed its rows over');
        $this->checkSame(1, $requests, 'CONTROL: and sent its one request');
    }

    /**
     * A FOUND round with no rows carries the PHP_INT_MAX sentinel, so the row leg cannot fire even when
     * the cursor matches the request point; the empty round stays an empty round.
     */
    private function testEmptyFoundRoundNeverFiresTheRowLeg(): void
    {
        [$c, $mq, $key] = $this->pullQueue('GID-POS-EMPTY', 'PosEmpty', 10);
        [$verdict, $stored, $pending, $requests, $warnings] = $this->round(
            $c,
            $mq,
            $key,
            $this->found('PosEmpty', 10, []),
        );
        $this->checkSame(0, count($warnings), 'an empty FOUND round with a consistent cursor logs nothing');
        $this->checkSame(false, $verdict, 'CONTROL: the empty round stays an empty round for the pacing');
        $this->checkSame(10, $stored, 'CONTROL: and it still advanced its cursor');
        $this->checkSame([], $pending, 'CONTROL: and buffered nothing');
        $this->checkSame(1, $requests, 'CONTROL: and sent its one request');
    }

    // ================================================================ 4. diagnostic only

    /**
     * The warning changes nothing: the warned round and an identical consistent round store the same
     * cursor and reach the same cadence verdict. Both legs are asserted, so a check that grew teeth --
     * clamping the cursor or re-pacing the round -- reddens here.
     */
    private function testTheWarningChangesNeitherCursorNorVerdict(): void
    {
        [$cBad, $mqBad, $keyBad] = $this->pullQueue('GID-POS-PAIR-BAD', 'PosPair', 10);
        [$verdictBad, $storedBad, $pendingBad, $requestsBad, $warningsBad] = $this->round(
            $cBad,
            $mqBad,
            $keyBad,
            $this->found('PosPair', 12, [$this->row('PosPair', 9), $this->row('PosPair', 10)]),
        );

        [$cGood, $mqGood, $keyGood] = $this->pullQueue('GID-POS-PAIR-GOOD', 'PosPair', 10);
        [$verdictGood, $storedGood, $pendingGood, $requestsGood, $warningsGood] = $this->round(
            $cGood,
            $mqGood,
            $keyGood,
            $this->found('PosPair', 12, [$this->row('PosPair', 10), $this->row('PosPair', 11)]),
        );

        $this->checkSame(1, count($warningsBad), 'the warning leg: the off round logged its one line');
        $this->checkSame(0, count($warningsGood), 'the control leg: the identical consistent round logged none');
        $this->checkSame($storedGood, $storedBad, 'both rounds store the same cursor despite the warning');
        $this->checkSame($verdictGood, $verdictBad, 'both rounds reach the same cadence verdict');
        $this->checkSame(2, count($pendingBad), 'the warned round handed its two rows over like any delivered round');
        $this->checkSame(count($pendingGood), count($pendingBad),
            'and the same number of rows on both legs of the pair');
        $this->checkSame($requestsGood, $requestsBad, 'both legs sent exactly the same requests');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testThePredicateFlagsExactlyTheTwoLegs();
        $this->testBackwardCursorWarnsOnce();
        $this->testBackwardFirstRowWarnsOnce();
        $this->testConsistentRoundLogsNothing();
        $this->testEmptyFoundRoundNeverFiresTheRowLeg();
        $this->testTheWarningChangesNeitherCursorNorVerdict();
        return $this->summary();
    }
}

exit((new RunClientPullOffsetSanity())->run());

<?php

declare(strict_types=1);

// SQL92 filtering + CHECK_CLIENT_CONFIG(46) verified against a **real 5.5.1 cluster**.
//
//   php examples/live_sql92.php [namesrv]
//   e.g.: php examples/live_sql92.php 127.0.0.1:9876
//
// Why this must run on a real cluster: the SQL92 path fails silently more easily than any
// other. When the broker's `ExpressionMessageFilter` cannot get the compiled filter data at
// the ConsumeQueue stage it **admits all messages**, so both misconfigurations show up as
// "the consumer starts normally and all messages arrive": the broker has
// `enablePropertyFilter` off (the expression is never compiled), or the expression has a
// syntax error. Offline tests can pin the protocol shape but cannot prove the broker really
// filters by property. All four sections verify here:
//
//   preflight  the broker accepts SQL92 subscriptions at all (enablePropertyFilter=true);
//              when it refuses, the probe prints the actionable fix and exits 0 (skip),
//              the same contract as nodeJs/examples/live_check_client_config.ts
//   S1         a SQL92 subscription triggers exactly one CHECK_CLIENT_CONFIG(46), and the
//              body is a CheckClientRequestBody (clientId / group / subscriptionData); a
//              pure TAG subscription sends no 46 at all (isTagType short-circuits)
//   S2 / S3    the broker really filters by property: color='red' gets exactly the 3 red
//              messages, the never-matching color='green' gets none, and the TAG '*'
//              control group gets all 6 (red + blue)
//   S4         an illegal expression fails during startup with the broker's
//              SUBSCRIPTION_PARSE_FAILED(23), immediately (<5s, no timeout fallback)
//
// PHP is single-threaded: every wait window actively ticks the consumers in a polling loop
// (the same approach as examples/live_acl_message_types.php).
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\MessageSelector;
use RocketMQ\Client\SendStatus;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Common\ExpressionType;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageExt;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\RPCHook;

$ns = $argv[1] ?? '127.0.0.1:9876';
$stamp = (string) intval(microtime(true) * 1000);
$topic = 'Sql92Php_' . $stamp;
$group = 'GID_Sql92Php_' . $stamp;

$pass = 0;
$fail = 0;

function check(bool $ok, string $name, string $detail = ''): void
{
    global $pass, $fail;
    if ($ok) {
        $pass++;
        echo '  PASS  ' . $name . PHP_EOL;
    } else {
        $fail++;
        echo '  FAIL  ' . $name . (($detail !== '') ? ('  <- ' . $detail) : '') . PHP_EOL;
    }
}

/** Polls the condition within the window; ticks the passed consumers each round (PHP has no background threads). */
function wait_until(float $windowSec, callable $cond, object ...$tickables): bool
{
    $deadline = microtime(true) + $windowSec;
    while (true) {
        foreach ($tickables as $t) {
            if (method_exists($t, 'tick')) {
                $t->tick();
            }
        }
        if ($cond()) {
            return true;
        }
        if (microtime(true) >= $deadline) {
            return false;
        }
        usleep(100_000);
    }
}

/** Walks the exception chain looking for the broker response code. */
function broker_code(Throwable $e): ?int
{
    $cur = $e;
    while ($cur !== null) {
        if (property_exists($cur, 'responseCode') && $cur->responseCode !== null) {
            return (int) $cur->responseCode;
        }
        $cur = $cur->getPrevious();
    }
    return null;
}

/** Captures the code-46 requests really sent during startup (with the body). */
final class Check46Probe extends RPCHook
{
    /** @var list<int> */
    public array $codes = [];
    /** @var list<array<string,mixed>> */
    public array $bodies = [];

    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        $this->codes[] = $request->code;
        if ($request->code === RequestCode::CHECK_CLIENT_CONFIG && $request->body !== null && $request->body !== '') {
            $this->bodies[] = json_decode($request->body, true) ?: [];
        }
    }

    public function checkCount(): int
    {
        return count(array_filter($this->codes, static fn (int $c): bool => $c === RequestCode::CHECK_CLIENT_CONFIG));
    }
}


/**
 * Captures the subscription cells the PULL / LITE_PULL requests really carried to the broker.
 *
 * The cells are read off the request header, not ext_fields: the hook runs before encoding and
 * the encoder is what copies a non-null header field into ext_fields, so at capture time the
 * header is the only place the subscription exists.
 */
final class PullWireProbe extends RPCHook
{
    /** @var list<array<string, string>> */
    public array $rows = [];

    public function doBeforeRequest(string $remoteAddr, RemotingCommand $request): void
    {
        if ($request->code === RequestCode::PULL_MESSAGE
            || $request->code === RequestCode::LITE_PULL_MESSAGE) {
            $this->rows[] = [
                'code' => (string) $request->code,
                'topic' => self::cell($request, 'topic'),
                'subscription' => self::cell($request, 'subscription'),
                'expressionType' => self::cell($request, 'expressionType'),
                'subVersion' => self::cell($request, 'subVersion'),
                'sysFlag' => self::cell($request, 'sysFlag'),
            ];
        }
    }

    private static function cell(RemotingCommand $request, string $name): string
    {
        $header = $request->customHeader;
        if ($header !== null) {
            $fields = get_object_vars($header);
            if (isset($fields[$name]) && $fields[$name] !== '') {
                return (string) $fields[$name];
            }
        }
        return (string) ($request->extFields[$name] ?? '');
    }

    /** Last recorded request for this topic, or null. */
    public function last(string $topic): ?array
    {
        foreach (array_reverse($this->rows) as $r) {
            if ($r['topic'] === $topic) {
                return $r;
            }
        }
        return null;
    }
}

/** Collects the consumed messages. */
final class Box
{
    /** @var list<MessageExt> */
    public array $msgs = [];

    public function add(MessageExt $m): void
    {
        $this->msgs[] = $m;
    }

    /** @return list<string> */
    public function bodies(): array
    {
        return array_map(static fn (MessageExt $m): string => (string) $m->getBody(), $this->msgs);
    }

    /** @return list<string> */
    public function sortedBodies(): array
    {
        $b = $this->bodies();
        sort($b, SORT_STRING);
        return $b;
    }
}

final class CollectListener implements MessageListenerConcurrently
{
    public function __construct(private readonly Box $box)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $ctx): ConsumeConcurrentlyStatus
    {
        foreach ($msgs as $m) {
            $this->box->add($m);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
}

/**
 * Builds a push consumer on this topic (fresh group per consumer; CONSUME_FROM_FIRST_OFFSET
 * makes it immune to historical offsets). When probe is non-None the RPC hook gathers wire
 * evidence.
 *
 * @return array{0: DefaultMQPushConsumer, 1: Box}
 */
function new_consumer(string $suffix, ?MessageSelector $selector = null, string $expression = '*', ?Check46Probe $probe = null, ?string $topicOverride = null): array
{
    global $ns, $group, $topic, $stamp;
    $useTopic = $topicOverride ?? $topic;
    $c = new DefaultMQPushConsumer($group . '_' . $suffix);
    $c->setNamesrvAddr($ns);
    $c->setInstanceName('sql92php-' . $suffix . '-' . $stamp);
    $c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    // Single-threaded tick polls queues in order: suspended long polls on idle queues would
    // burn the whole window, so both ends are shortened.
    $c->pullSuspendTimeoutMillis = 1000;
    $c->pullTimeoutMillis = 3000;
    if ($probe !== null) {
        $c->rpcHook = $probe;
    }
    if ($selector !== null) {
        $c->subscribeWithSelector($useTopic, $selector);
    } else {
        $c->subscribe($useTopic, $expression);
    }
    $box = new Box();
    $c->setMessageListener(new CollectListener($box));
    return [$c, $box];
}

echo 'SQL92 / CHECK_CLIENT_CONFIG live check on ' . $ns . PHP_EOL;

$admin = new DefaultMQAdminExt();
$admin->setNamesrvAddr($ns);
$admin->start();
$brokerName = 'broker-a';
try {
    $ci = $admin->fetchBrokerClusterInfo();
    $names = array_keys($ci->brokerAddrTable);
    sort($names, SORT_STRING);
    $brokerName = $names[0] ?? 'broker-a';
} catch (Throwable $e) {
    echo '!! fetchBrokerClusterInfo failed: ' . $e->getMessage() . PHP_EOL;
    $admin->shutdown();
    exit(1);
}

$prod = new DefaultMQProducer($group . '_P');
$prod->setNamesrvAddr($ns);
$prod->start();
$consumers = [];
try {
    $admin->createTopic($brokerName, $topic, 4);
    $routeReady = wait_until(20.0, function () use ($admin, $topic): bool {
        try {
            $r = $admin->examineTopicRoute($topic);
        } catch (Throwable) {
            return false;
        }
        $n = 0;
        foreach ($r->queueDatas as $q) {
            $n += $q->readQueueNums;
        }
        return $n >= 4;
    });
    check($routeReady, 'T0 the topic route is ready (4 queues)');

    // ---------- preflight: does this broker accept SQL92 subscriptions at all? ----------
    // The broker answers CHECK_CLIENT_CONFIG(46) with SYSTEM_ERROR ("The broker does not
    // support consumer to filter message by SQL92") unless it runs with
    // enablePropertyFilter=true, and the Java client fails start() the same way. Detect that
    // up front and say what to do, instead of dying with a raw error inside S1.
    [$pre, ] = new_consumer('PREFLIGHT', MessageSelector::bySql("color = 'red'"));
    try {
        $pre->start();
        $pre->shutdown();
        echo 'preflight OK: the broker accepts SQL92 subscriptions (enablePropertyFilter=true)' . PHP_EOL;
    } catch (MQClientException $e) {
        if (str_contains($e->getMessage(), 'does not support consumer to filter message by')) {
            echo '!! the broker refuses SQL92 subscriptions: ' . $e->getMessage() . PHP_EOL;
            echo '!! fix: set enablePropertyFilter=true in broker.conf and restart the broker,' . PHP_EOL;
            echo '!!      then re-run this probe (the same requirement applies to the Java client).' . PHP_EOL;
            echo '!! skipping end-to-end SQL92 verification: broker not configured for property filter' . PHP_EOL;
            $prod->shutdown();
            $admin->shutdown();
            exit(0);
        }
        throw $e;
    }

    // ---------- S1 live evidence: the shape of the code-46 request ----------
    echo PHP_EOL . '---------- S1 the code-46 request during startup ----------' . PHP_EOL;
    $probe = new Check46Probe();
    [$c1, ] = new_consumer('SQL', MessageSelector::bySql("color = 'red'"), probe: $probe);
    $consumers[] = $c1;
    $c1->start();
    check($probe->checkCount() === 1, 'the SQL92 subscription triggers exactly one CHECK_CLIENT_CONFIG(46)',
        'codes=' . implode(',', $probe->codes));
    $body = $probe->bodies[0] ?? [];
    $sd = $body['subscriptionData'] ?? [];
    check(
        ($body['group'] ?? null) === $group . '_SQL'
        && ($body['clientId'] ?? null) === $c1->clientId
        && ($sd['expressionType'] ?? null) === ExpressionType::SQL92
        && ($sd['subString'] ?? null) === "color = 'red'"
        && ($sd['topic'] ?? null) === $topic,
        'the body is a CheckClientRequestBody (clientId / group / subscriptionData)',
        json_encode($body)
    );
    $c1->shutdown();

    $tagProbe = new Check46Probe();
    [$c2, ] = new_consumer('TAG', null, 'tagA || tagB', $tagProbe);
    $consumers[] = $c2;
    $c2->start();
    check($tagProbe->checkCount() === 0, 'a pure TAG subscription does not send a single code-46 (isTagType short-circuits)',
        'codes=' . implode(',', $tagProbe->codes));
    $c2->shutdown();

    // ---------- S2 / S3: bring the consumers up first, then send the messages ----------
    echo PHP_EOL . '---------- S2 the broker filters by property / S3 an expression that never matches ----------' . PHP_EOL;
    [$redC, $redBox] = new_consumer('FILTER', MessageSelector::bySql("color = 'red'"));
    [$greenC, $greenBox] = new_consumer('NONE', MessageSelector::bySql("color = 'green'"));
    [$allC, $allBox] = new_consumer('ALL');
    $consumers = array_merge($consumers, [$redC, $greenC, $allC]);
    foreach ([$redC, $greenC, $allC] as $c) {
        $c->start();
    }
    $sent = ['red' => [], 'blue' => []];
    foreach (['red', 'blue'] as $color) {
        for ($i = 0; $i < 3; $i++) {
            $text = 'body-' . $color . '-' . $i;
            $m = new Message($topic, $text, null, $stamp . '-' . $color . '-' . $i);
            $m->putProperty('color', $color);
            $ok = false;
            $err = '';
            for ($try = 0; $try < 10; $try++) {
                try {
                    $r = $prod->send($m);
                    $ok = $r->sendStatus === SendStatus::SEND_OK;
                    if (!$ok) {
                        $err = (string) $r->sendStatus;
                    }
                    break;
                } catch (Throwable $e) {
                    $err = $e->getMessage();
                    usleep(300_000);
                }
            }
            check($ok, 'sending ' . $text . ' succeeded', $err);
            $sent[$color][] = $text;
        }
    }

    wait_until(25.0, static fn (): bool => count($allBox->msgs) >= 6, $redC, $greenC, $allC);
    // wait a bit longer to confirm green did not simply "arrive late"
    $settle = microtime(true) + 4.0;
    while (microtime(true) < $settle) {
        $redC->tick();
        $greenC->tick();
        $allC->tick();
        usleep(100_000);
    }
    $redBodies = $redBox->sortedBodies();
    $allBodies = $allBox->sortedBodies();
    sort($sent['red']);
    sort($sent['blue']);
    $wantAll = array_merge($sent['red'], $sent['blue']);
    sort($wantAll);
    check($redBodies === $sent['red'], "SQL92(color=red) received 3 messages, exactly the 3 that were sent",
        'got=' . implode(',', $redBodies));
    check($allBodies === $wantAll, "the TAG '*' control group received all 6 messages (red + blue)",
        'got=' . implode(',', $allBodies));
    $leaked = array_intersect($sent['blue'], $redBodies);
    check($leaked === [], 'none of the 3 blue messages leaked into the SQL92 consumer (proving the broker really filters)',
        'got=' . implode(',', $redBodies));
    $colors = array_values(array_unique(array_map(
        static fn (MessageExt $m): string => (string) $m->getProperty('color'),
        $redBox->msgs
    )));
    check($colors === ['red'], 'the color property of the received messages is readable and all red',
        'got=' . implode(',', $colors));
    check(count($greenBox->msgs) === 0, "subscribing to the never-matching color='green' => not a single message received",
        'got=' . implode(',', $greenBox->bodies()));

    // ---------- S4 counter-proof: an illegal expression makes startup fail ----------
    echo PHP_EOL . '---------- S4 an illegal expression fails during startup ----------' . PHP_EOL;
    [$c4, ] = new_consumer('BAD', MessageSelector::bySql('color =='));
    $consumers[] = $c4;
    $err = null;
    $t0 = microtime(true);
    try {
        $c4->start();
    } catch (MQClientException $e) {
        $err = $e;
    }
    $cost = microtime(true) - $t0;
    check($err !== null, 'start() raised an MQClientException', $err === null ? 'no exception' : $err->getMessage());
    check($err !== null && broker_code($err) === 23,
        'the error code is the broker\'s SUBSCRIPTION_PARSE_FAILED(23)',
        'code=' . var_export(broker_code($err), true) . ' msg=' . ($err === null ? '' : $err->getMessage()));
    check($err !== null && $cost < 5.0, 'the illegal expression fails immediately (<5s, not waiting for a timeout fallback)',
        sprintf('%.2fs', $cost));
    // Was the rollback clean? The same object with a legal expression should be startable again
    $retryErr = null;
    try {
        $c4->subscribeWithSelector($topic, MessageSelector::bySql("color = 'blue'"));
        $c4->start();
    } catch (Throwable $e) {
        $retryErr = $e;
    }
    check($retryErr === null, 'after fixing the expression it can be started again (the startup rolled back in place)',
        $retryErr === null ? '' : $retryErr->getMessage());

    // ---------- S5 send-first + CONSUME_FROM_FIRST_OFFSET (the regression this leg pins) ----------
    // An earlier draft resolved the initial offset of a SQL92 subscription to the max
    // offset, silently skipping every message produced *before* the consumer started
    // (consumer-first passed only because the queues were empty and max was 0 anyway).
    // Java (RebalancePushImpl#computePullFromWhere) starts at 0 for FIRST_OFFSET
    // regardless of the subscription type and lets the broker filter.
    echo PHP_EOL . '---------- S5 send-first + CONSUME_FROM_FIRST_OFFSET ----------' . PHP_EOL;
    $firstTopic = 'Sql92FirstPhp_' . $stamp;
    $admin->createTopic($brokerName, $firstTopic, 4);
    $firstReady = wait_until(20.0, function () use ($admin, $firstTopic): bool {
        try {
            $r = $admin->examineTopicRoute($firstTopic);
        } catch (Throwable) {
            return false;
        }
        $n = 0;
        foreach ($r->queueDatas as $q) {
            $n += $q->readQueueNums;
        }
        return $n >= 4;
    });
    check($firstReady, 'S5 the send-first topic route is ready (4 queues)');
    $firstRed = [];
    $firstBlue = [];
    if ($firstReady) {
        foreach (['red', 'blue'] as $color) {
            for ($i = 0; $i < 2; $i++) {
                $text = 'first-' . $color . '-' . $i;
                $m = new Message($firstTopic, $text, null, $stamp . '-first-' . $color . '-' . $i);
                $m->putProperty('color', $color);
                $ok = false;
                $err = '';
                for ($try = 0; $try < 10; $try++) {
                    try {
                        $r = $prod->send($m);
                        $ok = $r->sendStatus === SendStatus::SEND_OK;
                        if (!$ok) {
                            $err = (string) $r->sendStatus;
                        }
                        break;
                    } catch (Throwable $e) {
                        $err = $e->getMessage();
                        usleep(300_000);
                    }
                }
                check($ok, 'S5 sending ' . $text . ' succeeded', $err);
                if ($color === 'red') {
                    $firstRed[] = $text;
                } else {
                    $firstBlue[] = $text;
                }
            }
        }
        [$fc, $fBox] = new_consumer('FIRST', MessageSelector::bySql("color = 'red'"), topicOverride: $firstTopic);
        $consumers[] = $fc;
        $fc->start();
        wait_until(25.0, static fn (): bool => count($fBox->msgs) >= 2, $fc);
        $settle = microtime(true) + 2.0;
        while (microtime(true) < $settle) {
            $fc->tick();
            usleep(100_000);
        }
        $fBodies = $fBox->sortedBodies();
        sort($firstRed, SORT_STRING);
        check($fBodies === $firstRed,
            'S5 SQL92 + FIRST_OFFSET receives the 2 red messages sent BEFORE start (was: skipped by the old max-offset short-circuit)',
            'got=' . implode(',', $fBodies));
        $leakedFirst = array_intersect($firstBlue, $fBodies);
        check($leakedFirst === [], 'S5 no blue leaks into the send-first SQL92 consumer',
            'got=' . implode(',', $fBodies));
    }

    // ---------- S6 the pull paths put the selector on the wire and the broker obeys it ----------
    // The sections above drive the push consumer. The pull/lite-pull entry points carry the
    // subscription themselves, so the two halves of the fix have to be proven separately: the
    // request really says SQL92 (the broker then filters by property), and the client does NOT
    // re-run a tag pass over what the broker matched. Reverting the selector support turns both
    // readings red: the request goes out typed TAG (so the broker admits all six messages) and the
    // raw expression is parsed as a tag list, so the local pass drops every one of them.
    echo PHP_EOL . '---------- S6 pull-path selector against a real broker ----------' . PHP_EOL;
    $pullTopic = 'Sql92PullPhp_' . $stamp;
    // One write queue: a single walk of one queue sees every message, so the counts below are
    // decided by filtering, not by how the sends spread over several queues.
    $admin->createTopic($brokerName, $pullTopic, 1);
    $pullReady = wait_until(20.0, function () use ($admin, $pullTopic): bool {
        try {
            $r = $admin->examineTopicRoute($pullTopic);
        } catch (Throwable) {
            return false;
        }
        $n = 0;
        foreach ($r->queueDatas as $q) {
            $n += $q->readQueueNums;
        }
        return $n >= 1;
    });
    check($pullReady, 'S6 the one-queue pull topic route is ready');
    if ($pullReady) {
        $pullRed = [];
        $pullBlue = [];
        foreach (['red', 'blue'] as $color) {
            for ($i = 0; $i < 3; $i++) {
                $text = 'pull-' . $color . '-' . $i;
                $m = new Message($pullTopic, $text, 'TagPull', $stamp . '-pull-' . $color . '-' . $i);
                $m->putProperty('color', $color);
                $ok = false;
                $err = '';
                for ($try = 0; $try < 10; $try++) {
                    try {
                        $r = $prod->send($m);
                        $ok = $r->sendStatus === SendStatus::SEND_OK;
                        if (!$ok) {
                            $err = (string) $r->sendStatus;
                        }
                        break;
                    } catch (Throwable $e) {
                        $err = $e->getMessage();
                        usleep(300_000);
                    }
                }
                check($ok, 'S6 sending ' . $text . ' succeeded', $err);
                if ($color === 'red') {
                    $pullRed[] = $text;
                } else {
                    $pullBlue[] = $text;
                }
            }
        }
        sort($pullRed, SORT_STRING);
        sort($pullBlue, SORT_STRING);

        $pullProbe = new PullWireProbe();
        $pc = new DefaultMQPullConsumer($group . '_PULL', $pullProbe);
        $pc->setNamesrvAddr($ns);
        $pc->setInstanceName('sql92php-pull-' . $stamp);
        $pc->start();
        $consumers[] = $pc;
        $mqs = $pc->fetchSubscribeMessageQueues($pullTopic);
        check(count($mqs) >= 1, 'S6 the pull topic has a queue to read', 'queues=' . count($mqs));
        if (count($mqs) >= 1) {
            $mq = $mqs[0];
            $sqlRed = "color = 'red'";

            $redBodies = [];
            $offset = 0;
            for ($round = 0; $round < 8; $round++) {
                $r = $pc->pullBySelector($mq, MessageSelector::bySql($sqlRed), $offset, 32);
                $offset = (int) $r->nextBeginOffset;
                foreach ($r->msgFoundList as $mm) {
                    $redBodies[] = (string) $mm->getBody();
                }
                if ($r->status !== PullStatus::FOUND) {
                    break;
                }
            }
            sort($redBodies, SORT_STRING);
            check($redBodies === $pullRed,
                'S6 the broker filtered a SQL92 pull by property: exactly the 3 red messages',
                'got=' . implode(',', $redBodies));
            check(array_intersect($pullBlue, $redBodies) === [],
                'S6 no blue leaked into the SQL92 pull', 'got=' . implode(',', $redBodies));

            $sqlRow = $pullProbe->last($pullTopic);
            check($sqlRow !== null && $sqlRow['expressionType'] === ExpressionType::SQL92
                && $sqlRow['subscription'] === $sqlRed && (int) $sqlRow['subVersion'] > 0,
                'S6 the pull request carried SQL92, the raw expression and a real subscription version',
                json_encode($sqlRow, JSON_UNESCAPED_UNICODE));
            check($sqlRow !== null
                && (((int) $sqlRow['sysFlag']) & PullSysFlag::FLAG_SUBSCRIPTION) !== 0
                && (((int) $sqlRow['sysFlag']) & PullSysFlag::FLAG_CLASS_FILTER) === 0,
                'S6 the SQL92 pull asks for the subscription without the legacy filter-server bit',
                $sqlRow === null ? 'no pull seen' : 'sysFlag=' . $sqlRow['sysFlag']);

            // Control on the same queue at the same offset: a plain tag expression is admitted
            // wholesale, which is what a dropped selector would have looked like.
            $all = [];
            for ($round = 0; $round < 8; $round++) {
                $r = $pc->pull($mq, '*', 0, 32);
                foreach ($r->msgFoundList as $mm) {
                    $all[] = (string) $mm->getBody();
                }
                break;
            }
            sort($all, SORT_STRING);
            check(count($all) === 6,
                'S6 control: the same queue read with "*" returns all six messages',
                'got=' . count($all) . ' [' . implode(',', $all) . ']');
            $tagRow = $pullProbe->last($pullTopic);
            check($tagRow !== null && $tagRow['expressionType'] === ExpressionType::TAG
                && (int) $tagRow['subVersion'] === 0 && $tagRow['subscription'] === '*',
                'S6 control: the tag pull keeps the historic shape (TAG, expression "*", version 0)',
                json_encode($tagRow, JSON_UNESCAPED_UNICODE));

            // The lite-pull entry point, assign mode with a selector.
            $liteProbe = new PullWireProbe();
            $lite = new DefaultLitePullConsumer($group . '_LITE', $liteProbe);
            $lite->setNamesrvAddr($ns);
            $lite->setInstanceName('sql92php-lite-' . $stamp);
            $lite->autoCommit = false;
            $lite->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
            $lite->setSubExpressionForAssignWithSelector($pullTopic, MessageSelector::bySql("color = 'blue'"));
            $lite->assign([$mq]);
            $lite->seek($mq, 0);
            $lite->start();
            $consumers[] = $lite;
            $blueBodies = [];
            $deadline = microtime(true) + 25.0;
            while (microtime(true) < $deadline && count($blueBodies) < 3) {
                $lite->tick();
                foreach ($lite->poll(200) as $mm) {
                    $blueBodies[] = (string) $mm->getBody();
                }
            }
            sort($blueBodies, SORT_STRING);
            check($blueBodies === $pullBlue,
                'S6 the lite pull of a selector subscription read only the blue messages',
                'got=' . implode(',', $blueBodies));
            $liteRow = $liteProbe->last($pullTopic);
            check($liteRow !== null && (int) $liteRow['code'] === RequestCode::LITE_PULL_MESSAGE
                && $liteRow['expressionType'] === ExpressionType::SQL92
                && $liteRow['subscription'] === "color = 'blue'" && (int) $liteRow['subVersion'] > 0,
                'S6 the LITE_PULL request carried the selector verbatim',
                json_encode($liteRow, JSON_UNESCAPED_UNICODE));
            check($liteRow !== null
                && (((int) $liteRow['sysFlag']) & PullSysFlag::FLAG_LITE_PULL_MESSAGE) !== 0
                && (((int) $liteRow['sysFlag']) & PullSysFlag::FLAG_CLASS_FILTER) === 0,
                'S6 the lite pull sysFlag keeps the lite bit and leaves the filter-server bit off',
                $liteRow === null ? 'no lite pull seen' : 'sysFlag=' . $liteRow['sysFlag']);
        }
    }
} catch (Throwable $e) {
    check(false, 'unexpected exception', $e->getMessage());
} finally {
    foreach ($consumers as $c) {
        try {
            $c->shutdown();
        } catch (Throwable) {
        }
    }
    try {
        $prod->shutdown();
    } catch (Throwable) {
    }
    try {
        $admin->shutdown();
    } catch (Throwable) {
    }
}

echo PHP_EOL . $pass . ' PASS / ' . $fail . ' FAIL' . PHP_EOL;
exit($fail === 0 ? 0 : 1);

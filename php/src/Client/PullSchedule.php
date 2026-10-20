<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\UtilAll;
use RocketMQ\Remoting\Protocol\MessageModel;
use RocketMQ\Remoting\Protocol\NamespaceUtil;

/**
 * Scheduled pull service for the pull consumer: one task per queue, driven by the caller's loop.
 *
 * The service keeps three tables, all observable for tests:
 *  - a callback table (topic -> task body), filled by {@see registerPullTaskCallback()};
 *  - a task table (queue -> task with its next due time), filled by {@see putTask()} or by the
 *    queue view a registered callback topic has not got tasks for yet;
 *  - a pull-request table (queue -> offset cursor), holding the state that decides where the
 *    next pull of that queue starts.
 *
 * A tick advances **only the due tasks**: after a run a task waits ``pullNextDelayTimeMillis``
 * before it pulls again, so a caller that ticks in a tight loop does not stampede the broker.
 * PHP has no threads, so {@see tick()} replaces both the scheduler pool and the periodic scan;
 * nothing here sleeps and nothing runs by itself.
 */
final class MQPullConsumerScheduleService
{
    /** Delay a task waits before its next run when the body asked for nothing else. */
    public const DEFAULT_PULL_NEXT_DELAY_MILLIS = 200;

    /** Delay applied when a task body throws. */
    public const PULL_TASK_ERROR_DELAY_MILLIS = 1000;

    /** How long a queue may stay unpullable before its task is dropped. */
    public const PULL_IMPOSSIBLE_MILLIS = 120000;

    /** Pump budget each tick spends reading pull responses already sent. */
    public const ASYNC_PUMP_MILLIS = 20;

    private DefaultMQPullConsumer $pullConsumer;

    /** Advisory only: this port runs no pools, so the number changes no behaviour. */
    private int $pullThreadNums = 20;

    /** @var array<string, PullTaskCallback> key = topic as registered */
    private array $callbackTable = [];

    /** @var array<string, PullRequest> key = queue key */
    private array $pullRequestTable = [];

    /** @var array<string, array{mq: MessageQueue, cancelled: bool, nextRunAt: int}> key = queue key */
    private array $taskTable = [];

    private bool $started = false;

    /** Clock the last tick ran on (0 until the first tick); every delay is measured against it. */
    private int $lastClockMillis = 0;

    public function __construct(string $consumerGroup, mixed $rpcHook = null, string $namespace = '')
    {
        $this->pullConsumer = new DefaultMQPullConsumer($consumerGroup, $rpcHook, $namespace);
        $this->pullConsumer->setMessageModel(MessageModel::CLUSTERING);
    }

    // ---------------- Wiring ----------------

    public function getDefaultMQPullConsumer(): DefaultMQPullConsumer
    {
        return $this->pullConsumer;
    }

    public function setDefaultMQPullConsumer(DefaultMQPullConsumer $consumer): void
    {
        $this->pullConsumer = $consumer;
    }

    public function getMessageModel(): string
    {
        return $this->pullConsumer->messageModel;
    }

    public function setMessageModel(string $model): void
    {
        $this->pullConsumer->setMessageModel($model);
    }

    public function getPullThreadNums(): int
    {
        return $this->pullThreadNums;
    }

    public function setPullThreadNums(int $nums): void
    {
        $this->pullThreadNums = $nums;
    }

    /**
     * Register the body that runs per queue of one topic, and put that topic on the subscription
     * list the heartbeat reports.
     */
    public function registerPullTaskCallback(string $topic, PullTaskCallback $callback): void
    {
        $this->callbackTable[NamespaceUtil::wrapNamespace($this->pullConsumer->namespace, $topic)] = $callback;
        $this->pullConsumer->registerTopic($topic);
    }

    /** @return array<string, PullTaskCallback> */
    public function getCallbackTable(): array
    {
        return $this->callbackTable;
    }

    // ---------------- Lifecycle ----------------

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        // A queue view from the consumer re-fills the task table, exactly as the pull consumer's
        // queue-changed callback does.
        $this->pullConsumer->setMessageQueueListener(new PullScheduleQueueListener($this));
        $this->pullConsumer->start();
        $this->started = true;
        Logger::info(sprintf(
            'pull schedule service started, group=%s, topics=%d',
            $this->pullConsumer->consumerGroup,
            count($this->callbackTable),
        ));
    }

    public function isRunning(): bool
    {
        return $this->started;
    }

    public function shutdown(): void
    {
        if (!$this->started) {
            return;
        }
        $this->started = false;
        foreach (array_keys($this->taskTable) as $key) {
            $this->taskTable[$key]['cancelled'] = true;
        }
        $this->pullConsumer->shutdown();
        Logger::info(sprintf('pull schedule service stopped, group=%s', $this->pullConsumer->consumerGroup));
    }

    // ---------------- Task table ----------------

    /**
     * Reconcile the tasks of one topic against a queue view: queues that left the view get their
     * task cancelled and dropped, queues that joined get a task due immediately.
     *
     * @param list<MessageQueue> $mqNewSet
     */
    public function putTask(string $topic, array $mqNewSet): void
    {
        $wanted = [];
        foreach ($mqNewSet as $mq) {
            if ($mq instanceof MessageQueue) {
                $wanted[self::key($mq)] = true;
            }
        }
        foreach ($this->taskTable as $key => $task) {
            if ($task['mq']->topic === $topic && !isset($wanted[$key])) {
                $this->taskTable[$key]['cancelled'] = true;
                unset($this->taskTable[$key]);
            }
        }
        foreach ($mqNewSet as $mq) {
            if (!$mq instanceof MessageQueue) {
                continue;
            }
            $key = self::key($mq);
            if (isset($this->taskTable[$key])) {
                continue;
            }
            $this->taskTable[$key] = ['mq' => $mq, 'cancelled' => false, 'nextRunAt' => $this->nowMillis()];
            $this->pullRequestTable[$key] ??= new PullRequest($this->pullConsumer->consumerGroup, $mq);
        }
    }

    /**
     * Dispatch one queue view the way the pull consumer's queue-changed callback does:
     * broadcasting walks every queue, clustering only the ones allocated to this client.
     *
     * @param list<MessageQueue> $mqAll
     * @param list<MessageQueue> $mqDivided
     */
    public function queueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        if ($this->pullConsumer->messageModel === MessageModel::BROADCASTING) {
            $this->putTask($topic, $mqAll);
            return;
        }
        $this->putTask($topic, $mqDivided !== [] ? $mqDivided : $mqAll);
    }

    /**
     * Clock a task may read: the time the last tick ran on, so injected-clock tests stay deterministic.
     */
    public function nowMillis(): int
    {
        return $this->lastClockMillis !== 0 ? $this->lastClockMillis : UtilAll::currentTimeMillis();
    }

    // ---------------- State readable by tests ----------------

    /** @return list<string> queue keys holding a task, in registration order */
    public function taskKeys(): array
    {
        return array_keys($this->taskTable);
    }

    public function taskCount(): int
    {
        return count($this->taskTable);
    }

    public function hasTask(MessageQueue $mq): bool
    {
        return isset($this->taskTable[self::key($mq)]);
    }

    /** When the task of a queue may pull next (0 when it holds no task). */
    public function nextRunAt(MessageQueue $mq): int
    {
        return $this->taskTable[self::key($mq)]['nextRunAt'] ?? 0;
    }

    /** @return array<string, PullRequest> */
    public function getPullRequestTable(): array
    {
        return $this->pullRequestTable;
    }

    public function pullRequestOf(MessageQueue $mq): ?PullRequest
    {
        return $this->pullRequestTable[self::key($mq)] ?? null;
    }

    // ---------------- The scan ----------------

    /**
     * One round of the scheduler: pump the pull responses already on the wire, pick up the queue
     * view of callback topics that hold no task yet, then run every task that is due.
     */
    public function tick(?int $nowMillis = null): void
    {
        if (!$this->started) {
            return;
        }
        $now = $nowMillis ?? UtilAll::currentTimeMillis();
        $this->lastClockMillis = $now;
        $this->pullConsumer->tick($now);

        foreach (array_keys($this->callbackTable) as $topicKey) {
            if (!$this->topicHasTask($topicKey)) {
                $this->discoverTasks($topicKey);
            }
        }

        foreach ($this->taskKeys() as $key) {
            if (!isset($this->taskTable[$key])) {
                continue;
            }
            $this->runTask($key, $now);
        }
    }

    private function topicHasTask(string $topic): bool
    {
        foreach ($this->taskTable as $task) {
            if ($task['mq']->topic === $topic) {
                return true;
            }
        }
        return false;
    }

    /** The queue view of one registered topic, as far as this port can compute it without a rebalance loop. */
    private function discoverTasks(string $topic): void
    {
        try {
            $mqAll = $this->pullConsumer->fetchSubscribeMessageQueues($topic);
            $mqDivided = $this->pullConsumer->fetchMessageQueuesInBalance($topic);
        } catch (\Throwable $e) {
            Logger::debug(sprintf('queue view of %s unavailable: %s', $topic, $e->getMessage()));
            return;
        }
        if ($mqAll === [] && $mqDivided === []) {
            return;
        }
        $this->queueChanged($topic, $mqAll, $mqDivided);
    }

    private function runTask(string $key, int $now): void
    {
        $task = $this->taskTable[$key];
        if ($task['cancelled']) {
            return;
        }
        $mq = $task['mq'];
        $request = $this->pullRequestTable[$key] ?? new PullRequest($this->pullConsumer->consumerGroup, $mq);
        $this->pullRequestTable[$key] = $request;

        if ($request->isPullImpossible($now, self::PULL_IMPOSSIBLE_MILLIS)) {
            // Unpullable for the whole window: drop the task; a later queue view registers it anew.
            unset($this->taskTable[$key]);
            Logger::warning(sprintf('the pull task of %s is dropped, pulling has been impossible for %d ms',
                (string) $mq, self::PULL_IMPOSSIBLE_MILLIS));
            return;
        }
        if ($now < $task['nextRunAt']) {
            return;
        }
        $callback = $this->callbackOf($mq->topic);
        if ($callback === null) {
            Logger::warning(sprintf('pull task callback not exist, topic=%s', $mq->topic));
            return;
        }

        $context = new PullTaskContext($this->pullConsumer, $this);
        $context->attach($mq, $request);
        try {
            $callback->doPullTask($mq, $context);
        } catch (\Throwable $e) {
            $context->setPullNextDelayTimeMillis(self::PULL_TASK_ERROR_DELAY_MILLIS);
            $request->markPullFailure($now);
            Logger::error('pull task failed: ' . $e->getMessage());
        }

        if (($this->taskTable[$key]['cancelled'] ?? true) === true) {
            Logger::warning(sprintf('the pull task is cancelled after the task body, %s', (string) $mq));
            return;
        }
        $this->taskTable[$key]['nextRunAt'] = $now + $context->getPullNextDelayTimeMillis();
    }

    /** Body registered for a topic (raw name first, then its namespace-wrapped form). */
    private function callbackOf(string $topic): ?PullTaskCallback
    {
        return $this->callbackTable[$topic]
            ?? $this->callbackTable[NamespaceUtil::wrapNamespace($this->pullConsumer->namespace, $topic)]
            ?? null;
    }

    private static function key(MessageQueue $mq): string
    {
        return $mq->topic . $mq->brokerName . (string) $mq->queueId;
    }
}

/**
 * The queue-changed callback of the schedule service: broadcasting takes every queue,
 * clustering only the ones allocated to this client.
 */
final class PullScheduleQueueListener implements MessageQueueListener
{
    public function __construct(private MQPullConsumerScheduleService $service)
    {
    }

    public function messageQueueChanged(string $topic, array $mqAll, array $mqDivided): void
    {
        $this->service->queueChanged($topic, $mqAll, $mqDivided);
    }
}

/**
 * Per-queue cursor of the schedule service: where the next pull starts, the bounds the broker
 * last reported, when it last answered, and whether that answer was usable.
 *
 * The offset rules are the classic ones:
 *  - a found batch and an offset the broker rejected both move the cursor to the
 *    ``nextBeginOffset`` the response carries;
 *  - "nothing to consume" leaves the cursor **where it is** -- unless the cursor no longer falls
 *    inside the bounds the broker reports, which means the queue moved under it. Then the cursor
 *    is reset to the offset the broker names, and the task is marked as not pulling well.
 */
final class PullRequest
{
    public function __construct(
        public string $consumerGroup,
        public MessageQueue $messageQueue,
        public int $nextBeginOffset = 0,
    ) {
    }

    public int $minOffset = 0;

    public int $maxOffset = 0;

    /** Null until the first pull answer (or the first failure) is recorded. */
    public ?int $lastPullTimestamp = null;

    public bool $pullOk = true;

    public function processPullResult(?PullResult $result, int $nowMillis): void
    {
        if ($result === null) {
            // Nothing came back, but the round itself happened: the window of a stalled queue
            // counts from the last round the loop made, not from the last answer.
            $this->lastPullTimestamp = $nowMillis;
            return;
        }
        $this->minOffset = $result->minOffset;
        $this->maxOffset = $result->maxOffset;
        switch ($result->status) {
            case PullStatus::FOUND:
            case PullStatus::OFFSET_ILLEGAL:
                $this->nextBeginOffset = $result->nextBeginOffset;
                $this->pullOk = true;
                break;
            case PullStatus::NO_NEW_MSG:
            case PullStatus::NO_MATCHED_MSG:
                $this->pullOk = false;
                if ($this->nextBeginOffset < $result->minOffset || $this->nextBeginOffset > $result->maxOffset) {
                    // The cursor points outside the queue as it now stands: follow the broker.
                    $this->nextBeginOffset = $result->nextBeginOffset;
                }
                break;
        }
        $this->lastPullTimestamp = $nowMillis;
    }

    /** Record a round that produced no result at all (a thrown body, a failed request). */
    public function markPullFailure(int $nowMillis): void
    {
        $this->pullOk = false;
        $this->lastPullTimestamp = $nowMillis;
    }

    public function isPullImpossible(int $nowMillis, int $windowMillis): bool
    {
        return !$this->pullOk
            && $this->lastPullTimestamp !== null
            && ($nowMillis - $this->lastPullTimestamp) > $windowMillis;
    }

    public function __toString(): string
    {
        return sprintf(
            'PullRequest [consumerGroup=%s, messageQueue=%s, nextBeginOffset=%d]',
            $this->consumerGroup,
            (string) $this->messageQueue,
            $this->nextBeginOffset,
        );
    }
}

/**
 * What a task body gets: the consumer to pull with, the queue and cursor it pulls for, and the
 * delay before this task runs again.
 */
final class PullTaskContext
{
    private int $pullNextDelayTimeMillis = MQPullConsumerScheduleService::DEFAULT_PULL_NEXT_DELAY_MILLIS;

    private ?MessageQueue $messageQueue = null;

    private ?PullRequest $pullRequest = null;

    public function __construct(
        private DefaultMQPullConsumer $pullConsumer,
        private MQPullConsumerScheduleService $service,
    ) {
    }

    /** Bind the queue and its cursor for one run (called by the service, not by task bodies). */
    public function attach(MessageQueue $mq, PullRequest $request): void
    {
        $this->messageQueue = $mq;
        $this->pullRequest = $request;
    }

    public function getPullConsumer(): DefaultMQPullConsumer
    {
        return $this->pullConsumer;
    }

    public function setPullConsumer(DefaultMQPullConsumer $consumer): void
    {
        $this->pullConsumer = $consumer;
    }

    public function getService(): MQPullConsumerScheduleService
    {
        return $this->service;
    }

    public function getMessageQueue(): ?MessageQueue
    {
        return $this->messageQueue;
    }

    public function getPullRequest(): ?PullRequest
    {
        return $this->pullRequest;
    }

    public function getPullNextDelayTimeMillis(): int
    {
        return $this->pullNextDelayTimeMillis;
    }

    public function setPullNextDelayTimeMillis(int $millis): void
    {
        $this->pullNextDelayTimeMillis = $millis;
    }

    /**
     * Pull this queue from its recorded next offset.
     *
     * With no callback this is the synchronous pull and the cursor moves before the result is
     * handed back; with a callback the request is asynchronous and the cursor moves on the
     * response, right before the caller's own callback runs.
     */
    public function pullNext(int $maxNums = 32, string $subExpression = '*', ?PullCallback $callback = null): ?PullResult
    {
        $mq = $this->messageQueue;
        $request = $this->pullRequest;
        if ($mq === null || $request === null) {
            throw new MQClientException('this context carries no queue to pull');
        }
        if ($callback === null) {
            $result = $this->pullConsumer->pull($mq, $subExpression, $request->nextBeginOffset, $maxNums);
            $request->processPullResult($result, $this->service->nowMillis());
            return $result;
        }
        $this->pullConsumer->pullAsync(
            $mq,
            $subExpression,
            $request->nextBeginOffset,
            $maxNums,
            new PullTaskResultCallback($request, $this->service, $callback),
        );
        return null;
    }
}

/**
 * Moves the queue cursor by the classic offset rules, then forwards the result to the caller's
 * own callback. A failed request counts as "this round pulled nothing".
 */
final class PullTaskResultCallback implements PullCallback
{
    public function __construct(
        private PullRequest $request,
        private MQPullConsumerScheduleService $service,
        private PullCallback $next,
    ) {
    }

    public function onSuccess(PullResult $pullResult): void
    {
        $this->request->processPullResult($pullResult, $this->service->nowMillis());
        $this->next->onSuccess($pullResult);
    }

    public function onException(\Throwable $e): void
    {
        $this->request->markPullFailure($this->service->nowMillis());
        $this->next->onException($e);
    }
}

/**
 * Contract a task body implements: it runs once per queue the service holds a task for, and it
 * decides how long that queue waits before the next run.
 */
interface PullTaskCallback
{
    public function doPullTask(MessageQueue $mq, PullTaskContext $context): void;
}

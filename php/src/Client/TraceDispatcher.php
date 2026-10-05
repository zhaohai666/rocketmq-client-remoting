<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\UtilAll;

/**
 * 异步轨迹分发器（对应 org.apache.rocketmq.client.trace.AsyncTraceDispatcher，
 * 移植自 python/client/trace_dispatcher.py）。
 *
 * 职责：钩子把 TraceContext 丢进内存队列（append），按「攒够 batchNum 条 或 距上次
 * 发送超过 5s」两个条件触发刷写，再把编码后的文本用**独立的内部生产者**发到轨迹
 * topic（默认 RMQ_SYS_TRACE_TOPIC）。
 *
 * ⚠ PHP 无线程（见 php/PORTING.md「异步模型」）：Python 的后台线程 + 线程池
 *   （_async_run / ThreadPoolExecutor）在这里改由**调用方驱动**：
 *     - append() 只入队（与 Java/Python 一致，绝不阻塞业务）；
 *     - flush() 强制排空队列；pumpOnce() 是「跑一轮后台循环」的显式等价物，
 *       两个方法都在**当前线程同步**把批量发送做完。**不起线程/子进程**。
 *
 * 与 Java 的对应关系：
 *   - traceContextQueue      ← ArrayBlockingQueue<TraceContext>(2048)
 *   - flushTraceContext      ← flushTraceContext（含 forceFlush 语义）
 *   - sendTraceData          ← AsyncDataSendTask.sendTraceData（按 topic@traceTopic 分组）
 *   - flushData              ← flushData（按 maxMsgSize 切块）
 *   - sendTraceDataByMq      ← sendTraceDataByMQ（带 broker 过滤的选择器）
 *   - 内部生产者组名          ← _INNER_TRACE_PRODUCER-<group>-<PRODUCE|CONSUME>-<N>
 *
 * **防递归**：内部生产者自身的 enableTrace 必须为 False，且 SendMessageTraceHook 会
 * 跳过 topic 以轨迹 topic 开头的消息 —— 两道保险都要有，否则轨迹会自我复制到无限。
 */
enum TraceDispatcherType: string
{
    case PRODUCE = 'PRODUCE';
    case CONSUME = 'CONSUME';
}

/**
 * 只在指定 broker 集合里轮询选队列（对应 Java 里那个匿名 MessageQueueSelector，
 * 移植自 trace_dispatcher.py 的 _BrokerSetSelector）。
 *
 * 构造时按引用接住 dispatcher 的 sendWhichQueue，保证跨多次发送共享同一个游标。
 */
final class BrokerSetSelector
{
    private int $counter;

    public function __construct(int &$counter)
    {
        $this->counter = &$counter;
    }

    /**
     * @param list<MessageQueue> $mqs
     * @param array<string,true>|null $arg broker 集合
     */
    public function select(array $mqs, Message $msg, mixed $arg): MessageQueue
    {
        $brokerSet = is_array($arg) ? $arg : [];
        $filtered = [];
        foreach ($mqs as $q) {
            if (isset($brokerSet[$q->brokerName])) {
                $filtered[] = $q;
            }
        }
        if ($filtered === []) {
            // Java 在这里会 filterMqs.get(pos) 抛越界；退化为全量轮询更安全，
            // 语义上等价于「没有跨集群过滤需求」。
            $filtered = array_values($mqs);
        }
        if ($filtered === []) {
            throw new \RuntimeException('no message queue available for trace send');
        }
        $pos = $this->counter % count($filtered);
        $this->counter = $this->counter + 1;
        return $filtered[$pos];
    }
}

/**
 * 轨迹异步分发器。
 */
final class AsyncTraceDispatcher
{
    /** 内部生产者组名的全局自增序号（Python itertools.count(1)）。 */
    private static int $counter = 1;
    /** traceInstanceId 的全局自增序号（Python itertools.count(0)）。 */
    private static int $instanceNum = 0;

    public const WAIT_FOR_SHUTDOWN = 5000;
    public const FLUSH_TRACE_INTERVAL = 5000;
    /** 对应 Java ArrayBlockingQueue<TraceContext>(2048)。 */
    public const MAX_QUEUE_SIZE = 2048;

    public int $batchNum;
    public int $maxMsgSize = 128000;
    public int $traceInstanceId;
    public string $group;
    public TraceDispatcherType $type;
    /** @var list<TraceContext> */
    public array $traceContextQueue = [];
    public int $discardCount = 0;
    public bool $stopped = false;
    public bool $isStarted = false;
    /** PHP 无线程：恒为 null（保留字段只为逐字对照 Python）。 */
    public mixed $worker = null;
    public AccessChannel $accessChannel = AccessChannel::LOCAL;
    public string $namespaceV2 = '';
    public mixed $hostProducer = null;
    public mixed $hostConsumer = null;
    /** 内部生产者（DefaultMQProducer 未移植时为 null，可由调用方注入 fake）。 */
    public mixed $traceProducer = null;
    public int $sendWhichQueue = 0;
    public int $lastFlushTime;
    public string $traceTopicName;

    public function __construct(
        string $group,
        TraceDispatcherType $type,
        int $batchNum = 10,
        ?string $traceTopicName = null,
        mixed $rpcHook = null,
    ) {
        $this->batchNum = min($batchNum, 20);      // Java 注释明说最大 20
        $this->traceInstanceId = self::$instanceNum++;
        $this->group = $group;
        $this->type = $type;
        $this->lastFlushTime = UtilAll::currentTimeMillis();
        $this->traceTopicName = $traceTopicName ?? \RocketMQ\Common\MixAll::TRACE_TOPIC;
        $this->traceProducer = $this->getAndCreateTraceProducer($rpcHook);
    }

    // ---------------- 内部生产者 ----------------

    private function getAndCreateTraceProducer(mixed $rpcHook): mixed
    {
        // DefaultMQProducer 由其它子任务移植；未落地时不自行创建（测试注入 fake）。
        if (!class_exists(\RocketMQ\Client\DefaultMQProducer::class)) {
            return null;
        }
        try {
            $producer = new \RocketMQ\Client\DefaultMQProducer($this->genGroupNameForTrace(), $rpcHook);
            if (method_exists($producer, 'setSendMsgTimeout')) {
                $producer->setSendMsgTimeout(5000);
            }
            if (method_exists($producer, 'setMaxMessageSize')) {
                $producer->setMaxMessageSize($this->maxMsgSize);
            }
            // ⚠ 必须关闭自身的轨迹，否则轨迹消息会被再次追踪 → 无限递归
            if (method_exists($producer, 'setEnableTrace')) {
                $producer->setEnableTrace(false);
            }
            return $producer;
        } catch (\Throwable $e) {
            Logger::warning('create trace producer failed: ' . $e->getMessage());
            return null;
        }
    }

    /** 内部生产者组名：_INNER_TRACE_PRODUCER-<group>-<PRODUCE|CONSUME>-<N>。 */
    public function genGroupNameForTrace(): string
    {
        return sprintf(
            '%s-%s-%s-%d',
            TraceConstants::GROUP_NAME_PREFIX,
            $this->group,
            $this->type->value,
            self::$counter++
        );
    }

    public function getTraceTopicName(): string
    {
        return $this->traceTopicName;
    }

    public function setHostProducer(mixed $host): void
    {
        $this->hostProducer = $host;
    }

    public function setHostConsumer(mixed $host): void
    {
        $this->hostConsumer = $host;
    }

    /** 宿主客户端的 clientId（EndTransaction 轨迹的 clientHost 用它）。 */
    public function clientId(): string
    {
        $host = $this->hostProducer ?? $this->hostConsumer;
        if (!is_object($host)) {
            return '';
        }
        $client = null;
        foreach (['mqClient', '_mq_client'] as $p) {
            if (property_exists($host, $p)) {
                $client = $host->{$p};
                break;
            }
        }
        if ($client === null && method_exists($host, 'getMqClient')) {
            $client = $host->getMqClient();
        }
        if (!is_object($client)) {
            return '';
        }
        foreach (['clientId', 'client_id'] as $p) {
            if (property_exists($client, $p)) {
                return (string) $client->{$p};
            }
        }
        if (method_exists($client, 'getClientId')) {
            return (string) $client->getClientId();
        }
        return '';
    }

    // ---------------- 生命周期 ----------------

    public function start(string $nameSrvAddr, ?AccessChannel $accessChannel = null): void
    {
        if (!$this->isStarted && $this->traceProducer !== null) {
            if (method_exists($this->traceProducer, 'setNamesrvAddr')) {
                $this->traceProducer->setNamesrvAddr($nameSrvAddr);
            }
            if (method_exists($this->traceProducer, 'setInstanceName')) {
                $this->traceProducer->setInstanceName(
                    sprintf('%s_%s', TraceConstants::TRACE_INSTANCE_NAME, $nameSrvAddr)
                );
            }
            if (method_exists($this->traceProducer, 'setEnableTrace')) {
                $this->traceProducer->setEnableTrace(false);
            }
            $this->traceProducer->start();
            $this->isStarted = true;
        }
        $this->accessChannel = $accessChannel ?? AccessChannel::LOCAL;
        // PHP 无线程：不启动 worker，刷写靠 flush()/pumpOnce() 显式驱动。
        $this->stopped = false;
    }

    public function shutdown(): void
    {
        try {
            $this->flush();
        } catch (\Throwable $e) {
            Logger::error('trace dispatcher flush before shutdown failed: ' . $e->getMessage());
        }
        if ($this->isStarted && $this->traceProducer !== null) {
            try {
                $this->traceProducer->shutdown();
            } catch (\Throwable $e) {
                Logger::debug('trace producer shutdown failed: ' . $e->getMessage());
            }
        }
        $this->stopped = true;
    }

    // ---------------- 入队 / 刷写 ----------------

    /** 把一个 TraceContext 入队。队列满时计数并丢弃（与 Java 一致，不阻塞业务）。 */
    public function append(TraceContext $ctx): bool
    {
        if (count($this->traceContextQueue) >= self::MAX_QUEUE_SIZE) {
            $this->discardCount++;
            Logger::info(sprintf('buffer full%d ,context is %s', $this->discardCount, (string) $ctx));
            return false;
        }
        $this->traceContextQueue[] = $ctx;
        return true;
    }

    /** 强制刷空队列（Java flush()）。 */
    public function flush(): void
    {
        while ($this->traceContextQueue !== []) {
            try {
                $this->flushTraceContext(true);
            } catch (\Throwable $e) {
                Logger::error('flushTraceContext error: ' . $e->getMessage());
            }
        }
    }

    /**
     * 显式跑一轮后台循环（替代 Python 的 _async_run 线程体）。
     *
     * @param bool $forceFlush true 时无条件刷写（等价 flush 的一步）
     */
    public function pumpOnce(bool $forceFlush = false): void
    {
        try {
            $this->flushTraceContext($forceFlush);
        } catch (\Throwable $e) {
            Logger::error('flushTraceContext error: ' . $e->getMessage());
        }
    }

    private function flushTraceContext(bool $forceFlush): void
    {
        $size = count($this->traceContextQueue);
        if ($size !== 0) {
            $now = UtilAll::currentTimeMillis();
            if ($forceFlush || $size >= $this->batchNum
                || ($now - $this->lastFlushTime) > self::FLUSH_TRACE_INTERVAL) {
                $contextList = [];
                for ($i = 0; $i < $this->batchNum; $i++) {
                    if ($this->traceContextQueue === []) {
                        break;
                    }
                    $contextList[] = array_shift($this->traceContextQueue);
                }
                $this->sendTraceData($contextList);
                return;
            }
        }
        // 对应 Java Thread.sleep(5) 的让路：PHP 由调用方驱动，不在这里 sleep。
    }

    // ---------------- 发送 ----------------

    /** 按 (业务 topic, 轨迹 topic) 分组后逐组发送（对应 Java sendTraceData）。 */
    private function sendTraceData(array $contextList): void
    {
        if ($contextList === []) {
            return;
        }
        $this->lastFlushTime = UtilAll::currentTimeMillis();
        /** @var array<string, list<?TraceTransferBean>> $beanMap */
        $beanMap = [];
        foreach ($contextList as $context) {
            $accessChannel = $context->accessChannel ?? $this->accessChannel;
            $regionId = $context->regionId;
            if ($regionId === '' || $context->traceBeans === []) {
                continue;
            }
            if ($accessChannel === AccessChannel::CLOUD) {
                $traceTopic = TraceConstants::TRACE_TOPIC_PREFIX . $regionId;
            } else {
                $traceTopic = $this->traceTopicName;
            }
            $topic = $context->traceBeans[0]->topic;
            $key = $topic . TraceConstants::CONTENT_SPLITOR . $traceTopic;
            $beanMap[$key][] = TraceDataEncoder::encoderFromContextBean($context);
        }
        foreach ($beanMap as $key => $beanList) {
            [$topic, $traceTopic] = explode(TraceConstants::CONTENT_SPLITOR, $key);
            $this->flushData($beanList, $topic, $traceTopic);
        }
    }

    /**
     * 按 maxMsgSize 把多条编码结果攒成一条消息体，超限即切块发送
     * （对应 Java flushData；Python 用 len(buffer) 计数，这里用 mb_strlen 对齐 code point）。
     */
    private function flushData(array $transBeanList, string $topic, string $traceTopic): void
    {
        if ($transBeanList === []) {
            return;
        }
        $buffer = '';
        $keySet = [];
        $count = 0;
        foreach ($transBeanList as $bean) {
            if ($bean === null) {
                continue;
            }
            foreach ($bean->transKey as $k => $_) {
                $keySet[$k] = true;
            }
            $buffer .= $bean->transData;
            $count++;
            if (mb_strlen($buffer, 'UTF-8') >= $this->maxMsgSize) {
                $this->sendTraceDataByMq($keySet, $buffer, $traceTopic);
                $buffer = '';
                $keySet = [];
                $count = 0;
            }
        }
        if ($count > 0) {
            $this->sendTraceDataByMq($keySet, $buffer, $traceTopic);
        }
    }

    private function sendTraceDataByMq(array $keySet, string $data, string $traceTopic): void
    {
        if ($this->traceProducer === null) {
            Logger::warning('trace producer is not available, drop trace data: ' . $traceTopic);
            return;
        }
        $msg = new Message($traceTopic, $data);
        // keys 里放的是**原始消息的 msgId**（不是 offsetMsgId），控制台按它反查轨迹
        $keys = [];
        foreach ($keySet as $k => $_) {
            if ($k !== '') {
                $keys[] = $k;
            }
        }
        sort($keys, SORT_STRING);
        $msg->setKeys(implode(MessageConst::KEY_SEPARATOR, $keys));
        try {
            $traceBrokerSet = self::tryGetMessageQueueBrokerSet($this->traceProducer, $traceTopic);
            if ($traceBrokerSet === []) {
                $this->traceProducer->send($msg, 5000);
            } else {
                $this->traceProducer->sendBySelector(
                    $msg,
                    new BrokerSetSelector($this->sendWhichQueue),
                    $traceBrokerSet,
                    5000
                );
            }
        } catch (\Throwable $e) {
            Logger::error(sprintf('send trace data failed, the traceData is %s: %s', $data, $e->getMessage()));
        }
    }

    /**
     * 取该 topic 涉及的 broker 名集合（对应 Java tryGetMessageQueueBrokerSet）。
     *
     * 只有跨集群（CLOUD）场景才需要按 broker 过滤；本地场景返回空集即走普通轮询。
     *
     * @return array<string,true>
     */
    private static function tryGetMessageQueueBrokerSet(mixed $producer, string $topic): array
    {
        $brokerSet = [];
        if (!is_object($producer)) {
            return $brokerSet;
        }
        try {
            $publish = null;
            if (method_exists($producer, 'topicPublishInfo')) {
                $publish = $producer->topicPublishInfo($topic);
            } elseif (method_exists($producer, 'getTopicPublishInfo')) {
                $publish = $producer->getTopicPublishInfo($topic);
            }
            if ($publish === null) {
                return $brokerSet;
            }
            $list = $publish->msgQueueList ?? [];
            foreach ($list as $q) {
                $brokerName = $q->brokerName ?? null;
                if ($brokerName !== null) {
                    $brokerSet[$brokerName] = true;
                }
            }
        } catch (\Throwable $e) {
            Logger::debug(sprintf('tryGetMessageQueueBrokerSet(%s) failed: %s', $topic, $e->getMessage()));
        }
        return $brokerSet;
    }
}

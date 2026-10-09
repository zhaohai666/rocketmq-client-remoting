<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

/**
 * 消费侧公共基础件，移植自 python/client/consumer.py 的模块常量、模块级辅助函数
 * 与 PopProcessQueue。
 *
 * 分配策略（AllocateMessageQueue*）已在 Allocation.php 移植，不在此重复；
 * 消费/拉取结果类型在 ConsumerResult.php。
 *
 * ⚠ PHP 的 autoloader 只加载类、不加载函数：本文件里的全局函数随本文件被
 * autoload（任何类被首次引用时）一并定义。调用方若只调函数不碰类，须先
 * require 本文件（各消费方文件头部已做该保护）。
 */

/** 消费侧常量（全部对齐 Java 数值）。 */
final class ConsumerDefaults
{
    /**
     * POP 消费失败的延迟梯度（秒），逐项对应 Java
     * DefaultMQPushConsumerImpl.popDelayLevel。
     * @var list<int>
     */
    public const POP_DELAY_LEVEL = [10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600,
        1200, 1800, 3600, 7200];

    /** Java DefaultMQPushConsumerImpl.MIN/MAX_POP_INVISIBLE_TIME：超出范围一律回落到 60000 */
    public const MIN_POP_INVISIBLE_TIME = 5000;
    public const MAX_POP_INVISIBLE_TIME = 300000;

    /** Java ConsumeInitMode */
    public const CONSUME_INIT_MODE_MIN = 0;
    public const CONSUME_INIT_MODE_MAX = 1;

    /** Java `Integer.MAX_VALUE`：顺序消费用尽判据的默认值（-1 在这条链路上读成它）。 */
    public const JAVA_INT_MAX = 0x7FFFFFFF;

    /**
     * Java ProcessQueue.PULL_MAX_IDLE_TIME（默认 120000ms）。
     * 一个仍归本实例的队列超过这么久没发起过任何拉取/弹出 ⇒ rebalance 撤掉重建。
     */
    public const PULL_MAX_IDLE_TIME = 120.0;

    /**
     * Java DefaultMQPushConsumerImpl.PULL_TIME_DELAY_MILLS_WHEN_SUSPEND（1000ms）：
     * suspend() 之后一条拉取循环每轮退避这么久。PHP 没有循环线程，退避由调用方的
     * tick 节奏承担（见 PushConsumer::suspend() 的说明），这个常量锁的是语义差：
     * 挂起**不撤队列**，而 PULL_MAX_IDLE_TIME 判的是"这条循环还活着吗"。
     */
    public const PULL_TIME_DELAY_WHEN_SUSPEND = 1.0;
}

/** 队列排序键，语义对齐 Java MessageQueue.compareTo：topic → brokerName → queueId。 */
function consumer_mq_sort_key(MessageQueue $mq): array
{
    // Java rebalance 会先把 mqAll/cidAll 排序再分配；顺序不一致会让不同实例算出
    // 不同的分配结果（同一队列被两个实例同时消费）。
    return [$mq->topic, $mq->brokerName, $mq->queueId];
}

function safe_hook_name(object $hook): string
{
    try {
        if (method_exists($hook, 'hookName')) {
            return (string) $hook->hookName();
        }
    } catch (\Throwable) {
        // 落到类名兜底
    }
    return get_class($hook);
}

/**
 * 客户端二次 tag 过滤（对应 Java PullAPIWrapper.processPullResult:113-122）。
 *
 * broker 侧是按 tag 的**哈希（codeSet）**过滤的，存在哈希碰撞误放；Java 因此让客户端
 * 再按字符串核一遍。守卫 `!tagsSet.isEmpty() && !isClassFilterMode` 意味着：
 * 订阅 `"*"`（SUB_ALL）时不过滤 —— 所以 `FilterAPI::buildSubscriptionData`
 * 对 SUB_ALL 必须保持 tagsSet 为空（那里有详细注释）。
 *
 * @param list<MessageExt> $msgs
 * @return list<MessageExt>
 */
function client_side_tag_filter(?SubscriptionData $sub, array $msgs): array
{
    if ($msgs === [] || $sub === null || $sub->tagsSet === [] || $sub->isClassFilterMode()) {
        return $msgs;
    }
    $out = [];
    foreach ($msgs as $m) {
        $tags = $m->getTags();
        // ⚠ tagsSet 是 list（addTag 用 in_array 追加），不是 tag→tag 的 map；
        //   用 isset($tagsSet[$tag]) 查键永远 miss，会把整批全滤掉。
        if ($tags !== null && in_array($tags, $sub->tagsSet, true)) {
            $out[] = $m;
        }
    }
    return $out;
}

/**
 * 依次执行过滤钩子，**异常一律吞掉**（Java PullAPIWrapper.executeHook:171-178 记 error）。
 *
 * 与 send/consume 钩子不同：过滤钩子失败不能影响消费，只是该次过滤不生效。
 *
 * @param list<object> $hookList
 */
function execute_filter_hooks(array $hookList, FilterMessageContext $context): void
{
    foreach ($hookList as $hook) {
        try {
            $hook->filterMessage($context);
        } catch (\Throwable $e) {
            Logger::error('execute hook error. hookName=' . safe_hook_name($hook) . ': ' . $e->getMessage());
        }
    }
}

/**
 * 投递前过滤 = 客户端二次 tag 过滤 + FilterMessageHook（拉取/POP/pull 三处共用）。
 *
 * 钩子拿到的是**可变的** ``msgList``；被摘掉的消息由调用方决定处置方式：
 * 拉取路径 = 静默跳过（位点照常推进，不 ack，Java 亦然）；
 * POP 路径 = 必须立刻 ack，否则 invisibleTime 到期后会复活重投。
 *
 * @param list<object> $hookList
 * @param list<MessageExt> $msgs
 * @return list<MessageExt>
 */
function filter_messages_for_delivery(
    string $consumerGroup,
    array $hookList,
    MessageQueue $mq,
    ?SubscriptionData $sub,
    array $msgs,
    bool $unitMode = false,
): array {
    $out = client_side_tag_filter($sub, array_values($msgs));
    if ($out !== [] && $hookList !== []) {
        $context = new FilterMessageContext($consumerGroup, $out, $mq);
        $context->unitMode = $unitMode;    // Java DefaultMQPushConsumerImpl:640
        execute_filter_hooks($hookList, $context);
        $out = array_values($context->msgList);
    }
    return $out;
}

// ---------------- 本地位点文件格式（对齐 Java LocalFileOffsetStore）----------------
// Java 侧 fastjson2 序列化 Map<MessageQueue, AtomicLong> 时把 MessageQueue 对象**直接
// 当 JSON key** 写出 —— 严格 JSON 非法，但 fastjson2 自产自销能读回。字段序固定为
// brokerName/queueId/topic（fastjson2 字母序）。队列 key 仍用 topic+broker+queueId 拼接。

/**
 * @param array<string,int> $items key = topic.brokerName.queueId 拼接串
 * @param array<string,MessageQueue> $mqMap
 */
function build_local_offsets_json(array $items, array $mqMap): string
{
    $parts = [];
    foreach ($items as $key => $off) {
        $mq = $mqMap[$key] ?? null;
        if ($mq === null) {
            // 与 Java persistAll(mqs) 一样：只写仍持有队列信息的条目
            continue;
        }
        $obj = json_encode([
            'brokerName' => $mq->brokerName,
            'queueId' => (int) $mq->queueId,
            'topic' => $mq->topic,
        ], JSON_UNESCAPED_SLASHES);
        $parts[] = $obj . ':' . (string) (int) $off;
    }
    return '{"offsetTable":{' . implode(',', $parts) . '}}';
}

/**
 * 解析本地位点文件（严格 JSON 扁平 map 或 Java fastjson2 的「对象作 key」格式）。
 *
 * @return array<string,int>|null key = topic.brokerName.queueId 拼接串
 */
function parse_local_offsets_json(?string $text): ?array
{
    if ($text === null || trim($text) === '') {
        return null;
    }
    $stripped = trim($text);
    // 先按严格 JSON 解析（旧版本端写过的扁平 map）；Java fastjson2 的"对象作 key"
    // 格式对严格解析必然失败（含 pretty 版），落进下面的容忍扫描器。
    $strict = json_decode($stripped, true);
    if (is_array($strict)) {
        $out = [];
        $ok = true;
        foreach ($strict as $k => $v) {
            if (!is_string($k) || !is_numeric($v)) {
                $ok = false;
                break;
            }
            $out[$k] = (int) $v;
        }
        if ($ok) {
            return $out;
        }
    }
    $body = scan_offset_table_body($stripped);
    if ($body === null) {
        return null;
    }
    $out = [];
    foreach ($body as [$topic, $broker, $qid, $off]) {
        $out[$topic . $broker . (string) $qid] = $off;
    }
    return $out;
}

/**
 * 容忍解析 {"offsetTable":{{..}:off,{..}:off}}；失败返回 null。
 *
 * @return list<array{0:string,1:string,2:int,3:int}>|null 元素 = [topic, brokerName, queueId, offset]
 */
function scan_offset_table_body(string $text): ?array
{
    if (!preg_match('/^\{\s*"offsetTable"\s*:\s*\{(.*)\}\s*\}\s*$/s', $text, $m)) {
        return null;
    }
    $body = trim($m[1]);
    $entries = [];
    if ($body === '') {
        return $entries;
    }
    $pos = 0;
    // 对应 Python _OBJ_AS_KEY_RE：(\{[^{}]*\})\s*:\s*(-?\d+)
    if (!preg_match_all('/(\{[^{}]*\})\s*:\s*(-?\d+)/s', $body, $matches, PREG_OFFSET_CAPTURE)) {
        return null;
    }
    foreach ($matches[0] as $i => $whole) {
        [$full, $start] = $whole;
        $objBody = $matches[1][$i][0];
        $offsetVal = (int) $matches[2][$i][0];
        if (trim(substr($body, $pos, $start - $pos)) !== ''
            && trim(substr($body, $pos, $start - $pos)) !== ',') {
            return null;
        }
        $fields = [];
        // 对应 Python _FIELD_RE："(\w+)"\s*:\s*("(?:[^"\\]|\\.)*"|-?\d+)
        if (preg_match_all('/"(\w+)"\s*:\s*("(?:[^"\\\\]|\\\\.)*"|-?\d+)/s', $objBody, $fm, PREG_SET_ORDER)) {
            foreach ($fm as $fo) {
                $raw = $fo[2];
                if ($raw !== '' && $raw[0] === '"') {
                    $decoded = json_decode($raw, true);
                    if (!is_string($decoded)) {
                        return null;
                    }
                    $fields[$fo[1]] = $decoded;
                } else {
                    $fields[$fo[1]] = (int) $raw;
                }
            }
        }
        if (!isset($fields['topic'], $fields['brokerName'], $fields['queueId'])) {
            return null;
        }
        $entries[] = [(string) $fields['topic'], (string) $fields['brokerName'],
            (int) $fields['queueId'], $offsetVal];
        $pos = $start + strlen($full);
    }
    if (trim(substr($body, $pos)) !== '') {
        return null;
    }
    return $entries;
}

/**
 * 对应 Java `MessageQueue#toString`（一致性哈希要哈希**它**，不是 PHP 的 repr）。
 *
 * ⚠ 必须逐字符等于 Java 的 `"MessageQueue [topic=.., brokerName=.., queueId=..]"`。
 */
function java_message_queue_string(MessageQueue $mq): string
{
    return sprintf('MessageQueue [topic=%s, brokerName=%s, queueId=%d]',
        $mq->topic, $mq->brokerName, $mq->queueId);
}

/**
 * 对应 Java `String#split(String)`（limit=0）：**丢掉末尾的空段**。
 *
 * PHP 的 `explode` 保留尾空段，`AllocateMessageQueueByMachineRoom` 恰好按
 * `count == 2` 判合法，差异会直接改变一条队列参不参与分配：
 *
 * | 输入 | Java | PHP 原生 |
 * |---|---|---|
 * | `"room1@"` | `["room1"]`（1 段，剔除） | `["room1", ""]`（2 段，误收） |
 * | `"room1@b@"` | `["room1", "b"]`（2 段，收） | `["room1", "b", ""]`（3 段，误剔） |
 * | `"@"` | `[]` | `["", ""]` |
 * | `""` / `"broker-a"` | 无分隔符命中时**整串原样返回**，哪怕是空串 | 同 |
 *
 * 最后一行是 Java `Pattern#split` 里 "If no match was found, return this" 那个早返回，
 * 所以不能无条件裁尾（否则 `""` 会变成 `[]`）。
 *
 * @return list<string>
 */
function java_split(string $text, string $sep): array
{
    $parts = explode($sep, $text);
    if (count($parts) === 1) {
        return $parts; // 没命中分隔符：Java 原样返回整串
    }
    while ($parts !== [] && end($parts) === '') {
        array_pop($parts);
    }
    return array_values($parts);
}

/**
 * POP 模式的队列状态（对应 org.apache.rocketmq.client.impl.consumer.PopProcessQueue）。
 *
 * 与 pull 模式的 ProcessQueue 不同，POP **没有"已拉未消费"缓冲**：消息一弹出就交给
 * 消费流程，确认靠 ack。这里只跟踪两件事：
 *
 * - ``waitAckCounter``：已弹出但还没 ack / 还没延长不可见时间的条数，用于流控；
 * - ``dropped``：队列是否已被 rebalance 撤走（撤走后本批消息不再消费、也不 ack，
 *   交给 invisibleTime 到期后 broker 自动复活重投）。
 *
 * PHP 单线程模型下不需要锁（PORTING.md「异步模型」）。
 */
final class PopProcessQueue
{
    private int $waitAckCounter = 0;
    private bool $dropped = false;

    /** 最近一次发起弹出的时刻（Java lastPopTimestamp），float 秒。 */
    public float $lastPopTimestamp = 0.0;

    public function __construct()
    {
        $this->lastPopTimestamp = microtime(true);
    }

    public function incFoundMsg(int $count): void
    {
        $this->waitAckCounter += $count;
    }

    /** Java 传的是负数（decFoundMsg(-msgs.size())），这里按"减多少"理解。 */
    public function decFoundMsg(int $count): void
    {
        $this->waitAckCounter += $count;
    }

    public function ack(): int
    {
        $this->waitAckCounter -= 1;
        return $this->waitAckCounter;
    }

    public function waitAckCount(): int
    {
        return $this->waitAckCounter;
    }

    public function isDropped(): bool
    {
        return $this->dropped;
    }

    public function setDropped(bool $dropped): void
    {
        $this->dropped = $dropped;
    }
}

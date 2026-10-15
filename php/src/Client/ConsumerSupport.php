<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\SubscriptionData;

/**
 * Common consumer-side foundation: module constants, module-level helper functions, and PopProcessQueue.
 *
 * Allocation strategies (AllocateMessageQueue*) are implemented in Allocation.php and not
 * repeated here; consume/pull result types live in ConsumerResult.php.
 *
 * WARNING: PHP's autoloader only loads classes, not functions: the global functions in this
 * file are defined along with this file being autoloaded (when any class is first
 * referenced). If a caller only calls functions without touching classes, it must
 * require this file first (each consumer file already has this guard in its header).
 */

/** Consumer-side default constants. */
final class ConsumerDefaults
{
    /**
     * Delay level table (seconds) for POP redelivery after a consume failure: pick a level
     * by how long the message has already lived; if still not consumed successfully past
     * twice the longest level, ack and drop it directly.
     * @var list<int>
     */
    public const POP_DELAY_LEVEL = [10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600,
        1200, 1800, 3600, 7200];

    /** Legal range of POP invisible time (milliseconds); anything out of range falls back to 60000 */
    public const MIN_POP_INVISIBLE_TIME = 5000;
    public const MAX_POP_INVISIBLE_TIME = 300000;

    /** pop request initMode field: 0 = consume from the queue's minimum offset, 1 = from the maximum offset */
    public const CONSUME_INIT_MODE_MIN = 0;
    public const CONSUME_INIT_MODE_MAX = 1;

    /** int32 upper bound: default for the orderly-consume "retries exhausted" check (-1 reads as it on this chain). */
    public const INT32_MAX = 0x7FFFFFFF;

    /**
     * Pull idle cap PULL_MAX_IDLE_TIME (default 120000ms).
     * A queue still owned by this instance has not initiated any pull/pop for this long
     * => rebalance drops and rebuilds it.
     */
    public const PULL_MAX_IDLE_TIME = 120.0;

    /**
     * Pull backoff duration after suspend() (1000ms). PHP has no loop threads, so the
     * backoff is borne by the caller's tick cadence (see PushConsumer::suspend()); this
     * constant locks in a semantic difference: suspend does **not** drop the queue,
     * whereas PULL_MAX_IDLE_TIME asks "is this loop still alive".
     */
    public const PULL_TIME_DELAY_WHEN_SUSPEND = 1.0;
}

/** Queue sort key: topic -> brokerName -> queueId. */
function consumer_mq_sort_key(MessageQueue $mq): array
{
    // rebalance must sort mqAll/cidAll before allocating; inconsistent ordering makes
    // different instances compute different allocation results (the same queue consumed
    // by two instances at once).
    return [$mq->topic, $mq->brokerName, $mq->queueId];
}

function safe_hook_name(object $hook): string
{
    try {
        if (method_exists($hook, 'hookName')) {
            return (string) $hook->hookName();
        }
    } catch (\Throwable) {
        // fall back to the class name
    }
    return get_class($hook);
}

/**
 * Client-side secondary tag filtering.
 *
 * The broker side filters by the tag **hash (codeSet)**, which allows false positives
 * from hash collisions, so the client must check the strings again. The guard
 * `!tagsSet.isEmpty() && !isClassFilterMode` means: when subscribed to `"*"` (SUB_ALL)
 * no filtering happens -- so `FilterAPI::buildSubscriptionData` must keep tagsSet empty
 * for SUB_ALL (detailed comments there).
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
        // WARNING: tagsSet is a list (addTag appends via in_array), not a tag->tag map;
        //   looking up with isset($tagsSet[$tag]) always misses and would filter out the
        //   whole batch.
        if ($tags !== null && in_array($tags, $sub->tagsSet, true)) {
            $out[] = $m;
        }
    }
    return $out;
}

/**
 * Execute the filter hooks in order, **swallowing all exceptions**, logging only one error line.
 *
 * Unlike send/consume hooks: a filter hook failure must not affect consumption, it just
 * means that filtering pass does not take effect.
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
 * Pre-delivery filtering = client-side secondary tag filtering + FilterMessageHook
 * (shared by the pull/POP/pull three call sites).
 *
 * The hooks receive a **mutable** ``msgList``; removed messages are disposed of by the
 * caller: the pull path = silently skip (offset advances as usual, no ack);
 * the POP path = must ack immediately, otherwise the message revives and is redelivered
 * after invisibleTime expires.
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
        $context->unitMode = $unitMode;    // unitized deployment marker; hooks use it to tell local-unit from cross-unit messages
        execute_filter_hooks($hookList, $context);
        $out = array_values($context->msgList);
    }
    return $out;
}

// ---------------- Local offset file format ----------------
// The MessageQueue -> offset map in offset.json writes the MessageQueue object
// **directly as a JSON key** -- not strict JSON, but this file can be parsed back by
// itself. The field order inside the key is fixed as brokerName/queueId/topic
// (alphabetical). In-memory queue keys still use topic+broker+queueId concatenation.

/**
 * @param array<string,int> $items key = topic.brokerName.queueId concatenated string
 * @param array<string,MessageQueue> $mqMap
 */
function build_local_offsets_json(array $items, array $mqMap): string
{
    $parts = [];
    foreach ($items as $key => $off) {
        $mq = $mqMap[$key] ?? null;
        if ($mq === null) {
            // Only write entries that still have queue info: queues present in the offset
            // table but no longer found in the route table are skipped
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
 * Parse the local offset file (either a strict-JSON flat map or the "object as key" format).
 *
 * @return array<string,int>|null key = topic.brokerName.queueId concatenated string
 */
function parse_local_offsets_json(?string $text): ?array
{
    if ($text === null || trim($text) === '') {
        return null;
    }
    $stripped = trim($text);
    // First parse as strict JSON (the flat map old versions wrote); the "object as key"
    // format necessarily fails strict parsing (including the pretty variant) and falls
    // through to the tolerant scanner below.
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
 * Tolerantly parse {"offsetTable":{{..}:off,{..}:off}}; returns null on failure.
 *
 * @return list<array{0:string,1:string,2:int,3:int}>|null element = [topic, brokerName, queueId, offset]
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
    // Match pattern for "object as key": (\{[^{}]*\})\s*:\s*(-?\d+)
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
        // Field match pattern: "(\w+)"\s*:\s*("(?:[^"\\]|\\.)*"|-?\d+)
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
 * The queue's online string form: consistent hashing hashes **it**, not PHP's object repr.
 *
 * WARNING: it must equal `"MessageQueue [topic=.., brokerName=.., queueId=..]"` character
 * for character; a different wording changes the hash value on the ring and shifts queue
 * ownership wholesale.
 */
function wire_queue_key_string(MessageQueue $mq): string
{
    return sprintf('MessageQueue [topic=%s, brokerName=%s, queueId=%d]',
        $mq->topic, $mq->brokerName, $mq->queueId);
}

/**
 * Split a string by the "trailing empty segments trimmed" rule (equivalent to a limit=0 split).
 *
 * PHP's `explode` keeps trailing empty segments, while `AllocateMessageQueueByMachineRoom`
 * judges validity exactly by `count == 2`, so the difference directly changes whether a
 * queue participates in allocation:
 *
 * | input | this function | native explode |
 * |---|---|---|
 * | `"room1@"` | `["room1"]` (1 segment, excluded) | `["room1", ""]` (2 segments, wrongly included) |
 * | `"room1@b@"` | `["room1", "b"]` (2 segments, included) | `["room1", "b", ""]` (3 segments, wrongly excluded) |
 * | `"@"` | `[]` | `["", ""]` |
 * | `""` / `"broker-a"` | when no separator matches, **the whole string is returned as-is**, even if empty | same |
 *
 * The last row is the easiest pitfall: when no separator matches the whole string must be
 * returned as-is, so you cannot trim trailing segments unconditionally (otherwise `""`
 * would become `[]`).
 *
 * @return list<string>
 */
function wire_split(string $text, string $sep): array
{
    $parts = explode($sep, $text);
    if (count($parts) === 1) {
        return $parts; // no separator match: return the whole string as-is
    }
    while ($parts !== [] && end($parts) === '') {
        array_pop($parts);
    }
    return array_values($parts);
}

/**
 * Queue state in POP mode.
 *
 * Unlike pull mode's ProcessQueue, POP has **no "pulled but not consumed" buffer**: a
 * message is handed to the consume flow as soon as it is popped, and confirmation is by
 * ack. Only two things are tracked here:
 *
 * - ``waitAckCounter``: the count popped but not yet acked / not yet extended with a new
 *   invisible time, used for flow control;
 * - ``dropped``: whether the queue has already been dropped by rebalance (after being
 *   dropped, this batch is neither consumed nor acked; it is left to the broker to
 *   revive and redeliver automatically after invisibleTime expires).
 *
 * No locks are needed under PHP's single-threaded model.
 */
final class PopProcessQueue
{
    private int $waitAckCounter = 0;
    private bool $dropped = false;

    /** Timestamp of the most recent pop initiation, float seconds. */
    public float $lastPopTimestamp = 0.0;

    public function __construct()
    {
        $this->lastPopTimestamp = microtime(true);
    }

    public function incFoundMsg(int $count): void
    {
        $this->waitAckCounter += $count;
    }

    /** Decrement follows the protocol-side convention: the caller passes a negative number (e.g. -count($msgs)); internally it just accumulates. */
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

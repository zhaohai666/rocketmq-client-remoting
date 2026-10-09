<?php

declare(strict_types=1);

/**
 * 消费者套件自测（DefaultMQPushConsumer / DefaultMQPullConsumer / DefaultLitePullConsumer
 * + ConsumerSupport 公共基础件）。不依赖 phpunit，不连真机。
 *
 * 运行：php tests/RunClientConsumer.php（或经 tests/run_all.php）
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
 *
 * 网络用例与 RunClientInstance 同款：本文件以 `--broker` 参数二次拉起自身作为
 * **子进程**假 broker。与实例套件的差别是这里的 broker **有状态**：
 *   - 每队列拉取计数（首拉回 FOUND / OFFSET_ILLEGAL，后续回 NO_NEW_MSG）；
 *   - OTopic 的 QUERY_CONSUMER_OFFSET 在"发生过 OFFSET_ILLEGAL"后返回 77；
 *   - 把 PULL / UPDATE_CONSUMER_OFFSET / CONSUMER_SEND_MSG_BACK / LOCK_BATCH_MQ
 *     请求按行写进 RMQ_LOG 指定的 JSONL 文件，父进程据此断言线上行为
 *     （sysFlag 的 suspend 位、回投 delayLevel、锁队列 mqSet、位点提交值）。
 * 消费者列表（rebalance 用）从 RMQ_CONSUMER_MAP 指定的 JSON 文件读：{group: [cid,...]}。
 */

require_once __DIR__ . '/../bootstrap.php';

// autoloader 只加载类不加载函数：本套件直接调 ConsumerSupport 的全局函数，先保证文件已加载。
if (!function_exists('RocketMQ\\Client\\java_split')) {
    require_once __DIR__ . '/../src/Client/ConsumerSupport.php';
}

use RocketMQ\Client\ConsumerDefaults;
use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\DefaultMQPushConsumer;
use RocketMQ\Client\PopProcessQueue;
use RocketMQ\Client\SimpleMessageListener;
use RocketMQ\Client\ConsumeConcurrentlyContext;
use RocketMQ\Client\ConsumeConcurrentlyStatus;
use RocketMQ\Client\ConsumeOrderlyContext;
use RocketMQ\Client\ConsumeOrderlyStatus;
use RocketMQ\Client\FilterMessageContext;
use RocketMQ\Client\MessageListenerConcurrently;
use RocketMQ\Client\MessageListenerOrderly;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\PullStatus;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PullSysFlag;
use RocketMQ\Common\SubscriptionData;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\PullMessageResponseHeader;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\RocketMQSerializable;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Common\PermName;
use function RocketMQ\Client\build_local_offsets_json;
use function RocketMQ\Client\client_side_tag_filter;
use function RocketMQ\Client\filter_messages_for_delivery;
use function RocketMQ\Client\java_message_queue_string;
use function RocketMQ\Client\java_split;
use function RocketMQ\Client\parse_local_offsets_json;
use function RocketMQ\Client\scan_offset_table_body;
use function RocketMQ\Remoting\Protocol\message_queue_key;

// ======================================================================
// 子进程假 broker：`php RunClientConsumer.php --broker`
// ======================================================================

if (($argv[1] ?? '') === '--broker') {
    consBrokerMain();
    exit(0);
}

/** 各 topic 的 FOUND 剧本：body 前缀 / 条数 / 首轮后是否转 NO_NEW_MSG。 */
function consPullPlan(string $topic, int $seq, string $reqCode): array
{
    // 返回 [responseCode, nextBeginOffset, list<array{body, offset}>]
    switch ($topic) {
        case 'CTopic':
        case 'RTopic':
        case 'PTopic':
        case 'STopic':
            $prefix = ['CTopic' => 'push', 'RTopic' => 'retry', 'PTopic' => 'part', 'STopic' => 'order'][$topic];
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 3, [
                    ['body' => "{$prefix}-0", 'offset' => 0],
                    ['body' => "{$prefix}-1", 'offset' => 1],
                    ['body' => "{$prefix}-2", 'offset' => 2],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 3, []];
        case 'PTopic2': // 预留
            return [ResponseCode::PULL_NOT_FOUND, 0, []];
        case 'FTopic':
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 3, [
                    ['body' => 'f-0', 'offset' => 0],
                    ['body' => 'f-1', 'offset' => 1],
                    ['body' => 'f-2', 'offset' => 2],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 3, []];
        case 'OTopic':
            if ($seq === 1) {
                $GLOBALS['CONS_OTOPIC_ILLEGAL'] = true;
                return [ResponseCode::PULL_OFFSET_MOVED, 77, []];
            }
            return [ResponseCode::PULL_NOT_FOUND, 77, []];
        case 'DTopic':
            return [ResponseCode::SUCCESS, 5, [['body' => 'direct-4', 'offset' => 4]]];
        case 'LTopic':
            if ($reqCode != RequestCode::LITE_PULL_MESSAGE) {
                return [ResponseCode::PULL_NOT_FOUND, 0, []];
            }
            if ($seq === 1) {
                return [ResponseCode::SUCCESS, 2, [
                    ['body' => 'lite-0', 'offset' => 0],
                    ['body' => 'lite-1', 'offset' => 1],
                ]];
            }
            return [ResponseCode::PULL_NOT_FOUND, 2, []];
        default:
            return [ResponseCode::PULL_NOT_FOUND, 0, []];
    }
}

function consLog(array $entry): void
{
    $path = getenv('RMQ_LOG');
    if ($path === false || $path === '') {
        return;
    }
    @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
}

/** 读满 n 字节；EOF 返回 null。 */
function consReadExact($conn, int $n): ?string
{
    $buf = '';
    while (strlen($buf) < $n) {
        $chunk = @fread($conn, $n - strlen($buf));
        if ($chunk === false || $chunk === '') {
            return null;
        }
        $buf .= $chunk;
    }
    return $buf;
}

/** 读一整帧；EOF 返回 null。 */
function consReadFrame($conn): ?RemotingCommand
{
    $head = consReadExact($conn, 4);
    if ($head === null) {
        return null;
    }
    $total = RocketMQSerializable::unpackSignedInt($head);
    if ($total <= 0) {
        return null;
    }
    $rest = consReadExact($conn, $total);
    if ($rest === null) {
        return null;
    }
    return RemotingCommand::decode($head . $rest);
}

/** 构造一条 route body：单 broker 单读写队列，全部指向假 broker 自身。 */
function consRouteBody(string $addr): string
{
    $qd = new QueueData('b-cons', 1, 1, PermName::PERM_READ | PermName::PERM_WRITE, 0);
    $bd = new BrokerData('DefaultCluster', 'b-cons', [MixAll::MASTER_ID => $addr]);
    $trd = new TopicRouteData();
    $trd->queueDatas = [$qd];
    $trd->brokerDatas = [$bd];
    return $trd->encode();
}

/** 用 17 段格式编码一条消息，拼进 PULL/LITE_PULL 响应体。 */
function consEncodeMsg(string $topic, string $body, int $queueOffset): string
{
    $m = new MessageExt(topic: $topic, body: $body);
    $m->setQueueId(0);
    $m->setQueueOffset($queueOffset);
    $m->setBornTimestamp(1_700_000_000_000);
    $m->setStoreTimestamp(1_700_000_000_000);
    return MessageDecoder::encodeMessageExt($m);
}

function consRespond(RemotingCommand $cmd): ?RemotingCommand
{
    $ext = $cmd->extFields;
    $topic = (string) ($ext['topic'] ?? '');

    switch ($cmd->code) {
        case RequestCode::GET_ROUTEINFO_BY_TOPIC:
            if (str_starts_with($topic, '%RETRY%')) {
                return RemotingCommand::createResponseCommand(ResponseCode::TOPIC_NOT_EXIST);
            }
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = consRouteBody($GLOBALS['CONS_BROKER_ADDR']);
            return $resp;

        case RequestCode::GET_CONSUMER_LIST_BY_GROUP: {
            $mapPath = getenv('RMQ_CONSUMER_MAP');
            $map = is_string($mapPath) && is_file($mapPath)
                ? (array) json_decode((string) file_get_contents($mapPath), true) : [];
            $cids = $map[(string) ($ext['consumerGroup'] ?? '')] ?? [];
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->body = json_encode(['consumerIdList' => array_values($cids)]);
            return $resp;
        }

        case RequestCode::QUERY_CONSUMER_OFFSET: {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            if ($topic === 'OTopic') {
                $off = !empty($GLOBALS['CONS_OTOPIC_ILLEGAL']) ? 77 : 0;
            } else {
                $off = match ($topic) {
                    'CTopic', 'RTopic', 'PTopic', 'STopic', 'LTopic', 'FTopic' => 0,
                    'DTopic' => 7,
                    default => null,
                };
            }
            if ($off === null) {
                return RemotingCommand::createResponseCommand(ResponseCode::QUERY_NOT_FOUND);
            }
            $resp->extFields['offset'] = (string) $off;
            return $resp;
        }

        case RequestCode::GET_MAX_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '100';
            return $resp;

        case RequestCode::GET_MIN_OFFSET:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '0';
            return $resp;

        case RequestCode::SEARCH_OFFSET_BY_TIMESTAMP:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['offset'] = '55';
            return $resp;

        case RequestCode::GET_EARLIEST_MSG_STORETIME:
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            $resp->extFields['timestamp'] = '1234567890000';
            return $resp;

        case RequestCode::PULL_MESSAGE:
        case RequestCode::LITE_PULL_MESSAGE: {
            $seq = ($GLOBALS['CONS_PULL_SEEN'][$topic] = ($GLOBALS['CONS_PULL_SEEN'][$topic] ?? 0) + 1);
            consLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'queueOffset' => (int) ($ext['queueOffset'] ?? 0),
                'sysFlag' => (int) ($ext['sysFlag'] ?? 0),
                'group' => (string) ($ext['consumerGroup'] ?? ''),
                'seq' => $seq,
            ]);
            [$code, $next, $msgs] = consPullPlan($topic, $seq, (string) $cmd->code);
            $h = new PullMessageResponseHeader(
                nextBeginOffset: $next,
                minOffset: 0,
                maxOffset: 100,
                suggestWhichBrokerId: MixAll::MASTER_ID,
            );
            $resp = RemotingCommand::createResponseCommandWithHeader($code, $h);
            if ($msgs !== []) {
                $body = '';
                foreach ($msgs as $m) {
                    $body .= consEncodeMsg($topic, $m['body'], $m['offset']);
                }
                $resp->body = $body;
            }
            return $resp;
        }

        case RequestCode::UPDATE_CONSUMER_OFFSET:
            consLog([
                'code' => $cmd->code,
                'topic' => $topic,
                'queueId' => (int) ($ext['queueId'] ?? 0),
                'commitOffset' => (int) ($ext['commitOffset'] ?? -1),
                'group' => (string) ($ext['consumerGroup'] ?? ''),
            ]);
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);

        case RequestCode::CONSUMER_SEND_MSG_BACK:
            consLog([
                'code' => $cmd->code,
                'group' => (string) ($ext['group'] ?? ''),
                'originTopic' => (string) ($ext['originTopic'] ?? ''),
                'originMsgId' => (string) ($ext['originMsgId'] ?? ''),
                'delayLevel' => (int) ($ext['delayLevel'] ?? 0),
                'maxReconsumeTimes' => isset($ext['maxReconsumeTimes']) ? (int) $ext['maxReconsumeTimes'] : null,
            ]);
            // FTopic 的回投一律失败：验证 C4 地板（回投失败条目卡住位点提交）。
            if (($ext['originTopic'] ?? '') === 'FTopic') {
                return RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'send back rejected');
            }
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);

        case RequestCode::LOCK_BATCH_MQ:
        case RequestCode::UNLOCK_BATCH_MQ: {
            $reqBody = (array) (json_decode($cmd->body ?? '{}', true) ?: []);
            $mqSet = array_values((array) ($reqBody['mqSet'] ?? []));
            consLog([
                'code' => $cmd->code,
                'group' => (string) ($reqBody['consumerGroup'] ?? ''),
                'clientId' => (string) ($reqBody['clientId'] ?? ''),
                'mqSet' => $mqSet,
            ]);
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
            if ($cmd->code === RequestCode::LOCK_BATCH_MQ) {
                // 全部锁成功：把请求里的 mqSet 原样回进 lockOKMQSet。
                $resp->body = json_encode(['lockOKMQSet' => $mqSet]);
            }
            return $resp;
        }

        default:
            return RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }
}

function consServeConn($conn): void
{
    stream_set_blocking($conn, true);
    while (($cmd = consReadFrame($conn)) !== null) {
        try {
            $resp = consRespond($cmd);
        } catch (\Throwable $e) {
            $resp = RemotingCommand::createResponseCommand(ResponseCode::SYSTEM_ERROR, 'broker error: ' . $e->getMessage());
        }
        if ($resp !== null) {
            $resp->opaque = $cmd->opaque;
            @fwrite($conn, $resp->encode());
        }
    }
}

function consBrokerMain(): void
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if ($server === false) {
        fwrite(STDERR, "broker: cannot listen: {$errstr}\n");
        return;
    }
    $GLOBALS['CONS_BROKER_ADDR'] = (string) stream_socket_get_name($server, false);
    $GLOBALS['CONS_PULL_SEEN'] = [];
    $GLOBALS['CONS_OTOPIC_ILLEGAL'] = false;
    fwrite(STDOUT, $GLOBALS['CONS_BROKER_ADDR'] . "\n");
    fflush(STDOUT);

    while (true) {
        $conn = @stream_socket_accept($server, 30);
        if ($conn === false) {
            break;
        }
        consServeConn($conn);
        @fclose($conn);
    }
    @fclose($server);
}

/**
 * 子进程假 broker 句柄（与 RunClientInstance 同形，独立声明避免跨文件 exit 污染）。
 */
final class ConsFakeBroker
{
    /** @var resource */
    private $proc;

    /** @var array<int, resource> */
    private array $pipes;

    public string $addr;

    private function __construct($proc, array $pipes, string $addr)
    {
        $this->proc = $proc;
        $this->pipes = $pipes;
        $this->addr = $addr;
    }

    public static function start(): self
    {
        $cmd = [PHP_BINARY, __FILE__, '--broker'];
        $desc = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $proc = proc_open($cmd, $desc, $pipes);
        if (!is_resource($proc)) {
            throw new \RuntimeException('cannot spawn fake broker');
        }
        $line = fgets($pipes[1]);
        $addr = $line === false ? '' : trim($line);
        if ($addr === '') {
            $err = stream_get_contents($pipes[2]) ?: '';
            proc_terminate($proc);
            proc_close($proc);
            throw new \RuntimeException('fake broker did not report address: ' . $err);
        }
        return new self($proc, $pipes, $addr);
    }

    public function stop(): void
    {
        foreach ($this->pipes as $p) {
            if (is_resource($p)) {
                @fclose($p);
            }
        }
        if (is_resource($this->proc)) {
            @proc_terminate($this->proc);
            @proc_close($this->proc);
        }
    }
}

// ======================================================================
// 测试用监听器
// ======================================================================

/**
 * 并发监听器：按首条消息的 topic 分流（CTopic 收下 / RTopic 重投 / PTopic 只认可第 1 条）。
 */
final class ConsTopicListener implements MessageListenerConcurrently
{
    /** @var array<string,int> */
    private array $calls = [];

    /** @param array<string,mixed> $behavior topic => ['status'=>..., 'ackIndex'=>int|null, 'ackIndexOnlyFirst'=>bool] */
    public function __construct(private array $behavior)
    {
    }

    public function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context): ConsumeConcurrentlyStatus
    {
        $topic = (string) ($msgs[0]->topic ?? '');
        foreach ($msgs as $m) {
            $GLOBALS['RECV'][$topic][] = (string) $m->getBody();
        }
        $this->calls[$topic] = ($this->calls[$topic] ?? 0) + 1;
        $b = $this->behavior[$topic] ?? ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null];
        if ($b['ackIndex'] !== null) {
            $context->ackIndex = (int) $b['ackIndex'];
        }
        if (!empty($b['ackIndexOnlyFirst']) && $this->calls[$topic] === 1) {
            // 首次只认可第 1 条（触发部分 ack 回投）；重排后的再消费整批认可，终结循环。
            $context->ackIndex = 0;
        }
        return $b['status'];
    }
}

/** 顺序监听器：第一次挂起、第二次成功（重投场景的骨架）。 */
final class ConsOrderlyListener implements MessageListenerOrderly
{
    public int $calls = 0;

    public function consumeMessage(array $msgs, ConsumeOrderlyContext $context): ConsumeOrderlyStatus
    {
        $this->calls++;
        foreach ($msgs as $m) {
            $GLOBALS['RECV']['STopic'][] = (string) $m->getBody();
        }
        return $this->calls === 1
            ? ConsumeOrderlyStatus::SUSPEND_CURRENT_QUEUE_A_MOMENT
            : ConsumeOrderlyStatus::SUCCESS;
    }
}

// ======================================================================
// 测试 runner
// ======================================================================

final class RunClientConsumer
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    /** @var list<array<string,mixed>> */
    private array $log = [];

    public function check(bool $cond, string $label): void
    {
        if ($cond) {
            $this->passed++;
        } else {
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$label}\n");
        }
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
        } else {
            $detail = sprintf("%s\n  expected: %s\n  actual:   %s", $label, var_export($expected, true), var_export($actual, true));
            $this->failures[] = $label;
            fwrite(STDERR, "FAIL: {$detail}\n");
        }
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

    // ---------------- 日志辅助 ----------------

    private function loadLog(string $path): void
    {
        $this->log = [];
        foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $d = json_decode($line, true);
            if (is_array($d)) {
                $this->log[] = $d;
            }
        }
    }

    /** @return list<array<string,mixed>> */
    private function logWhere(int $code, ?string $topic = null, ?string $group = null): array
    {
        $out = [];
        foreach ($this->log as $e) {
            if (($e['code'] ?? 0) !== $code) {
                continue;
            }
            if ($topic !== null && ($e['topic'] ?? null) !== $topic) {
                continue;
            }
            if ($group !== null && ($e['group'] ?? null) !== $group) {
                continue;
            }
            $out[] = $e;
        }
        return $out;
    }

    private function cleanup(DefaultMQPushConsumer|DefaultMQPullConsumer|DefaultLitePullConsumer $c): void
    {
        $c->shutdown();
        if ($c->clientId !== null) {
            MQClientInstance::removeInstance($c->clientId);
        }
    }

    // ================================================================ 1. ConsumerSupport 纯逻辑

    private function testConsumerSupportUnit(): void
    {
        // java_split：Java String#split 的丢尾空段语义。
        $this->checkSame(['room1'], java_split('room1@', '@'), 'java_split 丢尾空段');
        $this->checkSame(['room1', 'b'], java_split('room1@b@', '@'), 'java_split 中段保留尾剔除');
        $this->checkSame([], java_split('@', '@'), 'java_split 全分隔符 → 空');
        $this->checkSame([''], java_split('', '@'), 'java_split 无命中原样返回（含空串）');
        $this->checkSame(['abc'], java_split('abc', '@'), 'java_split 无命中原样返回');

        // java_message_queue_string：一致性哈希的输入，必须逐字符对齐 Java。
        $mq = new MessageQueue('T', 'b', 3);
        $this->checkSame('MessageQueue [topic=T, brokerName=b, queueId=3]', java_message_queue_string($mq),
            'java_message_queue_string 格式');

        // 本地位点 JSON：严格扁平 map 往返。
        $items = ['Tb1' => 42, 'Tb2' => 7];
        $mqMap = ['Tb1' => new MessageQueue('T', 'b', 1), 'Tb2' => new MessageQueue('T', 'b', 2)];
        $json = build_local_offsets_json($items, $mqMap);
        $this->checkSame($items, parse_local_offsets_json($json), '本地位点 JSON 往返');
        // fastjson2「对象作 key」格式（Java 写的文件）。
        $javaStyle = '{"offsetTable":{{"brokerName":"b","queueId":1,"topic":"T"}:42,'
            . '{"brokerName":"b","queueId":2,"topic":"T"}:7}}';
        $this->checkSame($items, parse_local_offsets_json($javaStyle), 'fastjson2 对象键格式解析');
        $pretty = "{\n  \"offsetTable\" : {\n    {\"brokerName\":\"b\",\"queueId\":1,\"topic\":\"T\"} : 42\n  }\n}";
        $this->checkSame(['Tb1' => 42], parse_local_offsets_json($pretty), 'fastjson2 对象键 pretty 容忍');
        $this->checkSame(null, parse_local_offsets_json('not json'), '坏输入返回 null');
        $this->checkSame(null, parse_local_offsets_json(''), '空串返回 null');
        $this->checkSame(null, scan_offset_table_body('{"offsetTable":garbage}'), 'offsetTable 体损坏返回 null');

        // 客户端二次 tag 过滤（订阅表走真实构建入口 FilterAPI）。
        $sub = \RocketMQ\Common\FilterAPI::buildSubscriptionData('T', 'TagA || TagB');
        $mTagA = new MessageExt(topic: 'T', body: 'a');
        $mTagA->setTags('TagA');
        $mTagC = new MessageExt(topic: 'T', body: 'c');
        $mTagC->setTags('TagC');
        $kept = client_side_tag_filter($sub, [$mTagA, $mTagC]);
        $this->checkSame([$mTagA], $kept, 'tag 过滤保留命中项');
        $this->checkSame([$mTagA, $mTagC], client_side_tag_filter(null, [$mTagA, $mTagC]), '无订阅不过滤');
        // SUB_ALL：tagsSet 必须为空 → 不过滤。
        $subAll = new SubscriptionData('T', '*');
        $this->checkSame([$mTagA, $mTagC], client_side_tag_filter($subAll, [$mTagA, $mTagC]), 'SUB_ALL 空 tagsSet 放行');

        // 投递前过滤钩子：钩子可摘消息、异常一律吞掉。
        $mDrop = new MessageExt(topic: 'T', body: 'drop');
        $dropper = new class {
            public function filterMessage(FilterMessageContext $ctx): void
            {
                $ctx->msgList = array_values(array_filter(
                    $ctx->msgList,
                    static fn(MessageExt $m): bool => $m->getBody() !== 'drop',
                ));
            }
        };
        $bomber = new class {
            public function filterMessage(FilterMessageContext $ctx): void
            {
                throw new \RuntimeException('hook boom');
            }
        };
        $out = filter_messages_for_delivery('G', [$dropper, $bomber], $mq, null, [$mTagA, $mDrop]);
        $this->checkSame(['a'], array_map(static fn(MessageExt $m) => $m->getBody(), $out),
            '钩子摘除 drop 消息且异常被吞');

        // PopProcessQueue：waitAck 计数与 dropped 标志。
        $pq = new PopProcessQueue();
        $pq->incFoundMsg(3);
        $this->checkSame(3, $pq->waitAckCount(), 'PopProcessQueue incFoundMsg');
        $this->checkSame(2, $pq->ack(), 'PopProcessQueue ack 减一');
        $pq->decFoundMsg(-1);
        $this->checkSame(1, $pq->waitAckCount(), 'PopProcessQueue decFoundMsg（Java 语义传负数）');
        $this->check(!$pq->isDropped(), 'PopProcessQueue 默认未 dropped');
        $pq->setDropped(true);
        $this->check($pq->isDropped(), 'PopProcessQueue setDropped');

        // 消费侧常量对齐 Java 数值。
        $this->checkSame([10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200],
            ConsumerDefaults::POP_DELAY_LEVEL, 'POP_DELAY_LEVEL 16 档');
        $this->checkSame(5000, ConsumerDefaults::MIN_POP_INVISIBLE_TIME, 'MIN_POP_INVISIBLE_TIME');
        $this->checkSame(300000, ConsumerDefaults::MAX_POP_INVISIBLE_TIME, 'MAX_POP_INVISIBLE_TIME');
    }

    // ================================================================ 2. Push 并发：收满 / 重投 / 部分 ack / 位点纠错

    private function testPushConcurrent(ConsFakeBroker $broker): void
    {
        $GLOBALS['RECV'] = [];
        $listener = new ConsTopicListener([
            'CTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null],
            'RTopic' => ['status' => ConsumeConcurrentlyStatus::RECONSUME_LATER, 'ackIndex' => null],
            'PTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => 0],
            'FTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null,
                'ackIndexOnlyFirst' => true],
            'OTopic' => ['status' => ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 'ackIndex' => null],
        ]);
        $c = new DefaultMQPushConsumer('GID-C-CONS');
        $c->setInstanceName('CONS-C');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener($listener);
        $c->consumeMessageBatchMaxSize = 3; // 整批投递，部分 ack / 回投场景才 meaningful
        $c->subscribe('CTopic');
        $c->subscribe('RTopic');
        $c->subscribe('PTopic');
        $c->subscribe('FTopic');
        $c->subscribe('OTopic');
        $c->start();
        $this->check($c->isStarted(), 'push start 后 isStarted');
        $this->checkSame(MixAll::clientIdFor('CONS-C', null, false), $c->clientId, 'push clientId 确定性');
        $this->checkSame(5, $c->assignedQueueCount(), 'start 同步完成首轮 rebalance（5 队列）');

        // 第一轮 tick：5 队列各拉一次 FOUND + 一次 NO_NEW_MSG；分发消费全部完成。
        $c->tick();
        $this->checkSame(['push-0', 'push-1', 'push-2'], $GLOBALS['RECV']['CTopic'] ?? [], 'CTopic 收满 3 条');
        $this->checkSame(['retry-0', 'retry-1', 'retry-2'], $GLOBALS['RECV']['RTopic'] ?? [], 'RTopic 收满 3 条');
        $this->checkSame(['part-0', 'part-1', 'part-2'], $GLOBALS['RECV']['PTopic'] ?? [], 'PTopic 收满 3 条');
        // C4 地板语义：FTopic 回投 f-1/f-2 失败 → floor=min(1,2)=1 卡住位点，失败条目塞回队首。
        // PHP 适配（有意偏差）：dispatchRound 对失败重排**同 tick 立即重试**（无 Java 的
        // submitConsumeRequestLater 延迟），所以 floor 中间态在单线程内不可观测，只有终态可断言；
        // "位点不越过失败条目"的真验证靠真机（重试天然跨 tick，见任务#10）。
        $this->checkSame(['f-0', 'f-1', 'f-2', 'f-1', 'f-2'], $GLOBALS['RECV']['FTopic'] ?? [],
            'FTopic 回投失败的 2 条在同 tick 内被塞回队首重放');
        // PTopic：回投 p-1/p-2 成功 → broker 会重投到 %RETRY%，客户端照常推进越过它们
        // （Java removeMessage 对"剩余 msgs"取 max(queueOffset)+1）。
        $this->checkSame(3, ($c->getConsumerStatus('PTopic')[0]['offset'] ?? -1),
            '回投成功后位点照常推进到 3');

        // 第二轮 tick：OTopic OFFSET_ILLEGAL 后重建并按修正位点拉取。
        $c->tick();

        // 位点终值：C=3 / R=3 / P=3 / F=3（重放后追平）/ O=77（OFFSET_ILLEGAL 纠正）。
        $status = $c->getConsumerStatus(null);
        $byTopic = [];
        foreach ($status as $e) {
            $byTopic[$e['mq']->topic] = $e['offset'];
        }
        $this->checkSame(3, $byTopic['CTopic'] ?? -1, 'CTopic 已消费位点=3');
        $this->checkSame(3, $byTopic['PTopic'] ?? -1, 'PTopic 位点=3');
        $this->checkSame(3, $byTopic['RTopic'] ?? -1, 'RTopic 重投后位点=3');
        $this->checkSame(3, $byTopic['FTopic'] ?? -1, 'FTopic 重放后位点=3');
        $this->checkSame(77, $byTopic['OTopic'] ?? -1, 'OTopic 位点被纠正为 77');

        // 线上行为断言。
        $this->loadLog(getenv('RMQ_LOG'));
        $pulls = $this->logWhere(RequestCode::PULL_MESSAGE);
        $this->check($pulls !== [], '有 PULL 请求日志');
        foreach ($pulls as $p) {
            $this->check(((int) $p['sysFlag'] & PullSysFlag::FLAG_SUSPEND) === 0,
                "push 短轮询 suspend 位关闭 (seq={$p['seq']})");
        }
        $firstC = $this->logWhere(RequestCode::PULL_MESSAGE, 'CTopic')[0] ?? null;
        $this->check($firstC !== null && (int) $firstC['queueOffset'] === 0, 'CTopic 首拉从位点 0 开始');
        $oPulls = $this->logWhere(RequestCode::PULL_MESSAGE, 'OTopic');
        $this->check(count($oPulls) >= 2 && (int) $oPulls[1]['queueOffset'] === 77,
            'OFFSET_ILLEGAL 后按修正位点 77 重新拉取');

        // 回投：RTopic 3 条 + PTopic 2 条成功；FTopic 2 条被 broker 拒绝。
        // delayLevel = 3 + reconsumeTimes(0)。
        $backs = $this->logWhere(RequestCode::CONSUMER_SEND_MSG_BACK, null, 'GID-C-CONS');
        $this->checkSame(7, count($backs), 'RECONSUME_LATER×3 + 部分 ack×2 + 回投失败×2 共 7 次');
        foreach ($backs as $b) {
            $this->checkSame(3, (int) $b['delayLevel'], '回投 delayLevel=3+reconsumeTimes');
            $this->checkSame(16, (int) $b['maxReconsumeTimes'], 'maxReconsumeTimes -1 → 16');
        }
        $this->checkSame(3, count(array_filter($backs, static fn($e) => ($e['originTopic'] ?? '') === 'RTopic')),
            'RTopic 回投 3 条');
        $this->checkSame(2, count(array_filter($backs, static fn($e) => ($e['originTopic'] ?? '') === 'PTopic')),
            'PTopic 回投 2 条');
        $this->checkSame(2, count(array_filter($backs, static fn($e) => ($e['originTopic'] ?? '') === 'FTopic')),
            'FTopic 回投 2 条（失败）');

        // 位点提交：persistOffsetsOnce 把已消费位点写给 broker。
        $c->persistOffsetsOnce();
        $this->loadLog(getenv('RMQ_LOG'));
        $updates = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, null, 'GID-C-CONS');
        $commitByTopic = [];
        foreach ($updates as $u) {
            $commitByTopic[$u['topic']] = (int) $u['commitOffset'];
        }
        $this->checkSame(3, $commitByTopic['CTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET CTopic=3');
        $this->checkSame(3, $commitByTopic['PTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET PTopic=3');
        $this->checkSame(77, $commitByTopic['OTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET OTopic=77');
        $this->checkSame(3, $commitByTopic['FTopic'] ?? -1, 'UPDATE_CONSUMER_OFFSET FTopic=3');

        // resetOffset（220 契约）：新位点 42 撤队列重建，之后空拉把位点修正回 3。
        $c->resetOffset('CTopic', [['mq' => new MessageQueue('CTopic', 'b-cons', 0), 'offset' => 42]]);
        $this->loadLog(getenv('RMQ_LOG'));
        $resetUpdates = array_filter(
            $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'CTopic', 'GID-C-CONS'),
            static fn($u) => (int) $u['commitOffset'] === 42,
        );
        $this->check($resetUpdates !== [], 'resetOffset 把新位点 42 提交给 broker');
        $c->tick();
        $status = $c->getConsumerStatus('CTopic');
        $this->checkSame(3, $status[0]['offset'] ?? -1, 'resetOffset 后空拉修正位点回到 3');

        // 实例鸭子类型契约：adjustThreadPool 是 no-op（对齐 Java 5.5.1），corePoolSize 声明值可观测。
        $c->adjustThreadPool();
        $this->checkSame(20, $c->getCorePoolSize(), 'corePoolSize 默认 20');

        $this->cleanup($c);
    }

    // ================================================================ 2b. Push 挂起/恢复（Java suspend/resume/isPause）

    /**
     * 对齐 Java DefaultMQPushConsumer#suspend:890 / #resume:898 / #isPause:902
     *（Impl#suspend:1312-1315、Impl#resume:741-745；读闸门的只有 pullMessage:263-266
     * 与 popMessage:518-521）。
     *
     * 这里锁的是**顺序**而不是"停没停"：闸门必须落在盖章之后。放前面的话挂起超过
     * PULL_MAX_IDLE_TIME（120s）就会让停摆判据把这条循环判死，rebalance 撤队列重建，
     * 运维"暂停两分钟再恢复"实际换来一次整队列重投。PHP 单线程没有"线程死了"分支，
     * 所以只有超时判据，这条竞态更容易被写错。
     */
    private function testPushSuspendResume(ConsFakeBroker $broker): void
    {
        // 常量：Java 的 1000ms 退避与 120s 停摆阈值，二者相差两个数量级才有意义。
        $this->checkSame(1.0, ConsumerDefaults::PULL_TIME_DELAY_WHEN_SUSPEND,
            'PULL_TIME_DELAY_WHEN_SUSPEND = Java 默认 1000ms');
        $this->checkSame(120.0, ConsumerDefaults::PULL_MAX_IDLE_TIME, 'PULL_MAX_IDLE_TIME = 120s');

        // 未 start 也必须安全（Java 不校验服务状态），且翻转幂等。
        $idle = new DefaultMQPushConsumer('GID-SUS-IDLE');
        $idle->setInstanceName('CONS-SUS-IDLE');
        $this->checkSame(false, $idle->isPaused(), '默认不挂起（Java pause = false）');
        $idle->suspend();
        $idle->suspend();
        $this->checkSame(true, $idle->isPaused(), '重复 suspend 保持挂起');
        $idle->resume();
        $idle->resume();
        $this->checkSame(false, $idle->isPaused(), '重复 resume 保持运行');

        $GLOBALS['RECV'] = [];
        $c = new DefaultMQPushConsumer('GID-SUS-CONS');
        $c->setInstanceName('CONS-SUS');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener(new ConsTopicListener([]));
        $c->subscribe('UTopic');
        $c->start();
        $this->checkSame(1, $c->assignedQueueCount(), '挂起用例分到 1 个队列');

        try {
            // 先把拉取跑起来：没有"原本在拉"这个前提，"停了"毫无意义。
            // tick 次数不固定 —— 首轮 tick 可能花在 resolveInitialOffset 上而不是拉取上。
            for ($i = 0; $i < 6; $i++) {
                $c->tick();
                usleep(200_000);
                $this->loadLog(getenv('RMQ_LOG'));
                if (count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS')) >= 2) {
                    break;
                }
            }
            $before = count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS'));
            $this->check($before >= 2, "挂起前已有 PULL 请求（$before 笔）");

            $key = message_queue_key(new MessageQueue('UTopic', 'b-cons', 0));
            $stampBefore = $c->consumerRunningInfo()->mqTable[$key]['lastPullTimestamp'] ?? 0;

            $c->suspend();
            $this->checkSame(true, $c->isPaused(), 'suspend 后 isPaused 为真');

            $baselinePulls = $before;
            // 多个 tick、跨过 Java 的 1s 退避窗口：挂起期间一次网络都不许发。
            for ($i = 0; $i < 4; $i++) {
                $c->tick();
                usleep(300_000);
            }
            $this->loadLog(getenv('RMQ_LOG'));
            $this->checkSame($baselinePulls, count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS')),
                '挂起期间不得再有 PULL_MESSAGE 上线（Java pullMessage:263-266）');

            // 关键不变量：挂起 ≠ 停摆。时刻必须继续推进，否则 120s 判据会把队列撤掉。
            $stampAfter = $c->consumerRunningInfo()->mqTable[$key]['lastPullTimestamp'] ?? 0;
            $this->check($stampAfter > $stampBefore,
                sprintf('挂起期间 lastPullTimestamp 必须继续推进（%d -> %d）：闸门在盖章之后',
                    $stampBefore, $stampAfter));
            $this->checkSame(1, $c->assignedQueueCount(), '挂起不撤队列、不改分配');
            $this->checkSame($stampAfter,
                $c->consumerRunningInfo()->mqTable[$key]['lastPullTimestamp'] ?? 0,
                '挂起期间 307 运行信息读到的仍是推进中的时刻');

            $c->resume();
            $this->checkSame(false, $c->isPaused(), 'resume 后回到运行态');
            $c->tick();
            $this->loadLog(getenv('RMQ_LOG'));
            $after = count($this->logWhere(RequestCode::PULL_MESSAGE, 'UTopic', 'GID-SUS-CONS'));
            $this->check($after > $baselinePulls,
                sprintf('resume 后自己重新发起拉取（%d -> %d）', $baselinePulls, $after));
        } finally {
            $this->cleanup($c);
        }
    }

    // ================================================================ 3. Push 顺序：锁队列 → 挂起重排 → 成功

    private function testPushOrderly(ConsFakeBroker $broker): void
    {
        $GLOBALS['RECV'] = [];
        $listener = new ConsOrderlyListener();
        $c = new DefaultMQPushConsumer('GID-S-CONS');
        $c->setInstanceName('CONS-S');
        $c->setNameServerAddresses([$broker->addr]);
        $c->setMessageListener($listener);
        $c->suspendCurrentQueueTimeMillis = 10; // 挂起重排周期钳到下限，测试不等 1s
        $c->consumeMessageBatchMaxSize = 3;     // 整批投递：一次挂起/成功覆盖全部 3 条
        $c->subscribe('STopic');
        $c->start();
        $this->checkSame(1, $c->assignedQueueCount(), '顺序消费 start 完成 rebalance');

        // 第一轮 tick：LOCK_BATCH_MQ → 拉取 → listener 挂起 → 批次塞回队首。
        $c->tick();
        $this->checkSame(['order-0', 'order-1', 'order-2'], $GLOBALS['RECV']['STopic'] ?? [], '顺序首轮收到 3 条');
        $this->checkSame(1, $listener->calls, '挂起路径 listener 只跑一次');
        $sStatus = $c->getConsumerStatus('STopic');
        $this->check($sStatus === [] || ($sStatus[0]['offset'] ?? -1) <= 0, '挂起期间位点不推进');

        $this->loadLog(getenv('RMQ_LOG'));
        $locks = $this->logWhere(RequestCode::LOCK_BATCH_MQ, null, 'GID-S-CONS');
        $this->check($locks !== [], '顺序消费发起 LOCK_BATCH_MQ');
        $lockMqSet = $locks[0]['mqSet'] ?? [];
        $this->check($lockMqSet !== []
            && ($lockMqSet[0]['topic'] ?? '') === 'STopic'
            && ($lockMqSet[0]['brokerName'] ?? '') === 'b-cons',
            '锁队列请求带 mqSet(topic/brokerName/queueId)');
        $this->checkSame(MixAll::clientIdFor('CONS-S', null, false), $locks[0]['clientId'] ?? '', '锁队列带 clientId');
        // 顺序消费不发生 CONSUMER_SEND_MSG_BACK（重试在本地原地重排）。
        $this->checkSame([], $this->logWhere(RequestCode::CONSUMER_SEND_MSG_BACK, null, 'GID-S-CONS'),
            '顺序挂起不回投');

        // 挂起到点：重排批次 → SUCCESS → 位点推进到 3。
        usleep(60000);
        $c->tick();
        $this->checkSame(2, $listener->calls, '重排后 listener 第二次执行');
        $this->checkSame(array_merge(['order-0', 'order-1', 'order-2'], ['order-0', 'order-1', 'order-2']),
            $GLOBALS['RECV']['STopic'] ?? [], '同一批消息被原地重排重放');
        $this->checkSame(3, ($c->getConsumerStatus('STopic')[0]['offset'] ?? -1), 'SUCCESS 后位点=3');

        // shutdown 顺序消费要解锁（UNLOCK_BATCH_MQ）。
        $this->cleanup($c);
        $this->loadLog(getenv('RMQ_LOG'));
        $this->check($this->logWhere(RequestCode::UNLOCK_BATCH_MQ, null, 'GID-S-CONS') !== [],
            'shutdown 发起 UNLOCK_BATCH_MQ');
    }

    // ================================================================ 4. PullConsumer：手拉手管位点

    private function testPullConsumer(ConsFakeBroker $broker): void
    {
        $c = new DefaultMQPullConsumer('GID-P-CONS');
        $c->setInstanceName('CONS-P');
        $c->setNameServerAddresses([$broker->addr]);
        $c->registerTopic('DTopic');
        $c->start();
        $this->check($c->mqClient !== null, 'pull start 后实例就绪');

        $queues = $c->fetchSubscribeMessageQueues('DTopic');
        $this->checkSame(1, count($queues), 'fetchSubscribeMessageQueues 1 条队列');
        $mq = $queues[0];
        $this->checkSame('DTopic', $mq->topic, '队列 topic');

        // 已提交位点（QUERY_CONSUMER_OFFSET → DTopic=7）。
        $this->checkSame(7, $c->fetchConsumeOffset($mq), 'fetchConsumeOffset=7');

        // 短轮询：suspend 位必须关（INVARIANTS B3：写 true 必现超时）。
        $r = $c->pull($mq, '*', 0);
        $this->checkSame(PullStatus::FOUND, $r->status, 'pull FOUND');
        $this->checkSame(5, $r->nextBeginOffset, 'pull nextBeginOffset=5');
        $this->checkSame(1, count($r->msgFoundList), 'pull 拉回 1 条');
        $this->checkSame('direct-4', $r->msgFoundList[0]->getBody(), 'pull 消息 body');
        $this->checkSame(4, $r->msgFoundList[0]->queueOffset, 'pull 消息 queueOffset');

        $this->loadLog(getenv('RMQ_LOG'));
        $dpull = $this->logWhere(RequestCode::PULL_MESSAGE, 'DTopic', 'GID-P-CONS');
        $this->check($dpull !== [] && (int) $dpull[0]['sysFlag'] === PullSysFlag::FLAG_SUBSCRIPTION,
            'pull sysFlag=4（短轮询 + 订阅位）');

        // 长轮询：suspend 位必须开。
        $rb = $c->pullBlockIfNotFound($mq, '*', 0, 32);
        $this->checkSame(PullStatus::FOUND, $rb->status, 'pullBlockIfNotFound 也能解析');
        $this->loadLog(getenv('RMQ_LOG'));
        $dpull = $this->logWhere(RequestCode::PULL_MESSAGE, 'DTopic', 'GID-P-CONS');
        $last = $dpull[count($dpull) - 1] ?? null;
        $this->check($last !== null && (int) $last['sysFlag'] === (PullSysFlag::FLAG_SUBSCRIPTION | PullSysFlag::FLAG_SUSPEND),
            'pullBlockIfNotFound sysFlag=6（挂起位开）');

        // 位点管理与运维查询。
        $c->updateConsumeOffset($mq, 9);
        $this->loadLog(getenv('RMQ_LOG'));
        $upd = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'DTopic', 'GID-P-CONS');
        $this->check($upd !== [] && (int) $upd[0]['commitOffset'] === 9, 'updateConsumeOffset 提交 9');
        $this->checkSame(55, $c->searchOffset($mq, 1_700_000_000_000), 'searchOffset=55');
        $this->checkSame(100, $c->maxOffset($mq), 'maxOffset=100');
        $this->checkSame(0, $c->minOffset($mq), 'minOffset=0');
        $this->checkSame(1234567890000, $c->earliestMsgStoreTime($mq), 'earliestMsgStoreTime');

        // 回投：delayLevel 调用方指定；maxReconsumeTimes 留 null 交给订阅组（有意差异）。
        $c->sendMessageBack($r->msgFoundList[0], 2);
        $this->loadLog(getenv('RMQ_LOG'));
        $backs = $this->logWhere(RequestCode::CONSUMER_SEND_MSG_BACK, null, 'GID-P-CONS');
        $this->checkSame(1, count($backs), 'pull sendMessageBack 1 次');
        $this->checkSame(2, (int) ($backs[0]['delayLevel'] ?? -1), '回投 delayLevel=2');
        $this->checkSame(null, $backs[0]['maxReconsumeTimes'] ?? null, 'pull 回投 maxReconsumeTimes 不下发');

        // fetchMessageQueuesInBalance（Java MQPullConsumer:187 / Impl:120-135）：
        // 本端口拉模式没有后台 rebalance，就按 rebalanceByTopic 同一条公式当场算。
        // 组里只有本实例（RMQ_CONSUMER_MAP 只登记了 CONS-P 一个 clientId）→ 全份都是我的。
        $balanced = $c->fetchMessageQueuesInBalance('DTopic');
        $this->checkSame(1, count($balanced), '平衡视图给出本实例负责的 1 条队列');
        $this->checkSame('DTopic', $balanced[0]->topic ?? '', '平衡视图队列 topic');
        $this->checkSame('b-cons', $balanced[0]->brokerName ?? '', '平衡视图队列 brokerName');
        $this->checkSame(0, $balanced[0]->queueId ?? -1, '平衡视图队列 queueId');
        // 查不到消费者列表时**保留现有分配**：换成没登记的组，
        // 实际拉过的 DTopic 还在（本地记账），没拉过的 ETopic 必须是空 ——
        // 绝不能回退成"独占全部队列"，否则同组多实例互相重复消费。
        $group = $c->consumerGroup;
        $c->consumerGroup = 'GID-NO-MAP';
        $this->checkSame(['DTopic@b-cons@0'],
            array_map(static fn($mq) => "{$mq->topic}@{$mq->brokerName}@{$mq->queueId}",
                $c->fetchMessageQueuesInBalance('DTopic')),
            '列表查不到时退回本地 pullFromWhichNodeTable 键集');
        $this->checkSame([], $c->fetchMessageQueuesInBalance('ETopic'),
            '没拉过又查不到列表 → 空集（Java rebalance 前的空表同语义）');
        $c->consumerGroup = $group;
        // 未启动的消费者一律拒绝（Java isRunning()）：新实例直接调用要抛。
        $idle = new DefaultMQPullConsumer('GID-P-CONS');
        $idle->setNameServerAddresses([$broker->addr]);
        $threw = false;
        try {
            $idle->fetchMessageQueuesInBalance('DTopic');
        } catch (MQClientException) {
            $threw = true;
        }
        $this->check($threw, '未启动就取平衡视图 → MQClientException');

        $this->cleanup($c);
    }

    // ================================================================ 5. LitePull：assign 模式双游标 + commit + seek

    private function testLitePull(ConsFakeBroker $broker): void
    {
        $c = new DefaultLitePullConsumer('GID-L-CONS');
        $c->setInstanceName('CONS-L');
        $c->setNamesrvAddr($broker->addr);
        $mq = new MessageQueue('LTopic', 'b-cons', 0);
        $c->assign([$mq]); // start 前指派：初始位点解析失败被吞，tick 时再解析
        $c->start();
        $this->check($c->isRunning(), 'lite start 后 running');
        $this->checkSame([$mq], $c->assignment(), 'assignment 返回指派队列');

        // tick：LITE_PULL_MESSAGE 短轮询 → FOUND 2 条进缓冲。
        $c->tick();
        $this->checkSame(2, $c->pullCursorOf($mq), '拉取游标推进到 2');
        $this->checkSame(-1, $c->consumeCursorOf($mq), 'poll 前已消费游标未动');

        // poll：交出缓冲并推进"已消费游标"（Java poll 语义）。
        $msgs = $c->poll(500);
        $this->checkSame(['lite-0', 'lite-1'], array_map(static fn($m) => $m->getBody(), $msgs), 'poll 交出 2 条');
        $this->checkSame(2, $c->consumeCursorOf($mq), 'poll 后已消费游标=2');
        $this->checkSame(2, $c->pullCursorOf($mq), 'poll 不动拉取游标');

        // commit：把"已消费游标"提交给 broker。
        $c->commit();
        $this->loadLog(getenv('RMQ_LOG'));
        $upd = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'LTopic', 'GID-L-CONS');
        $this->check($upd !== [] && (int) $upd[0]['commitOffset'] === 2, 'commit 提交位点 2');
        $this->checkSame(2, $c->committed($mq), 'committed 读内存位点表');
        $this->checkSame(2, $c->pendingCommitOf($mq), 'pendingCommit 暴露内存提交位');

        // 指定位点 commit（Map 重载）：只改提交位置，不动游标。
        $c->commit([$mq->topic . $mq->brokerName . '0' => 1], true);
        $this->loadLog(getenv('RMQ_LOG'));
        $upd = $this->logWhere(RequestCode::UPDATE_CONSUMER_OFFSET, 'LTopic', 'GID-L-CONS');
        $lastCommit = (int) ($upd[count($upd) - 1]['commitOffset'] ?? -1);
        $this->checkSame(1, $lastCommit, 'commit(Map) 提交指定位点 1');
        $this->checkSame(2, $c->pullCursorOf($mq), 'commit(Map) 不动拉取游标');
        $this->checkSame(2, $c->consumeCursorOf($mq), 'commit(Map) 不动已消费游标');

        // seek：三张游标一起拨回去，下一轮拉取从新位点出发。
        $c->seek($mq, 1);
        $this->checkSame(1, $c->pullCursorOf($mq), 'seek 拨回拉取游标');
        $this->checkSame(1, $c->consumeCursorOf($mq), 'seek 拨回已消费游标');
        $c->tick();
        $this->checkSame(2, $c->pullCursorOf($mq), 'seek 后空拉把拉取游标推回 next=2');

        // pause/resume：暂停期间不发起拉取。
        $this->loadLog(getenv('RMQ_LOG'));
        $litePullsBefore = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS'));
        $c->pause([$mq]);
        $c->tick();
        $this->loadLog(getenv('RMQ_LOG'));
        $litePullsPaused = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS'));
        $this->checkSame($litePullsBefore, $litePullsPaused, 'pause 期间不拉取');
        $c->resume([$mq]);
        $c->tick();
        $this->loadLog(getenv('RMQ_LOG'));
        $litePullsResumed = count($this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS'));
        $this->checkSame($litePullsBefore + 1, $litePullsResumed, 'resume 后恢复拉取');

        // LITE_PULL 线上行为：lite 位必须置上、suspend 位必须关。
        $liteReq = $this->logWhere(RequestCode::LITE_PULL_MESSAGE, 'LTopic', 'GID-L-CONS');
        $this->check($liteReq !== [], 'lite 走 LITE_PULL_MESSAGE 请求码');
        foreach ($liteReq as $e) {
            $this->check(((int) $e['sysFlag'] & PullSysFlag::FLAG_LITE_PULL_MESSAGE) !== 0, 'lite 位置上');
            $this->check(((int) $e['sysFlag'] & PullSysFlag::FLAG_SUSPEND) === 0, 'lite 短轮询 suspend 位关');
        }

        // SimpleMessageListener 包装 callable。
        $l = new SimpleMessageListener(static fn(array $ms) => ConsumeConcurrentlyStatus::CONSUME_SUCCESS);
        $ctx = new ConsumeConcurrentlyContext($mq);
        $this->checkSame(ConsumeConcurrentlyStatus::CONSUME_SUCCESS,
            $l->consumeMessage([], $ctx), 'SimpleMessageListener 透传 callable');

        $this->cleanup($c);
    }

    // ================================================================ run

    public function run(): int
    {
        $this->testConsumerSupportUnit();

        // ---- 网络用例：起假 broker + 日志/消费者列表文件 ----
        $tmpDir = sys_get_temp_dir() . '/rmq-cons-test-' . getmypid();
        if (!is_dir($tmpDir)) {
            @mkdir($tmpDir, 0777, true);
        }
        $logPath = $tmpDir . '/broker.jsonl';
        $groupMapPath = $tmpDir . '/groups.json';
        @unlink($logPath);
        putenv("RMQ_LOG={$logPath}");
        // 消费者列表按组隔离：每个组只含自己的 clientId（顺序必须与客户端一致）。
        $groupMap = [
            'GID-C-CONS' => [MixAll::clientIdFor('CONS-C', null, false)],
            'GID-S-CONS' => [MixAll::clientIdFor('CONS-S', null, false)],
            'GID-P-CONS' => [MixAll::clientIdFor('CONS-P', null, true)],
            'GID-L-CONS' => [MixAll::clientIdFor('CONS-L', null, true)],
            'GID-SUS-CONS' => [MixAll::clientIdFor('CONS-SUS', null, false)],
        ];
        file_put_contents($groupMapPath, json_encode($groupMap));
        putenv("RMQ_CONSUMER_MAP={$groupMapPath}");

        $broker = ConsFakeBroker::start();
        try {
            $this->testPushConcurrent($broker);
            $this->testPushSuspendResume($broker);
            $this->testPushOrderly($broker);
            $this->testPullConsumer($broker);
            $this->testLitePull($broker);
        } finally {
            $broker->stop();
            putenv('RMQ_LOG');
            putenv('RMQ_CONSUMER_MAP');
            @unlink($logPath);
            @unlink($groupMapPath);
            @rmdir($tmpDir);
        }
        return $this->summary();
    }
}

exit((new RunClientConsumer())->run());

<?php

declare(strict_types=1);

// PHP 端压缩矩阵腿：对标 go/examples/live_compression_matrix（参数与退出码一致）。
//
//   php examples/live_compression.php send <topic> <group> <size> <namesrv> [codec]
//   php examples/live_compression.php recv <topic> <group> <size> <namesrv>
//
// 载荷由每端按**同一配方本地重建**（同一行文本重复后截断）——必须与 Java
// CompressProbe.buildPayload 逐字节一致，否则两边算的是"同一个载荷"的不同 CRC，
// 所有腿都无理由失败。判定只看接收端打印的 `match=1`（标准 CRC-32 IEEE，
// PHP 的 crc32() 可能返回负数，用 sprintf('%u') 转无符号；**不是** Java
// UtilAll.crc32 的 &0x7FFFFFFF 口径，两边数字本来就差 2^31，不比数字）。
//
// codec 面覆盖 zlib（gzcompress）+ lz4（CompressionCodec 的纯 PHP **LZ4 Frame**
// 实现，frame 内部块就是 block-format）+ zstd（优先 `zstd` CLI，退回纯 PHP
// Raw/RLE 帧）——见 2026-10-08。接收端按 sysFlag 类型位
// 自动解压，所以「B 能解 A 压的」正是矩阵要证明的部分。
//
// 退出码（对齐 Python/Go 腿，脚本据此区分）：0 ok / 1 普通失败（含发送失败，
// 不能占用 2）/ 2 坏 codec 或坏用法 / 3 recv 超时。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageSysFlag;

// payloadLine 是矩阵的种子，任何端口都逐字节一致。
const PAYLOAD_LINE = "rocketmq-compress-interop-payload-line-0123456789\n";

function build_payload(int $size): string
{
    $out = '';
    while (strlen($out) < $size) {
        $out .= PAYLOAD_LINE;
    }
    return substr($out, 0, $size);
}

/** 标准 CRC-32（无符号十进制字符串），与其他四端口同一口径。 */
function crc_u32(string $data): string
{
    return sprintf('%u', crc32($data));
}

const USAGE = <<<TXT
usage:
  php live_compression.php send <topic> <group> <size> <namesrv> [codec]
  php live_compression.php recv <topic> <group> <size> <namesrv>
TXT;

$argv0 = $argv[1] ?? '';
if ($argv0 === 'send') {
    // send <topic> <group> <size> <namesrv> [codec]
    if (count($argv) < 5) {
        echo USAGE . PHP_EOL;
        exit(2);
    }
    $topic = $argv[2];
    $group = $argv[3];
    $size = (int) $argv[4];
    $namesrv = $argv[5] ?? '127.0.0.1:9876';
    $codec = $argv[6] ?? 'zlib';

    $codecTypes = ['zlib' => MessageSysFlag::ZLIB_TYPE, 'lz4' => MessageSysFlag::LZ4_TYPE, 'zstd' => MessageSysFlag::ZSTD_TYPE];
    if (!isset($codecTypes[$codec])) {
        // 2 严格留给"坏 codec / 坏用法"：普通发送失败绝不能报 2。
        echo "SEND_UNSUPPORTED codec=$codec (php port supports zlib|lz4|zstd)" . PHP_EOL;
        exit(2);
    }

    $payload = build_payload($size);
    try {
        $p = new DefaultMQProducer($group);
        $p->setNamesrvAddr($namesrv);
        $p->sendMsgTimeout = 5000;
        $p->setCompressType($codecTypes[$codec]);
        $p->start();
        // body >= compressMsgBodyOverHowmuch(4096) ⇒ 生产端自动压缩
        $r = $p->send(new Message($topic, $payload));
        $p->shutdown();
        if ($r->sendStatus !== SendStatus::SEND_OK) {
            echo 'SEND_FAILED status=' . $r->sendStatus->name . PHP_EOL;
            exit(1);
        }
        echo 'SEND_OK crc=' . crc_u32($payload) . ' size=' . strlen($payload)
            . ' msgId=' . ($r->msgId ?? '-') . PHP_EOL;
        exit(0);
    } catch (Throwable $e) {
        echo 'SEND_FAILED ' . get_class($e) . ': ' . $e->getMessage() . PHP_EOL;
        exit(1);
    }
}

if ($argv0 === 'recv') {
    // recv <topic> <group> <size> <namesrv>
    if (count($argv) < 5) {
        echo USAGE . PHP_EOL;
        exit(2);
    }
    $topic = $argv[2];
    $group = $argv[3];
    $size = (int) $argv[4];
    $namesrv = $argv[5] ?? '127.0.0.1:9876';

    $expect = crc_u32(build_payload($size));
    try {
        $c = new DefaultMQPullConsumer($group);
        $c->setNamesrvAddr($namesrv);
        $c->registerTopic($topic);
        $c->start();

        $deadline = microtime(true) + 120.0;
        while (microtime(true) < $deadline) {
            $c->tick();
            try {
                $queues = $c->fetchSubscribeMessageQueues($topic);
            } catch (Throwable) {
                usleep(500_000);
                continue; // 路由还没出来
            }
            foreach ($queues as $mq) {
                $offset = 0;
                for ($round = 0; $round < 40; $round++) {
                    $r = $c->pull($mq, '*', $offset, 32);
                    if ($r->status !== PullStatus::FOUND) {
                        break;
                    }
                    foreach ($r->msgFoundList as $m) {
                        $body = (string) $m->getBody();
                        // 按 size 找目标载荷（同 topic 上可能混有其他腿的消息）
                        if (strlen($body) !== $size) {
                            continue;
                        }
                        $got = crc_u32($body);
                        if ($got === $expect) {
                            echo "RECV_OK match=1 crc=$got size=$size msgId=" . ($m->msgId ?? '-') . PHP_EOL;
                            $c->shutdown();
                            exit(0);
                        }
                        // 解出了消息但 CRC 不一致 = 静默数据损坏本体
                        echo "RECV_BAD match=0 crc=$got expect=$expect size=$size" . PHP_EOL;
                        $c->shutdown();
                        exit(1);
                    }
                    $offset = $r->nextBeginOffset;
                }
            }
            usleep(500_000);
        }
        echo 'RECV_TIMEOUT expect=' . $expect . ' size=' . $size . PHP_EOL;
        $c->shutdown();
        exit(3);
    } catch (Throwable $e) {
        echo 'RECV_FAILED ' . get_class($e) . ': ' . $e->getMessage() . PHP_EOL;
        exit(1);
    }
}

echo USAGE . PHP_EOL;
exit(2);

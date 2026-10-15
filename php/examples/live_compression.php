<?php

declare(strict_types=1);

// Compression matrix leg: fixed parameters and exit codes.
//
//   php examples/live_compression.php send <topic> <group> <size> <namesrv> [codec]
//   php examples/live_compression.php recv <topic> <group> <size> <namesrv>
//
//
// The codec surface covers zlib (gzcompress) + lz4 (a pure-PHP **LZ4 Frame** implementation in
// CompressionCodec; the frame's inner chunks are the block format) + zstd (prefers the `zstd`
// CLI, falls back to a pure-PHP Raw/RLE frame) — see 2026-10-08. The receiver auto-decompresses
// by the sysFlag type bits, so "B can decompress what A compressed" is exactly what the matrix
// is meant to prove.
//
// Exit codes (the matrix script keys off them): 0 ok / 1 ordinary failure (including send
// failure; must not take 2) / 2 bad codec or bad usage / 3 recv timeout.
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultMQPullConsumer;
use RocketMQ\Client\PullStatus;
use RocketMQ\Client\SendStatus;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageSysFlag;

// payloadLine is the matrix's seed; every port must be byte-for-byte identical.
const PAYLOAD_LINE = "rocketmq-compress-interop-payload-line-0123456789\n";

function build_payload(int $size): string
{
    $out = '';
    while (strlen($out) < $size) {
        $out .= PAYLOAD_LINE;
    }
    return substr($out, 0, $size);
}

/** Standard CRC-32 (unsigned decimal string); the send and recv legs use it to reconcile the body. */
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
        // 2 is strictly reserved for "bad codec / bad usage": an ordinary send failure must never report 2.
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
        // body >= compressMsgBodyOverHowmuch(4096) => the producer compresses automatically
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
                continue; // route not ready yet
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
                        // Locate the target payload by size (other legs may share this topic)
                        if (strlen($body) !== $size) {
                            continue;
                        }
                        $got = crc_u32($body);
                        if ($got === $expect) {
                            echo "RECV_OK match=1 crc=$got size=$size msgId=" . ($m->msgId ?? '-') . PHP_EOL;
                            $c->shutdown();
                            exit(0);
                        }
                        // Message decoded but CRC mismatch = the silent data corruption itself
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

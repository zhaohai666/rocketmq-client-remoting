<?php

declare(strict_types=1);

namespace RocketMQ\Common;

use Throwable;

/**
 * 消息二进制编解码。
 *
 * broker 侧定义了两条编码路径，本类两条都实现，切勿混用：
 *
 * 1) 17 段存储格式 ``MessageDecoder::encodeMessageExt($msgExt, $needCompress)`` /
 *    ``decode($raw)``
 *    用于 broker 写入与 pull/get 返回的消息体：
 *
 *    序号  字段                             编码
 *    1     TOTALSIZE                        int(4)
 *    2     MAGICCODE                        int(4)  -626843481 (v1) / -626843477 (v2)
 *    3     BODYCRC                          int(4)
 *    4     QUEUEID                          int(4)
 *    5     FLAG                             int(4)
 *    6     QUEUEOFFSET                      long(8)
 *    7     PHYSICALOFFSET                   long(8)
 *    8     SYSFLAG                          int(4)
 *    9     BORNTIMESTAMP                    long(8)
 *    10    BORNHOST                         4|16B ip + 4B port
 *    11    STORETIMESTAMP                   long(8)
 *    12    STOREHOST                        4|16B ip + 4B port
 *    13    RECONSUMETIMES                   int(4)
 *    14    PREPAREDTRANSACTIONOFFSET        long(8)
 *    15    BODY                             int(4) len + body
 *    16    TOPIC                            1B(v1)|2B(v2) len + topic
 *    17    PROPERTIES                       short(2) len + k\x01v\x02 串
 *
 * 2) 6 段轻量格式 ``encodeMessage(Message)`` / ``decodeBatchMessage($raw)``
 *    仅用于**批量消息**（MessageBatch 的 body），不含 topic / crc：
 *
 *    TOTALSIZE(4) | MAGICCODE(4, 固定 0) | BODYCRC(4, 固定 0) | FLAG(4)
 *    | BODY(4+len) | PROPERTIES(2+len)
 */
final class MessageDecoder
{
    public const CHARSET_UTF8 = 'UTF-8';
    public const NAME_VALUE_SEPARATOR = "\x01";
    public const PROPERTY_SEPARATOR = "\x02";

    public const MESSAGE_MAGIC_CODE = -626843481;
    public const MESSAGE_MAGIC_CODE_V2 = -626843477;
    public const BLANK_MAGIC_CODE = -875286124;

    // 字段固定偏移（17 段存储格式里的字节位置）
    public const MESSAGE_MAGIC_CODE_POSITION = 4;
    public const MESSAGE_FLAG_POSITION = 16;
    public const MESSAGE_PHYSIC_OFFSET_POSITION = 28;
    public const QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4;
    public const PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
    public const SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
    public const MESSAGE_STORE_TIMESTAMP_POSITION = 56;

    private const HEX_TABLE = '0123456789ABCDEF';

    // ---------------------------------------------------------------- 基础工具

    /**
     * 协议里的文本字段一律 UTF-8。
     * PHP 字符串本就是字节串，此处仅作语义标注（入参须为 UTF-8 文本）。
     */
    public static function string2bytes(string $s): string
    {
        return $s;
    }

    /**
     * 逐字节转**大写**十六进制：msgId 的大小写是硬要求，小写串解不出 host/port。
     */
    public static function bytes2string(string $bs): string
    {
        return strtoupper(bin2hex($bs));
    }

    /**
     * 十六进制字符串 -> 字节（bytes2string 的逆运算）。
     */
    public static function string2bytesHex(?string $hexString): ?string
    {
        if ($hexString === null || $hexString === '') {
            return null;
        }
        $raw = @hex2bin($hexString);
        return $raw === false ? null : $raw;
    }

    public static function ipAndPortToBytes(string $ip, int $port, bool $v6 = false): string
    {
        return self::ipToBytes($ip, $v6) . pack('N', $port);
    }

    private static function ipToBytes(string $ip, bool $v6 = false): string
    {
        $bin = @inet_pton($ip);
        if ($bin === false) {
            throw new \RuntimeException(sprintf('invalid ip address: %s', $ip));
        }
        return $bin;
    }

    /**
     * @return array{0: string, 1: int} [ip, port]
     */
    public static function bytesToIpAndPort(string $raw): array
    {
        if (strlen($raw) === 8) {
            $ip = long2ip((int) unpack('N', substr($raw, 0, 4))[1]);
            $port = (int) unpack('N', substr($raw, 4, 4))[1];
        } else {
            $ip = @inet_ntop(substr($raw, 0, 16));
            if ($ip === false) {
                throw new \RuntimeException('invalid ipv6 bytes');
            }
            $port = (int) unpack('N', substr($raw, 16, 4))[1];
        }
        return [$ip, $port];
    }

    /**
     * BODYCRC 用标准 CRC32（poly 0xEDB88320），PHP 内置 crc32() 同参数，
     * 可直接和 broker 写入的值比对。
     */
    public static function crc32(string $data): int
    {
        return crc32($data) & 0xFFFFFFFF;
    }

    // ------------------------------------------------------- 属性串 <-> Map

    /**
     * 属性 map -> 协议串：逐项 `name\x01value\x02` 拼接。
     *
     * @param array<string, string|null>|null $properties
     */
    public static function messageProperties2String(?array $properties): string
    {
        if ($properties === null) {
            return '';
        }
        $parts = '';
        foreach ($properties as $name => $value) {
            if ($value === null) {
                continue;
            }
            $parts .= $name . self::NAME_VALUE_SEPARATOR . $value . self::PROPERTY_SEPARATOR;
        }
        return $parts;
    }

    /**
     * 协议串 -> 属性 map（messageProperties2String 的逆运算）。
     *
     * @return array<string, string>
     */
    public static function string2MessageProperties(?string $propertiesStr): array
    {
        $result = [];
        if ($propertiesStr === null || $propertiesStr === '') {
            return $result;
        }
        $length = strlen($propertiesStr);
        $index = 0;
        while ($index < $length) {
            $newIndex = strpos($propertiesStr, self::PROPERTY_SEPARATOR, $index);
            if ($newIndex === false) {
                $newIndex = $length;
            }
            if ($newIndex - $index >= 3) {
                $kvSep = strpos($propertiesStr, self::NAME_VALUE_SEPARATOR, $index);
                if ($kvSep !== false && $kvSep > $index && $kvSep < $newIndex - 1) {
                    $result[substr($propertiesStr, $index, $kvSep - $index)]
                        = substr($propertiesStr, $kvSep + 1, $newIndex - $kvSep - 1);
                }
            }
            $index = $newIndex + 1;
        }
        return $result;
    }

    // ---------------------------------------------------------------- msgId

    /**
     * ip+port(8 或 20B) + 8B commitLogOffset -> 十六进制 msgId。
     */
    public static function createMessageId(string $addrBytes, int $offset): string
    {
        return self::bytes2string($addrBytes . pack('J', $offset));
    }

    /**
     * 十六进制 msgId -> [ip, port, offset]（createMessageId 的逆运算）。
     *
     * @return array{0: string, 1: int, 2: int}
     */
    public static function decodeMessageId(string $msgId): array
    {
        $raw = @hex2bin($msgId);
        if ($raw === false) {
            throw new \ValueError(sprintf('invalid msgId hex: %s', $msgId));
        }
        $ipLen = strlen($raw) === 16 ? 4 : 16;
        if ($ipLen === 4) {
            $ip = long2ip((int) unpack('N', substr($raw, 0, 4))[1]);
        } else {
            $ip = @inet_ntop(substr($raw, 0, 16));
            if ($ip === false) {
                throw new \ValueError('invalid ipv6 bytes in msgId');
            }
        }
        $port = (int) unpack('N', substr($raw, $ipLen, 4))[1];
        $offset = (int) unpack('J', substr($raw, $ipLen + 4, 8))[1];
        return [$ip, $port, $offset];
    }

    // ------------------------------------------------- 压缩 / 解压（可选依赖）

    /**
     * 把 sysFlag 里解出的压缩类型归一化到"真实算法"。
     *
     * 压缩类型编号的向后兼容映射（协议里只有这几个取值）::
     *
     *     1 -> LZ4
     *     2 -> ZSTD
     *     0 -> ZLIB   （没有类型位能力的老格式消息，类型位就是 0）
     *     3 -> ZLIB
     *
     * 即**类型位为 0 的老版本压缩消息按 ZLIB 处理**。这是必需的：老版本客户端
     * （无类型位能力）产出的压缩消息类型位就是 0，若不映射到 ZLIB，解压会失败，
     * 而外层又会照样清掉 COMPRESSED_FLAG，结果是**静默返回压缩字节流**——数据损坏
     * 且事后无法识别。
     */
    public static function normalizeCompressionType(int $compressionType): int
    {
        if ($compressionType === 0) {
            return MessageSysFlag::ZLIB_TYPE;
        }
        return $compressionType;
    }

    private static function compress(string $data, int $compressionType, int $level = 5): string
    {
        $ctype = self::normalizeCompressionType($compressionType);
        if ($ctype === MessageSysFlag::ZLIB_TYPE) {
            $out = @gzcompress($data, $level);
            if ($out === false) {
                throw new \RuntimeException('zlib compress failed');
            }
            return $out;
        }
        if ($ctype === MessageSysFlag::LZ4_TYPE) {
            // LZ4 Frame 格式（魔数 0x184D2204 的自描述帧），纯 PHP 实现
            return CompressionCodec::lz4CompressFrame($data);
        }
        if ($ctype === MessageSysFlag::ZSTD_TYPE) {
            // 合法 ZSTD 帧（CLI 优先真压缩；无 CLI 时 Raw/RLE 块），
            // broker 按标准 ZSTD 帧解析，两种都能解。
            return CompressionCodec::zstdCompress($data);
        }
        throw self::unsupported($compressionType);
    }

    private static function decompressInternal(string $data, int $compressionType): string
    {
        $ctype = self::normalizeCompressionType($compressionType);
        if ($ctype === MessageSysFlag::ZLIB_TYPE) {
            // gzcompress 产出 zlib 格式（RFC1950），还原只能用 gzuncompress；
            // gzinflate 只吃裸 deflate，gzdecode 只吃 gzip 格式（RFC1952），都不能用
            $out = @gzuncompress($data);
            if ($out === false) {
                throw new \RuntimeException('zlib decompress failed');
            }
            return $out;
        }
        if ($ctype === MessageSysFlag::LZ4_TYPE) {
            return CompressionCodec::lz4DecompressFrame($data);
        }
        if ($ctype === MessageSysFlag::ZSTD_TYPE) {
            return CompressionCodec::zstdDecompress($data);
        }
        throw self::unsupported($compressionType);
    }

    /**
     * 未知压缩类型必须抛错。
     *
     * 类型位只有 0/3(ZLIB)、1(LZ4)、2(ZSTD) 四个合法取值，其余都是脏数据。
     * **绝不能原样透传**：调用方（``decodeMessage``）在解压后会清掉
     * ``COMPRESSED_FLAG``，透传等于把压缩字节流当正文交出去且事后无法识别，
     * 属于静默数据损坏。
     */
    private static function unsupported(int $compressionType): \RuntimeException
    {
        return new \RuntimeException(sprintf('unsupported compression type: %d', $compressionType));
    }

    /**
     * 按压缩类型解压（``decompressInternal`` 的公开入口）。
     *
     * 除 ``decodeMessage`` 之外还有第二个调用方：Request-Reply 的应答是从
     * ``PUSH_REPLY_MESSAGE_TO_CLIENT(326)`` 直接推过来的裸包，不走消息解码路径，
     * 需要自己按 ``sysFlag`` 判断并在这里解压。
     */
    public static function decompressBody(string $data, int $compressionType): string
    {
        return self::decompressInternal($data, $compressionType);
    }

    // ------------------------------------------- 1) 17 段存储格式：MessageExt

    private static function topicLengthSize(int $magicCode): int
    {
        return $magicCode === self::MESSAGE_MAGIC_CODE_V2 ? 2 : 1;
    }

    public static function encodeMessageExt(MessageExt $messageExt, bool $needCompress = false): string
    {
        /**
         * 17 段存储格式编码：needCompress 为真且 sysFlag 标了 COMPRESSED 时先压 body。
         *
         * 注意：topic 长度固定写 1 字节、魔数固定写 v1（-626843481），
         * storeSize > 0 时直接按其分配缓冲（尾部不足会按需补齐）。
         */
        $body = $messageExt->getBody() ?? '';
        if ($needCompress && ($messageExt->getSysFlag() & MessageSysFlag::COMPRESSED_FLAG) !== 0) {
            $compressionType = MessageSysFlag::getCompressionType($messageExt->getSysFlag());
            $body = self::compress($body, $compressionType);
        }
        $bodyLength = strlen($body);

        $topicBytes = self::string2bytes($messageExt->getTopic());
        $topicLen = strlen($topicBytes);
        $propertiesBytes = self::string2bytes(self::messageProperties2String($messageExt->getProperties()));
        $propertiesLength = strlen($propertiesBytes);

        $sysFlag = $messageExt->getSysFlag();
        $bornhostLength = ($sysFlag & MessageSysFlag::BORNHOST_V6_FLAG) !== 0 ? 20 : 8;
        $storehostLength = ($sysFlag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) !== 0 ? 20 : 8;

        $computedSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8
            + $bornhostLength + $storehostLength + 4 + 8
            + 4 + $bodyLength
            + 1 + $topicLen
            + 2 + $propertiesLength;
        $storeSize = $messageExt->getStoreSize() > 0 ? $messageExt->getStoreSize() : $computedSize;
        $storeSize = max($storeSize, $computedSize);

        $bornHost = $messageExt->getBornHost() ?? '127.0.0.1';
        $bornPort = $messageExt->bornHostPort;
        $storeHost = $messageExt->getStoreHost() ?? '127.0.0.1';
        $storePort = $messageExt->storeHostPort;

        $buf = '';
        $buf .= pack('N', $storeSize & 0xFFFFFFFF);                       // 1 TOTALSIZE
        $buf .= pack('N', self::MESSAGE_MAGIC_CODE & 0xFFFFFFFF);         // 2 MAGICCODE
        $buf .= pack('N', $messageExt->getBodyCrc() & 0xFFFFFFFF);        // 3 BODYCRC
        $buf .= pack('N', $messageExt->getQueueId() & 0xFFFFFFFF);        // 4 QUEUEID
        $buf .= pack('N', $messageExt->getFlag() & 0xFFFFFFFF);           // 5 FLAG
        $buf .= pack('J', $messageExt->getQueueOffset());                 // 6 QUEUEOFFSET
        $buf .= pack('J', $messageExt->getCommitLogOffset());             // 7 PHYSICALOFFSET
        $buf .= pack('N', $sysFlag & 0xFFFFFFFF);                         // 8 SYSFLAG
        $buf .= pack('J', $messageExt->getBornTimestamp());               // 9 BORNTIMESTAMP
        $buf .= self::ipAndPortToBytes(
            $bornHost,
            $bornPort,
            ($sysFlag & MessageSysFlag::BORNHOST_V6_FLAG) !== 0
        );                                                                // 10 BORNHOST
        $buf .= pack('J', $messageExt->getStoreTimestamp());              // 11 STORETIMESTAMP
        $buf .= self::ipAndPortToBytes(
            $storeHost,
            $storePort,
            ($sysFlag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) !== 0
        );                                                                // 12 STOREHOST
        $buf .= pack('N', $messageExt->getReconsumeTimes() & 0xFFFFFFFF); // 13 RECONSUMETIMES
        $buf .= pack('J', $messageExt->getPreparedTransactionOffset());   // 14 PREPAREDTRANSACTIONOFFSET
        $buf .= pack('N', $bodyLength & 0xFFFFFFFF);                      // 15 BODY
        $buf .= $body;
        $buf .= pack('C', $topicLen);                                     // 16 TOPIC
        $buf .= $topicBytes;
        $buf .= pack('n', $propertiesLength);                             // 17 PROPERTIES
        $buf .= $propertiesBytes;
        return $buf;
    }

    /**
     * 把 17 段消息解码为 MessageExt（对应 ``MessageDecoder.decode``）。
     */
    public static function decodeMessage(
        string $raw,
        bool $readBody = true,
        bool $decompress = true,
        bool $isClient = true,
        bool $checkCrc = false
    ): ?MessageExt {
        try {
            $msgExt = new MessageExt();
            $offset = 0;
            $storeSize = self::i32($raw, $offset);
            $magicCode = self::i32($raw, $offset);
            if ($magicCode !== self::MESSAGE_MAGIC_CODE && $magicCode !== self::MESSAGE_MAGIC_CODE_V2) {
                // 未知魔数说明版本判定失败 -> decode 返回 null，不当成消息解
                return null;
            }
            $useV2 = $magicCode === self::MESSAGE_MAGIC_CODE_V2;
            $bodyCrc = self::u32($raw, $offset);
            $queueId = self::i32($raw, $offset);
            $flag = self::i32($raw, $offset);
            $queueOffset = self::i64($raw, $offset);
            $physicOffset = self::i64($raw, $offset);
            $sysFlag = self::i32($raw, $offset);
            $bornTimestamp = self::i64($raw, $offset);

            $bornhostLen = ($sysFlag & MessageSysFlag::BORNHOST_V6_FLAG) !== 0 ? 20 : 8;
            [$bornHost, $bornPort] = self::bytesToIpAndPort(self::take($raw, $offset, $bornhostLen));
            $storeTimestamp = self::i64($raw, $offset);
            $storehostLen = ($sysFlag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG) !== 0 ? 20 : 8;
            [$storeHost, $storePort] = self::bytesToIpAndPort(self::take($raw, $offset, $storehostLen));
            $reconsumeTimes = self::i32($raw, $offset);
            $preparedTransactionOffset = self::i64($raw, $offset);

            $msgExt->setStoreSize($storeSize);
            $msgExt->setBodyCrc($bodyCrc);
            $msgExt->setQueueId($queueId);
            $msgExt->setFlag($flag);
            $msgExt->setQueueOffset($queueOffset);
            $msgExt->setCommitLogOffset($physicOffset);
            $msgExt->setSysFlag($sysFlag);
            $msgExt->setBornTimestamp($bornTimestamp);
            $msgExt->setBornHost($bornHost);
            $msgExt->bornHostPort = $bornPort;
            $msgExt->setStoreTimestamp($storeTimestamp);
            $msgExt->setStoreHost($storeHost);
            $msgExt->storeHostPort = $storePort;
            $msgExt->setReconsumeTimes($reconsumeTimes);
            $msgExt->setPreparedTransactionOffset($preparedTransactionOffset);

            // 15 BODY
            $bodyLen = self::i32($raw, $offset);
            if ($bodyLen > 0) {
                if ($readBody) {
                    $body = self::take($raw, $offset, $bodyLen);
                    if ($checkCrc && self::crc32($body) !== $bodyCrc) {
                        throw new \RuntimeException('Msg crc is error');
                    }
                    if ($decompress && ($sysFlag & MessageSysFlag::COMPRESSED_FLAG) !== 0) {
                        $compressionType = MessageSysFlag::getCompressionType($sysFlag);
                        $body = self::decompressInternal($body, $compressionType);
                        $msgExt->setSysFlag(MessageSysFlag::clearCompressedFlag($sysFlag));
                    }
                    $msgExt->setBody($body);
                } else {
                    // readBody=false：跳过 body 且不赋值 -> null（调用方靠 null 区分「没读」与「空 body」）
                    self::need($raw, $offset, $bodyLen);
                    $offset += $bodyLen;
                    $msgExt->setBody(null);
                }
            } else {
                $msgExt->setBody(null);
            }

            // 16 TOPIC
            if ($useV2) {
                $topicLen = self::u16($raw, $offset);
            } else {
                $topicLen = self::u8($raw, $offset);
            }
            $msgExt->setTopic(self::take($raw, $offset, $topicLen));

            // 17 PROPERTIES
            $propertiesLength = self::u16($raw, $offset);
            if ($propertiesLength > 0) {
                $propertiesBytes = self::take($raw, $offset, $propertiesLength);
                $msgExt->setProperties(self::string2MessageProperties($propertiesBytes));
            }

            // msgId = storeHost(ip+port) + commitLogOffset
            $storeAddrRaw = self::ipToBytes($storeHost, $storehostLen === 20) . pack('N', $storePort);
            $msgExt->setMsgId(self::createMessageId($storeAddrRaw, $physicOffset));
            if ($isClient) {
                $msgExt->setOffsetMsgId($msgExt->getMsgId());
            }
            return $msgExt;
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * 把 17 段消息流解码为 MessageExt 列表（对应 ``MessageDecoder.decodes``，用于 pull 结果）。
     *
     * @return list<MessageExt>
     */
    public static function decodeMessages(string $raw, bool $readBody = true): array
    {
        $result = [];
        $pos = 0;
        $total = strlen($raw);
        while ($pos < $total) {
            if ($total - $pos < 4) {
                break;
            }
            $storeSize = (int) unpack('N', substr($raw, $pos, 4))[1];
            if ($storeSize <= 0 || $storeSize > $total - $pos) {
                break;
            }
            $msg = self::decodeMessage(substr($raw, $pos, $storeSize), readBody: $readBody);
            if ($msg === null) {
                break;
            }
            $result[] = $msg;
            $pos += $storeSize;
        }
        return $result;
    }

    // --------------------------------------- 2) 6 段轻量格式：批量消息 body

    /**
     * 批量消息的单条编码（6 段轻量格式）。
     *
     * 只写 TOTALSIZE / MAGICCODE(0) / BODYCRC(0) / FLAG / BODY / PROPERTIES。
     */
    public static function encodeMessage(Message $message): string
    {
        $body = $message->getBody() ?? '';
        $propertiesBytes = self::string2bytes(self::messageProperties2String($message->getProperties()));
        $propertiesLength = strlen($propertiesBytes);
        $storeSize = 4 + 4 + 4 + 4 + 4 + strlen($body) + 2 + $propertiesLength;

        $buf = '';
        $buf .= pack('N', $storeSize & 0xFFFFFFFF);                 // 1 TOTALSIZE
        $buf .= pack('N', 0);                                       // 2 MAGICCODE（批量场景固定 0）
        $buf .= pack('N', 0);                                       // 3 BODYCRC
        $buf .= pack('N', $message->getFlag() & 0xFFFFFFFF);        // 4 FLAG
        $buf .= pack('N', strlen($body) & 0xFFFFFFFF);              // 5 BODY
        $buf .= $body;
        $buf .= pack('n', $propertiesLength);                       // 6 PROPERTIES
        $buf .= $propertiesBytes;
        return $buf;
    }

    /**
     * 多条消息逐条编码后串成一个批量消息 body。
     *
     * @param list<Message> $messages
     */
    public static function encodeMessages(array $messages): string
    {
        $out = '';
        foreach ($messages as $msg) {
            $out .= self::encodeMessage($msg);
        }
        return $out;
    }

    /**
     * 单条批量单元 -> Message（6 段轻量格式的逆解析）。
     */
    public static function decodeBatchMessage(string $raw): Message
    {
        $offset = 0;
        $offset += 4;   // TOTALSIZE
        $offset += 4;   // MAGICCODE
        $offset += 4;   // BODYCRC
        $flag = self::i32($raw, $offset);
        $bodyLen = self::i32($raw, $offset);
        $body = self::take($raw, $offset, $bodyLen);
        $propertiesLen = self::u16($raw, $offset);
        $properties = self::string2MessageProperties(self::take($raw, $offset, $propertiesLen));
        $msg = new Message();
        $msg->setFlag($flag);
        $msg->setBody($body);
        $msg->setProperties($properties);
        return $msg;
    }

    /**
     * 批量消息 body -> Message 列表（按每段 TOTALSIZE 前进）。
     *
     * @return list<Message>
     */
    public static function decodeBatchMessages(string $raw): array
    {
        $result = [];
        $pos = 0;
        $total = strlen($raw);
        while ($pos < $total) {
            if ($total - $pos < 4) {
                break;
            }
            $storeSize = (int) unpack('N', substr($raw, $pos, 4))[1];
            if ($storeSize <= 0 || $storeSize > $total - $pos) {
                break;
            }
            $result[] = self::decodeBatchMessage(substr($raw, $pos, $storeSize));
            $pos += $storeSize;
        }
        return $result;
    }

    /**
     * 数一个批量 body 里内嵌了几条消息（按每段 TOTALSIZE 前进，长度非法即停）。
     */
    public static function countInnerMsgNum(string $raw): int
    {
        $count = 0;
        $pos = 0;
        $total = strlen($raw);
        while ($pos < $total) {
            $count++;
            $size = (int) unpack('N', substr($raw, $pos, 4))[1];
            if ($size <= 0 || $size > $total - $pos) {
                break;
            }
            $pos += $size;
        }
        return $count;
    }

    // ------------------------------------------------------------ 读取原语

    private static function need(string $raw, int $offset, int $len): void
    {
        if ($offset < 0 || $len < 0 || $offset + $len > strlen($raw)) {
            throw new \RuntimeException('buffer underflow');
        }
    }

    private static function take(string $raw, int &$offset, int $len): string
    {
        self::need($raw, $offset, $len);
        $chunk = substr($raw, $offset, $len);
        $offset += $len;
        return $chunk;
    }

    /** 大端有符号 int32（struct ">i"）。 */
    private static function i32(string $raw, int &$offset): int
    {
        $v = (int) unpack('N', self::take($raw, $offset, 4))[1];
        return $v >= 0x80000000 ? $v - 0x100000000 : $v;
    }

    /** 大端无符号 int32（struct ">I"）。 */
    private static function u32(string $raw, int &$offset): int
    {
        return (int) unpack('N', self::take($raw, $offset, 4))[1];
    }

    /** 大端无符号 int16（struct ">H"）。 */
    private static function u16(string $raw, int &$offset): int
    {
        return (int) unpack('n', self::take($raw, $offset, 2))[1];
    }

    /** 大端单字节（struct ">B"）。 */
    private static function u8(string $raw, int &$offset): int
    {
        return ord(self::take($raw, $offset, 1));
    }

    /**
     * 大端有符号 int64（struct ">q"）。
     *
     * PHP unpack('J') 在 64 位构建上返回的就是补码位模式（高位为 1 时已是负数），
     * 与 ">q" 的有符号语义天然一致，无需再转换。
     */
    private static function i64(string $raw, int &$offset): int
    {
        return (int) unpack('J', self::take($raw, $offset, 8))[1];
    }
}

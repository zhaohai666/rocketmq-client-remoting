<?php

declare(strict_types=1);

namespace RocketMQ\Common;

use Throwable;

/**
 * Binary encoding and decoding of messages.
 *
 * The broker side defines two encoding paths, and this class implements both — never mix them up:
 *
 * 1) The 17-segment storage format ``MessageDecoder::encodeMessageExt($msgExt, $needCompress)`` /
 *    ``decode($raw)``
 *    Used for the message body written by the broker and returned by pull/get:
 *
 *    index  field                             encoding
 *    1      TOTALSIZE                         int(4)
 *    2      MAGICCODE                         int(4)  -626843481 (v1) / -626843477 (v2)
 *    3      BODYCRC                           int(4)
 *    4      QUEUEID                           int(4)
 *    5      FLAG                              int(4)
 *    6      QUEUEOFFSET                       long(8)
 *    7      PHYSICALOFFSET                    long(8)
 *    8      SYSFLAG                           int(4)
 *    9      BORNTIMESTAMP                     long(8)
 *    10     BORNHOST                          4|16B ip + 4B port
 *    11     STORETIMESTAMP                    long(8)
 *    12     STOREHOST                         4|16B ip + 4B port
 *    13     RECONSUMETIMES                    int(4)
 *    14     PREPAREDTRANSACTIONOFFSET         long(8)
 *    15     BODY                              int(4) len + body
 *    16     TOPIC                             1B(v1)|2B(v2) len + topic
 *    17     PROPERTIES                        short(2) len + k\x01v\x02 string
 *
 * 2) The 6-segment lightweight format ``encodeMessage(Message)`` / ``decodeBatchMessage($raw)``
 *    Used **only for batch messages** (the body of MessageBatch), without topic / crc:
 *
 *    TOTALSIZE(4) | MAGICCODE(4, fixed 0) | BODYCRC(4, fixed 0) | FLAG(4)
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

    // Fixed field offsets (the byte positions in the 17-segment storage format)
    public const MESSAGE_MAGIC_CODE_POSITION = 4;
    public const MESSAGE_FLAG_POSITION = 16;
    public const MESSAGE_PHYSIC_OFFSET_POSITION = 28;
    public const QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4;
    public const PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
    public const SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
    public const MESSAGE_STORE_TIMESTAMP_POSITION = 56;

    private const HEX_TABLE = '0123456789ABCDEF';

    // ---------------------------------------------------------------- basic utilities

    /**
     * Text fields in the protocol are always UTF-8.
     * A PHP string is a byte string anyway, so this is only a semantic annotation (the argument must be
     * UTF-8 text).
     */
    public static function string2bytes(string $s): string
    {
        return $s;
    }

    /**
     * Converts byte by byte to **upper-case** hex: the case of a msgId is a hard requirement, a
     * lower-case string cannot be decoded into host/port.
     */
    public static function bytes2string(string $bs): string
    {
        return strtoupper(bin2hex($bs));
    }

    /**
     * Hex string -> bytes (the inverse of bytes2string).
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
     * BODYCRC uses the standard CRC32 (poly 0xEDB88320); the PHP built-in crc32() takes the same
     * parameter, so the value can be compared directly against what the broker wrote.
     */
    public static function crc32(string $data): int
    {
        return crc32($data) & 0xFFFFFFFF;
    }

    // ------------------------------------------------------- property string <-> Map

    /**
     * Property map -> protocol string: concatenated item by item as `name\x01value\x02`.
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
     * Protocol string -> property map (the inverse of messageProperties2String).
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
     * ip+port(8 or 20B) + 8B commitLogOffset -> hex msgId.
     */
    public static function createMessageId(string $addrBytes, int $offset): string
    {
        return self::bytes2string($addrBytes . pack('J', $offset));
    }

    /**
     * Hex msgId -> [ip, port, offset] (the inverse of createMessageId).
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

    // ------------------------------------------------- compression / decompression (optional dependency)

    /**
     * Normalizes the compression type decoded from sysFlag to the "real algorithm".
     *
     * The backward-compatible mapping of compression type numbers (the protocol only has these values)::
     *
     *     1 -> LZ4
     *     2 -> ZSTD
     *     0 -> ZLIB   (the old message format without type-bit capability, its type bits are simply 0)
     *     3 -> ZLIB
     *
     * That is, **compressed messages from old versions whose type bits are 0 are handled as ZLIB**. This
     * is required: compressed messages produced by old clients (without type-bit capability) have type
     * bits of 0, and without mapping them to ZLIB the decompression fails, while the outer layer still
     * clears COMPRESSED_FLAG as usual, resulting in **silently returning the compressed byte stream** —
     * data corruption that cannot be recognized afterwards.
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
            // The LZ4 Frame format (the self-describing frame with magic 0x184D2204), pure PHP implementation
            return CompressionCodec::lz4CompressFrame($data);
        }
        if ($ctype === MessageSysFlag::ZSTD_TYPE) {
            // A legal ZSTD frame (the CLI gives real compression when present; Raw/RLE blocks without
            // it); the broker parses it as a standard ZSTD frame, so both variants can be decoded.
            return CompressionCodec::zstdCompress($data);
        }
        throw self::unsupported($compressionType);
    }

    private static function decompressInternal(string $data, int $compressionType): string
    {
        $ctype = self::normalizeCompressionType($compressionType);
        if ($ctype === MessageSysFlag::ZLIB_TYPE) {
            // gzcompress produces the zlib format (RFC1950), so restoring it can only use gzuncompress;
            // gzinflate only reads bare deflate and gzdecode only reads the gzip format (RFC1952), so
            // neither of them can be used
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
     * An unknown compression type must throw.
     *
     * The type bits have only four legal values, 0/3(ZLIB), 1(LZ4), 2(ZSTD); anything else is dirty
     * data. **It must never be passed through as-is**: the caller (``decodeMessage``) clears
     * ``COMPRESSED_FLAG`` after decompression, so passing it through amounts to handing out the
     * compressed byte stream as payload with no way to recognize it afterwards, which is silent data
     * corruption.
     */
    private static function unsupported(int $compressionType): \RuntimeException
    {
        return new \RuntimeException(sprintf('unsupported compression type: %d', $compressionType));
    }

    /**
     * Decompresses according to the compression type (the public entry point of
     * ``decompressInternal``).
     *
     * Besides ``decodeMessage`` there is a second caller: the Request-Reply answer is pushed straight
     * over as a bare packet from ``PUSH_REPLY_MESSAGE_TO_CLIENT(326)`` without going through the message
     * decoding path, so it has to inspect ``sysFlag`` itself and decompress here.
     */
    public static function decompressBody(string $data, int $compressionType): string
    {
        return self::decompressInternal($data, $compressionType);
    }

    // ------------------------------------------- 1) 17-segment storage format: MessageExt

    private static function topicLengthSize(int $magicCode): int
    {
        return $magicCode === self::MESSAGE_MAGIC_CODE_V2 ? 2 : 1;
    }

    public static function encodeMessageExt(MessageExt $messageExt, bool $needCompress = false): string
    {
        /**
         * 17-segment storage format encoding: when needCompress is true and sysFlag has COMPRESSED set,
         * the body is compressed first.
         *
         * Note: the topic length is always written as 1 byte and the magic code is always written as v1
         * (-626843481); when storeSize > 0 the buffer is allocated according to it (and extended on
         * demand when the tail is short).
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
     * Decodes a 17-segment message into a MessageExt (corresponds to ``MessageDecoder.decode``).
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
                // An unknown magic code means version detection failed -> decode returns null, it is not
                // decoded as a message
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
                    // readBody=false: the body is skipped and not assigned -> null (the caller relies on
                    // null to tell "not read" apart from "empty body")
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
     * Decodes a stream of 17-segment messages into a list of MessageExt (corresponds to
     * ``MessageDecoder.decodes``, used for pull results).
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

    // --------------------------------------- 2) 6-segment lightweight format: batch message body

    /**
     * The single-message encoding of a batch message (6-segment lightweight format).
     *
     * Only TOTALSIZE / MAGICCODE(0) / BODYCRC(0) / FLAG / BODY / PROPERTIES are written.
     */
    public static function encodeMessage(Message $message): string
    {
        $body = $message->getBody() ?? '';
        $propertiesBytes = self::string2bytes(self::messageProperties2String($message->getProperties()));
        $propertiesLength = strlen($propertiesBytes);
        $storeSize = 4 + 4 + 4 + 4 + 4 + strlen($body) + 2 + $propertiesLength;

        $buf = '';
        $buf .= pack('N', $storeSize & 0xFFFFFFFF);                 // 1 TOTALSIZE
        $buf .= pack('N', 0);                                       // 2 MAGICCODE (fixed 0 in the batch scenario)
        $buf .= pack('N', 0);                                       // 3 BODYCRC
        $buf .= pack('N', $message->getFlag() & 0xFFFFFFFF);        // 4 FLAG
        $buf .= pack('N', strlen($body) & 0xFFFFFFFF);              // 5 BODY
        $buf .= $body;
        $buf .= pack('n', $propertiesLength);                       // 6 PROPERTIES
        $buf .= $propertiesBytes;
        return $buf;
    }

    /**
     * Encodes several messages one by one and concatenates them into a single batch message body.
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
     * A single batch unit -> Message (the inverse parsing of the 6-segment lightweight format).
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
     * Batch message body -> list<Message> (advancing by the TOTALSIZE of each segment).
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
     * Counts how many messages are embedded in a batch body (advancing by the TOTALSIZE of each
     * segment, stopping as soon as a length is illegal).
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

    // ------------------------------------------------------------ read primitives

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

    /** Big-endian signed int32 (struct ">i"). */
    private static function i32(string $raw, int &$offset): int
    {
        $v = (int) unpack('N', self::take($raw, $offset, 4))[1];
        return $v >= 0x80000000 ? $v - 0x100000000 : $v;
    }

    /** Big-endian unsigned int32 (struct ">I"). */
    private static function u32(string $raw, int &$offset): int
    {
        return (int) unpack('N', self::take($raw, $offset, 4))[1];
    }

    /** Big-endian unsigned int16 (struct ">H"). */
    private static function u16(string $raw, int &$offset): int
    {
        return (int) unpack('n', self::take($raw, $offset, 2))[1];
    }

    /** Big-endian single byte (struct ">B"). */
    private static function u8(string $raw, int &$offset): int
    {
        return ord(self::take($raw, $offset, 1));
    }

    /**
     * Big-endian signed int64 (struct ">q").
     *
     * On a 64-bit build PHP's unpack('J') already returns the two's-complement bit pattern (when the
     * high bit is 1 it is already negative), which naturally matches the signed semantics of ">q", so no
     * further conversion is needed.
     */
    private static function i64(string $raw, int &$offset): int
    {
        return (int) unpack('J', self::take($raw, $offset, 8))[1];
    }
}

<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * RocketMQ 4.x 私有二进制协议（对应 org.apache.rocketmq.remoting.protocol.RocketMQSerializable，
 * 移植自 protocol/serialize.py 的 RocketMQSerializable）。
 *
 * 格式：code(2) + language(1) + version(2) + opaque(4) + flag(4)
 *       + remark(int+utf8) + extFields(int + key(short+utf8) / value(int+utf8))。
 * 端序一律大端；**字节级必须与 Python/Java 一致**。
 */
final class RocketMQSerializable
{
    /**
     * 带 4 字节大端长度前缀的十进制 long 写出（对应 Java RocketMQSerializable.writeDecimalLong）。
     */
    public static function writeDecimalLong(string &$buf, int $value): void
    {
        $buf .= pack('N', 0);
        $start = strlen($buf);
        if ($value === 0) {
            $buf .= '0';
        } else {
            $neg = $value < 0;
            if ($neg) {
                $buf .= '-';
                if ($value === PHP_INT_MIN) {
                    $buf .= '9223372036854775808';
                    $buf = substr_replace($buf, pack('N', strlen($buf) - $start), $start - 4, 4);
                    return;
                }
                $value = -$value;
            }
            $buf .= (string)$value;
        }
        $buf = substr_replace($buf, pack('N', strlen($buf) - $start), $start - 4, 4);
    }

    public static function writeDecimalInt(string &$buf, int $value): void
    {
        self::writeDecimalLong($buf, $value);
    }

    /** 写一个带长度前缀的 UTF-8 字符串（useShortLength：2 字节 vs 4 字节长度）。 */
    public static function writeStr(string &$buf, bool $useShortLength, ?string $s): void
    {
        $bs = $s === null ? '' : $s;
        $n = strlen($bs);
        if ($useShortLength) {
            $buf .= pack('n', $n);
        } else {
            $buf .= pack('N', $n);
        }
        $buf .= $bs;
    }

    /**
     * 读一个带长度前缀的 UTF-8 字符串；长度 0 → null（对应 Python read_str）。
     *
     * @return array{0: ?string, 1: int} [value, newOffset]
     */
    public static function readStr(string $buf, int $offset, bool $useShortLength): array
    {
        if ($useShortLength) {
            $n = unpack('n', substr($buf, $offset, 2))[1];
            $offset += 2;
        } else {
            $n = unpack('N', substr($buf, $offset, 4))[1];
            $offset += 4;
        }
        if ($n === 0) {
            return [null, $offset];
        }
        $s = substr($buf, $offset, $n);
        return [$s, $offset + $n];
    }

    /** @param array<string, string>|null $mapData */
    public static function mapSerialize(?array $mapData): ?string
    {
        if ($mapData === null || $mapData === []) {
            return null;
        }
        $buf = '';
        foreach ($mapData as $k => $v) {
            if ($k === null || $v === null) {
                continue;
            }
            self::writeStr($buf, true, (string)$k);
            self::writeStr($buf, false, (string)$v);
        }
        return $buf;
    }

    public static function calTotalLen(?string $remark, ?string $ext): int
    {
        $remarkLen = ($remark === null || $remark === '') ? 0 : strlen($remark);
        $extLen = ($ext === null || $ext === '') ? 0 : strlen($ext);
        return 2 + 1 + 2 + 4 + 4 + 4 + $remarkLen + 4 + $extLen;
    }

    /**
     * 编码命令头（对应 Python rocket_mq_protocol_encode）。
     * 顺序：code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) extFields(int + map)。
     */
    public static function rocketMqProtocolEncode(RemotingCommand $cmd): string
    {
        $remarkBytes = ($cmd->remark !== null && $cmd->remark !== '') ? $cmd->remark : null;
        $extFieldsBytes = self::mapSerialize($cmd->extFields);
        $totalLen = self::calTotalLen($remarkBytes, $extFieldsBytes);
        $buf = '';
        $buf .= pack('n', $cmd->code & 0xFFFF);
        $buf .= pack('C', $cmd->language & 0xFF);
        $buf .= pack('n', $cmd->version & 0xFFFF);
        $buf .= pack('N', $cmd->opaque & 0xFFFFFFFF);
        $buf .= pack('N', $cmd->flag & 0xFFFFFFFF);
        if ($remarkBytes !== null) {
            $buf .= pack('N', strlen($remarkBytes));
            $buf .= $remarkBytes;
        } else {
            $buf .= pack('N', 0);
        }
        if ($extFieldsBytes !== null) {
            $buf .= pack('N', strlen($extFieldsBytes));
            $buf .= $extFieldsBytes;
        } else {
            $buf .= pack('N', 0);
        }
        assert(strlen($buf) === $totalLen, 'rocketmq protocol encode length mismatch');
        return $buf;
    }

    /**
     * 解码 extFields map（key 用 short 长度、value 用 int 长度）。
     *
     * @return array{0: array<string, string>, 1: int} [map, newOffset]
     */
    public static function mapDeserialize(string $buf, int $offset, int $length): array
    {
        $mapData = [];
        $end = $offset + $length;
        while ($offset < $end) {
            [$k, $offset] = self::readStr($buf, $offset, true);
            [$v, $offset] = self::readStr($buf, $offset, false);
            if ($k !== null && $v !== null) {
                $mapData[$k] = $v;
            }
        }
        return [$mapData, $offset];
    }

    /**
     * 解码 ROCKETMQ 二进制命令头（对应 Python rocket_mq_protocol_decode）。
     *
     * @return array{code: int, language: int, version: int, opaque: int, flag: int, remark: ?string, extFields: ?array<string, string>}
     */
    public static function rocketMqProtocolDecode(string $headerBytes): array
    {
        $offset = 0;
        // ">h" 是有符号 short；这里按无符号读再截 16 位，与 Python 的 code & 0xFFFF 一致
        $code = unpack('n', substr($headerBytes, $offset, 2))[1];
        $offset += 2;
        $language = ord($headerBytes[$offset]);
        $offset += 1;
        $version = unpack('n', substr($headerBytes, $offset, 2))[1];
        $offset += 2;
        $opaque = self::unpackSignedInt(substr($headerBytes, $offset, 4));
        $offset += 4;
        $flag = self::unpackSignedInt(substr($headerBytes, $offset, 4));
        $offset += 4;
        [$remark, $offset] = self::readStr($headerBytes, $offset, false);
        $extLen = unpack('N', substr($headerBytes, $offset, 4))[1];
        $offset += 4;
        $extFields = null;
        if ($extLen > 0) {
            [$extFields, $offset] = self::mapDeserialize($headerBytes, $offset, $extLen);
        }
        return [
            'code' => $code & 0xFFFF,
            'language' => $language,
            'version' => $version & 0xFFFF,
            'opaque' => $opaque,
            'flag' => $flag,
            'remark' => $remark,
            'extFields' => $extFields,
        ];
    }

    /** 4 字节大端 → 有符号 int（对应 Python struct ">i" 的语义）。 */
    public static function unpackSignedInt(string $bytes): int
    {
        $v = unpack('N', $bytes)[1];
        return $v > 0x7FFFFFFF ? $v - 0x100000000 : $v;
    }
}

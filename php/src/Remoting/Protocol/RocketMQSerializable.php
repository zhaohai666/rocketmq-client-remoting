<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * The compact ROCKETMQ binary command header (header serialize type 1, chosen alternatively with the
 * JSON header).
 *
 * Format: code(2) + language(1) + version(2) + opaque(4) + flag(4)
 *       + remark(int+utf8) + extFields(int + key(short+utf8) / value(int+utf8)).
 * The endianness is always big-endian; **at the byte level it has to be aligned with the broker-side
 * parser byte for byte**, one byte off makes the peer report a parsing failure.
 */
final class RocketMQSerializable
{
    /**
     * Writes a decimal number with a 4-byte big-endian length prefix: writes a placeholder length first,
     * then backfills the real byte count.
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

    /** Writes a length-prefixed UTF-8 string (useShortLength: 2-byte vs 4-byte length). */
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
     * Reads a length-prefixed UTF-8 string; length 0 -> null.
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
     * Encodes a command header.
     * Order: code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) extFields(int + map).
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
     * Decodes the extFields map (key uses a short length, value uses an int length).
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
     * Decodes a ROCKETMQ binary command header (field order matches the encoder).
     *
     * @return array{code: int, language: int, version: int, opaque: int, flag: int, remark: ?string, extFields: ?array<string, string>}
     */
    public static function rocketMqProtocolDecode(string $headerBytes): array
    {
        $offset = 0;
        // ">h" is a signed short; read it as unsigned and truncate to 16 bits so code is always 0..65535
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

    /** 4-byte big-endian -> signed int: values above 0x7FFFFFFF wrap back to negative. */
    public static function unpackSignedInt(string $bytes): int
    {
        $v = unpack('N', $bytes)[1];
        return $v > 0x7FFFFFFF ? $v - 0x100000000 : $v;
    }
}

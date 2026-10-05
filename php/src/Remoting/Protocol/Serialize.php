<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * 协议序列化：JSON（RemotingSerializable）与 fastjson2 兼容解析（移植自 protocol/serialize.py）。
 *
 * 对应 org.apache.rocketmq.remoting.protocol.RemotingSerializable。
 * ROCKETMQ 私有二进制部分见 RocketMQSerializable.php。
 */

/** fastjson2 兼容解析失败（对应 Python FastJsonDecodeError(ValueError)）。 */
class FastJsonDecodeError extends \InvalidArgumentException
{
}

/**
 * FastJSON（RocketMQ 5.x 默认 JSON 实现）会把 Map 的数字键写成不带引号的形式，
 * 例如 {"brokerAddrs":{0:"127.0.0.1:10911"}}，这不是标准 JSON。
 */
final class FastJsonParser
{
    private const ESCAPES = [
        '"' => '"', '\\' => '\\', '/' => '/',
        'b' => "\b", 'f' => "\f", 'n' => "\n", 'r' => "\r", 't' => "\t",
    ];
    private const WS = " \t\r\n";

    private string $text;
    private int $pos = 0;
    private int $size;

    public function __construct(string $text)
    {
        $this->text = $text;
        $this->size = strlen($text);
    }

    // ---------------- 基础设施 ----------------

    private function skipWs(): void
    {
        while ($this->pos < $this->size && strpos(self::WS, $this->text[$this->pos]) !== false) {
            $this->pos++;
        }
    }

    private function peek(): string
    {
        if ($this->pos >= $this->size) {
            throw new FastJsonDecodeError("unexpected end of input at offset {$this->pos}");
        }
        return $this->text[$this->pos];
    }

    // ---------------- 各类型 ----------------

    public function parse(): mixed
    {
        $value = $this->value();
        $this->skipWs();
        if ($this->pos !== $this->size) {
            throw new FastJsonDecodeError("trailing data at offset {$this->pos}");
        }
        return $value;
    }

    private function value(): mixed
    {
        $this->skipWs();
        $c = $this->peek();
        if ($c === '{') {
            return $this->object();
        }
        if ($c === '[') {
            return $this->array();
        }
        if ($c === '"') {
            return $this->string();
        }
        return $this->literal();
    }

    /** @return array<string, mixed> */
    private function object(): array
    {
        $this->pos++; // 吃掉 '{'
        $result = [];
        $this->skipWs();
        if ($this->peek() === '}') {
            $this->pos++;
            return $result;
        }
        while (true) {
            $key = $this->key();
            $this->skipWs();
            if ($this->peek() !== ':') {
                throw new FastJsonDecodeError("expected ':' at offset {$this->pos}");
            }
            $this->pos++;
            $result[$key] = $this->value();
            $this->skipWs();
            $c = $this->peek();
            if ($c === ',') {
                $this->pos++;
                $this->skipWs();
                if ($this->peek() === '}') { // 容忍尾随逗号
                    $this->pos++;
                    return $result;
                }
                continue;
            }
            if ($c === '}') {
                $this->pos++;
                return $result;
            }
            throw new FastJsonDecodeError("expected ',' or '}' at offset {$this->pos}");
        }
    }

    /** @return list<mixed> */
    private function array(): array
    {
        $this->pos++; // 吃掉 '['
        $items = [];
        $this->skipWs();
        if ($this->peek() === ']') {
            $this->pos++;
            return $items;
        }
        while (true) {
            $items[] = $this->value();
            $this->skipWs();
            $c = $this->peek();
            if ($c === ',') {
                $this->pos++;
                $this->skipWs();
                if ($this->peek() === ']') {
                    $this->pos++;
                    return $items;
                }
                continue;
            }
            if ($c === ']') {
                $this->pos++;
                return $items;
            }
            throw new FastJsonDecodeError("expected ',' or ']' at offset {$this->pos}");
        }
    }

    private function key(): string
    {
        // 键：字符串 / 内联对象 / 内联数组 / 裸字面量（数字、true、false、null）。
        $this->skipWs();
        $c = $this->peek();
        if ($c === '"') {
            return $this->string();
        }
        if ($c === '{' || $c === '[') {
            // fastjson2 把非字符串 map 键（如 MessageQueue）原样写成 JSON。
            // 保留原始文本，调用方可用 decodeMessageQueueKey() 再解析。
            $start = $this->pos;
            $this->value();
            return substr($this->text, $start, $this->pos - $start);
        }
        $start = $this->pos;
        while ($this->pos < $this->size && $this->text[$this->pos] !== ':' && strpos(self::WS, $this->text[$this->pos]) === false) {
            $this->pos++;
        }
        return substr($this->text, $start, $this->pos - $start);
    }

    private function string(): string
    {
        $this->pos++; // 吃掉开引号
        $out = [];
        while (true) {
            if ($this->pos >= $this->size) {
                throw new FastJsonDecodeError('unterminated string');
            }
            $c = $this->text[$this->pos];
            if ($c === '"') {
                $this->pos++;
                return implode('', $out);
            }
            if ($c === '\\') {
                $this->pos++;
                if ($this->pos >= $this->size) {
                    throw new FastJsonDecodeError('unterminated escape');
                }
                $e = $this->text[$this->pos];
                if ($e === 'u') {
                    $digits = substr($this->text, $this->pos + 1, 4);
                    if (strlen($digits) !== 4 || !ctype_xdigit($digits)) {
                        throw new FastJsonDecodeError('bad \\u escape');
                    }
                    $out[] = mb_chr((int)hexdec($digits), 'UTF-8');
                    $this->pos += 5;
                    continue;
                }
                $out[] = self::ESCAPES[$e] ?? $e;
                $this->pos++;
                continue;
            }
            $out[] = $c;
            $this->pos++;
        }
    }

    private function literal(): mixed
    {
        $rest = substr($this->text, $this->pos);
        foreach (['true' => true, 'false' => false, 'null' => null,
                  'NaN' => NAN, 'Infinity' => INF, '-Infinity' => -INF] as $token => $value) {
            if (str_starts_with($rest, $token)) {
                $tail = substr($rest, strlen($token), 1);
                // 不能把 "nullish" 之类误判成 null
                if ($tail === '' || $tail === ',' || $tail === '}' || $tail === ']' || strpos(self::WS, $tail) !== false) {
                    $this->pos += strlen($token);
                    return $value;
                }
            }
        }
        if (!preg_match('/-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?/A', $rest, $m)) {
            $snippet = substr($rest, 0, 20);
            throw new FastJsonDecodeError("unexpected token at offset {$this->pos}: {$snippet}");
        }
        $this->pos += strlen($m[0]);
        $raw = $m[0];
        if (strpbrk($raw, '.eE') !== false) {
            return (float)$raw;
        }
        return (int)$raw;
    }
}

final class RemotingSerializable
{
    /** fastjson2（RocketMQ 5.x 默认 JSON 实现）的输出**不是**严格 JSON，仅靠 json_decode 无法解析：
     *  1) Map 的数字键不带引号：{"brokerAddrs":{0:"127.0.0.1:10911"}}
     *  2) Map 的对象键直接内联成 JSON 对象（这是**非法 JSON**），管理端必然遇到：
     *     TopicStatsTable / ConsumeStats 的 offsetTable 以 MessageQueue 为键
     *  3) 特殊浮点：NaN / Infinity / -Infinity
     *  因此需要一个容忍「非字符串键」的小型解析器，而不是事后补引号。
     */
    public static function fastjsonLoads(string $text): mixed
    {
        return (new FastJsonParser($text))->parse();
    }

    /**
     * 把 fastjson2 写出的 MessageQueue 内联对象键还原成 array。
     * 返回 null 表示这个键不是内联对象（例如普通字符串键）。
     *
     * @return array<string, mixed>|null
     */
    public static function decodeMessageQueueKey(string $key): ?array
    {
        $text = trim($key);
        if (!str_starts_with($text, '{')) {
            return null;
        }
        try {
            $v = self::fastjsonLoads($text);
            return is_array($v) ? $v : null;
        } catch (FastJsonDecodeError) {
            return null;
        }
    }

    private static function tolerantJsonLoads(string $text): mixed
    {
        try {
            $v = json_decode($text, true, 512, JSON_THROW_ON_ERROR);
            if (is_array($v) || is_scalar($v) || $v === null) {
                return $v;
            }
        } catch (\JsonException) {
            // fall through
        }
        try {
            return self::fastjsonLoads($text);
        } catch (FastJsonDecodeError) {
            // 兜底：老路径（只补数字键引号）。再失败则让异常冒泡，避免静默返回错数据。
            $fixed = preg_replace(
                '/([{,]\s*)(-?\d+(?:\.\d+)?)\s*(:)/',
                '${1}"${2}"${3}',
                $text
            );
            return json_decode((string)$fixed, true, 512, JSON_THROW_ON_ERROR);
        }
    }

    // ---------------- 编码（PHP 端 → 线上字节）----------------

    /**
     * 对应 Python RemotingSerializable.encode：None → b""，bytes 原样，其余 JSON UTF-8。
     * JSON 端序：ensure_ascii=False + 不转义斜杠（与 Python json.dumps 对齐）。
     */
    public static function encode(mixed $obj): string
    {
        if ($obj === null) {
            return '';
        }
        if (is_string($obj)) {
            return $obj;
        }
        return json_encode($obj, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
    }

    public static function toJson(mixed $obj, bool $prettyFormat = false): string
    {
        $flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR;
        if ($prettyFormat) {
            $flags |= JSON_PRETTY_PRINT;
        }
        return json_encode($obj, $flags);
    }

    // ---------------- 解码（线上字节 → PHP）----------------

    public static function decode(?string $data): mixed
    {
        if ($data === null || $data === '') {
            return null;
        }
        return self::fromJson($data);
    }

    /** @param class-string|null $expectClass 带 fromDict() 的目标类（对应 Python expect_type） */
    public static function fromJson(string $jsonStr, ?string $expectClass = null): mixed
    {
        $obj = json_decode($jsonStr, true, 512, JSON_THROW_ON_ERROR);
        if ($expectClass !== null) {
            try {
                return $expectClass::fromDict($obj);
            } catch (\Throwable) {
                return $obj;
            }
        }
        return $obj;
    }

    /** 容忍 fastjson2 非标准输出（对应 Python decode_json → _tolerant_json_loads）。 */
    public static function decodeJson(string $data): mixed
    {
        return self::tolerantJsonLoads($data);
    }
}

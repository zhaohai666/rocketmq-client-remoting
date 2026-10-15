<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * Protocol serialization: JSON command header / body encode-decode, plus tolerant parsing of the
 * broker's non-standard JSON.
 *
 * See RocketMQSerializable.php for the ROCKETMQ private binary part.
 */

/** Tolerant parsing still failed: the input does not even match the shape of non-standard JSON. */
class FastJsonDecodeError extends \InvalidArgumentException
{
}

/**
 * FastJSON (RocketMQ 5.x's default JSON implementation) writes the numeric keys of a Map without
 * quotes, e.g. {"brokerAddrs":{0:"127.0.0.1:10911"}}, which is not standard JSON.
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

    // ---------------- Infrastructure ----------------

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

    // ---------------- Value types ----------------

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
        $this->pos++; // consume '{'
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
                if ($this->peek() === '}') { // tolerate a trailing comma
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
        $this->pos++; // consume '['
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
        // Key: string / inline object / inline array / bare literal (number, true, false, null).
        $this->skipWs();
        $c = $this->peek();
        if ($c === '"') {
            return $this->string();
        }
        if ($c === '{' || $c === '[') {
            // fastjson2 writes non-string map keys (e.g. MessageQueue) verbatim as JSON.
            // Keep the raw text; callers can re-parse it with decodeMessageQueueKey().
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
        $this->pos++; // consume the opening quote
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
                // Do not mistake something like "nullish" for null
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
    /** fastjson2 (RocketMQ 5.x's default JSON implementation) output is **not** strict JSON and
     *  json_decode alone cannot parse it:
     *  1) Map numeric keys are unquoted: {"brokerAddrs":{0:"127.0.0.1:10911"}}
     *  2) Map object keys are inlined directly as JSON objects (**illegal JSON**); the admin path
     *     always hits this: the offsetTable of TopicStatsTable / ConsumeStats is keyed by MessageQueue
     *  3) Special floats: NaN / Infinity / -Infinity
     *  Hence a small parser that tolerates "non-string keys" is needed instead of patching quotes
     *  after the fact.
     */
    public static function fastjsonLoads(string $text): mixed
    {
        return (new FastJsonParser($text))->parse();
    }

    /**
     * Restores the inline-object MessageQueue keys written by fastjson2 into an array.
     * Returning null means the key is not an inline object (e.g. an ordinary string key).
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
            // Last resort: the old path (quote numeric keys only). If that also fails, let the
            // exception bubble up rather than silently returning wrong data.
            $fixed = preg_replace(
                '/([{,]\s*)(-?\d+(?:\.\d+)?)\s*(:)/',
                '${1}"${2}"${3}',
                $text
            );
            return json_decode((string)$fixed, true, 512, JSON_THROW_ON_ERROR);
        }
    }

    // ---------------- Encode (PHP side -> wire bytes) ----------------

    /**
     * null -> empty string, strings pass through as-is, everything else is output as UTF-8 JSON.
     * The JSON side does not escape non-ASCII or slashes, staying consistent with the literals
     * the broker reads.
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

    // ---------------- Decode (wire bytes -> PHP) ----------------

    public static function decode(?string $data): mixed
    {
        if ($data === null || $data === '') {
            return null;
        }
        return self::fromJson($data);
    }

    /** @param class-string|null $expectClass target class with fromDict(); falls back to the raw array if construction fails */
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

    /** Tolerates the broker-side fastjson non-standard JSON output (unquoted numeric keys etc.). */
    public static function decodeJson(string $data): mixed
    {
        return self::tolerantJsonLoads($data);
    }
}

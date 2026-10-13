<?php

declare(strict_types=1);

namespace RocketMQ\Client;

class DefaultTopAddressing
{
    public const DEFAULT_NAMESRV_ADDR_LOOKUP = 'jmenv.tbsite.net';
    public const DEFAULT_DOMAIN_SUBGROUP = 'nsaddr';

    public string $wsAddr;

    public ?string $nsAddr = null;

    private int $timeoutMillis;
    private string $unitName;

    /** @var array<string, string>|null */
    private ?array $para;

    /**
     * @param array<string, string>|null $para
     */
    public function __construct(
        ?string $wsAddr = null,
        string $unitName = '',
        ?array $para = null,
        int $timeoutMillis = 3000,
        ?string $domain = null,
        ?string $subgroup = null,
    ) {
        $this->timeoutMillis = $timeoutMillis;
        $this->unitName = $unitName;
        $this->para = $para !== null ? $para : null;
        if ($wsAddr !== null && $wsAddr !== '') {
            $this->wsAddr = $wsAddr;
        } else {
            $dom = $domain ?? (string) getenv('ROCKETMQ_NAMESRV_DOMAIN');
            $this->wsAddr = $dom !== '' ? self::getWsAddr($dom, $subgroup) : '';
        }
    }

    /**
     * 对应 ``DefaultTopAddressing.clearNewLine``：trim 后截断到第一个 \r 或 \n。
     * （落成静态方法以便 autoload。）
     */
    public static function clearNewLine(string $content): string
    {
        $s = trim($content);
        $idx = strpos($s, "\r");
        if ($idx !== false) {
            return substr($s, 0, $idx);
        }
        $idx = strpos($s, "\n");
        if ($idx !== false) {
            return substr($s, 0, $idx);
        }
        return $s;
    }

    // ---------------- URL 构造（MixAll.getWSAddr / fetchNSAddr 语义）----------------

    /** 对应 ``MixAll.getWSAddr``：domain 自带端口（含 ':'）时不追加默认 :8080。 */
    public static function getWsAddr(string $domain, ?string $subgroup = null): string
    {
        $grp = ($subgroup !== null && $subgroup !== '') ? $subgroup : self::DEFAULT_DOMAIN_SUBGROUP;
        if (str_contains($domain, ':')) {
            return sprintf('http://%s/rocketmq/%s', $domain, $grp);
        }
        return sprintf('http://%s:8080/rocketmq/%s', $domain, $grp);
    }

    /** 对应 ``fetchNSAddr`` 里的 URL 拼装（unitName / para 规则逐条照抄）。 */
    public function buildUrl(): string
    {
        $url = $this->wsAddr;
        if ($this->para !== null && $this->para !== []) {
            if (trim($this->unitName) !== '') {
                $url = sprintf('%s-%s?nofix=1&', $url, $this->unitName);
            } else {
                $url = $url . '?';
            }
            $parts = [];
            foreach ($this->para as $k => $v) {
                $parts[] = sprintf('%s=%s', $k, $v);
            }
            $url = $url . implode('&', $parts);
        } else {
            if (trim($this->unitName) !== '') {
                $url = sprintf('%s-%s?nofix=1', $url, $this->unitName);
            }
        }
        return $url;
    }

    /** 动态取址是否可用（domain 已显式配置）。 */
    public static function isConfigured(): bool
    {
        return getenv('ROCKETMQ_NAMESRV_DOMAIN') !== false
            && (string) getenv('ROCKETMQ_NAMESRV_DOMAIN') !== '';
    }

    // ---------------- 取址 ----------------

    public function fetchNsAddr(bool $verbose = true): ?string
    {
        if ($this->wsAddr === '') {
            return null;
        }
        $url = $this->buildUrl();
        try {
            $body = $this->httpGet($url, $this->timeoutMillis / 1000.0);
            if ($body !== null) {
                return self::clearNewLine($body);
            }
            if ($verbose) {
                Logger::error(sprintf('fetch nameserver address failed, statusCode!=200 url=%s', $url));
            }
        } catch (\Throwable $e) {
            if ($verbose) {
                Logger::debug(sprintf('fetch name server address exception url=%s: %s', $url, $e->getMessage()));
            }
        }
        return null;
    }

    public function fetchAndApply(): ?string
    {
        $addrs = $this->fetchNsAddr();
        if ($addrs !== null && trim($addrs) !== '') {
            if ($addrs !== $this->nsAddr) {
                Logger::info(sprintf('name server address changed, old=%s, new=%s', (string) $this->nsAddr, $addrs));
                $this->nsAddr = $addrs;
                return $this->nsAddr;
            }
        }
        return null;
    }

    // ---------------- 传输 ----------------

    public function httpGet(string $url, float $timeoutSeconds): ?string
    {
        $timeoutMs = max(1, (int) round($timeoutSeconds * 1000));
        if (function_exists('curl_init')) {
            $ch = curl_init($url);
            if ($ch === false) {
                throw new \RuntimeException('curl_init failed');
            }
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT_MS => $timeoutMs,
                CURLOPT_HTTPHEADER => ['Accept: */*'],
            ]);
            $body = curl_exec($ch);
            $status = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
            $err = curl_error($ch);
            curl_close($ch);
            if ($body === false) {
                throw new \RuntimeException('http get failed: ' . $err);
            }
            if ($status !== 200) {
                throw new \RuntimeException(sprintf('http status %d', $status));
            }
            return (string) $body;
        }

        $ctx = stream_context_create([
            'http' => [
                'method' => 'GET',
                'timeout' => $timeoutSeconds,
                'header' => "Accept: */*\r\n",
                'ignore_errors' => true,
            ],
        ]);
        $body = @file_get_contents($url, false, $ctx);
        if ($body === false) {
            throw new \RuntimeException('http get failed: ' . $url);
        }
        $status = 0;
        /** @var list<string> $http_response_header */
        foreach ($http_response_header ?? [] as $h) {
            if (preg_match('#^HTTP/\S+\s+(\d+)#', $h, $m) === 1) {
                $status = (int) $m[1];
            }
        }
        if ($status !== 200) {
            throw new \RuntimeException(sprintf('http status %d', $status));
        }
        return $body;
    }
}

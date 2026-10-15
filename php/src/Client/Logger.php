<?php

declare(strict_types=1);

namespace RocketMQ\Client;

final class Logger
{
    public const DEBUG = 10;
    public const INFO = 20;
    public const WARNING = 30;
    public const ERROR = 40;

    public const DEFAULT_LOG_DIR = 'logs/rocketmqlogs';
    public const DEFAULT_LOG_FILE = 'rocketmq_php_client.log';

    private static int $level = self::INFO;

    /** @var (\Closure(string): void)|null */
    private static ?\Closure $handler = null;

    private static ?FileSink $sink = null;
    private static bool $sinkResolved = false;

    /** ROCKETMQ_CLIENT_LOG_LEVEL is parsed once on first use (same one-time resolution as the sink). */
    private static bool $levelResolved = false;

    public static function setLevel(int $level): void
    {
        self::$level = $level;
        self::$levelResolved = true;
    }

    /** @param (\Closure(string): void)|null $handler null restores the default (file + stderr) */
    public static function setHandler(?\Closure $handler): void
    {
        self::$handler = $handler;
    }

    /** Absolute path of the active log file; null when file logging is disabled (diagnostics/tests). */
    public static function logFilePath(): ?string
    {
        $sink = self::sink();
        return $sink === null ? null : $sink->path;
    }

    /** Close the file handle (call before process exit; rotation and reopening do not depend on it). */
    public static function close(): void
    {
        if (self::$sink !== null) {
            self::$sink->close();
        }
    }

    /**
     * Effective level: ROCKETMQ_CLIENT_LOG_LEVEL takes precedence; if missing or the name is
     * unrecognized, fall back to INFO. TRACE maps to the most verbose tier (DEBUG is the
     * finest tier in this port). If setLevel() was called explicitly, the caller wins.
     */
    private static function resolvedLevel(): int
    {
        if (self::$levelResolved) {
            return self::$level;
        }
        self::$levelResolved = true;
        $raw = strtoupper(trim((string)getenv('ROCKETMQ_CLIENT_LOG_LEVEL')));
        $map = [
            'TRACE' => self::DEBUG,
            'DEBUG' => self::DEBUG,
            'INFO' => self::INFO,
            'WARN' => self::WARNING,
            'WARNING' => self::WARNING,
            'ERROR' => self::ERROR,
        ];
        if ($raw !== '') {
            self::$level = $map[$raw] ?? self::INFO;
        }
        return self::$level;
    }

    public static function debug(string $message): void
    {
        self::log(self::DEBUG, 'DEBUG', $message);
    }

    public static function info(string $message): void
    {
        self::log(self::INFO, 'INFO', $message);
    }

    public static function warning(string $message): void
    {
        self::log(self::WARNING, 'WARNING', $message);
    }

    public static function error(string $message): void
    {
        self::log(self::ERROR, 'ERROR', $message);
    }

    private static function log(int $level, string $name, string $message): void
    {
        if ($level < self::resolvedLevel()) {
            return;
        }
        $line = sprintf("[%s] %s: %s\n", date('Y-m-d H:i:s'), $name, $message);
        if (self::$handler !== null) {
            (self::$handler)($line);
            return;
        }
        $sink = self::sink();
        if ($sink !== null) {
            $sink->write($line);
            return;
        }
        fwrite(STDERR, $line);
    }

    /** Resolve the sink target once (module-level sink, resolved only once per process). */
    private static function sink(): ?FileSink
    {
        if (self::$sinkResolved) {
            return self::$sink;
        }
        self::$sinkResolved = true;
        if (getenv('ROCKETMQ_CLIENT_LOG_USE_STDOUT') !== false
            && getenv('ROCKETMQ_CLIENT_LOG_USE_STDOUT') !== '') {
            return self::$sink = null;
        }
        $file = getenv('ROCKETMQ_CLIENT_LOG_FILE');
        if ($file === false || $file === '') {
            $file = self::DEFAULT_LOG_FILE;
        }
        $upper = strtoupper($file);
        if ($upper === 'OFF' || $upper === 'NONE') {
            return self::$sink = null;
        }
        // A value containing a path separator is treated as a full path, otherwise it is
        // appended to the log directory
        if (!str_starts_with($file, '/') && !str_contains($file, DIRECTORY_SEPARATOR)) {
            $dir = getenv('ROCKETMQ_CLIENT_LOG_DIR');
            if ($dir === false || $dir === '') {
                $dir = self::defaultLogDir();
            }
            $file = rtrim($dir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $file;
        }
        return self::$sink = new FileSink($file);
    }

    /**
     * Default log directory = <cwd>/logs/rocketmqlogs, deliberately avoiding the user HOME
     * (see the trade-off note under the same name in the class comment).
     * If cwd is unavailable (deleted directory), fall back to the system temp dir, not HOME.
     */
    public static function defaultLogDir(): string
    {
        $cwd = getcwd();
        if ($cwd === false || $cwd === '') {
            $cwd = sys_get_temp_dir();
        }
        return rtrim($cwd, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . self::DEFAULT_LOG_DIR;
    }
}

/**
 * Size-rotating file sink. Write failures degrade (once) and never affect the client main flow.
 */
final class FileSink
{
    public string $path;
    /** @var resource|null */
    private $fh = null;
    private int $size = 0;
    private bool $failed = false;
    private int $maxSize;
    private int $maxIndex;

    public function __construct(string $path)
    {
        $this->path = $path;
        $this->maxSize = self::envInt('ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE', 64 * 1024 * 1024);
        $this->maxIndex = self::envInt('ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX', 10);
    }

    private static function envInt(string $name, int $default): int
    {
        $raw = getenv($name);
        if ($raw === false || $raw === '') {
            return $default;
        }
        $v = (int)$raw;
        return $v > 0 ? $v : $default;
    }

    private function open(): bool
    {
        if ($this->failed) {
            return false;
        }
        if (is_resource($this->fh)) {
            return true;
        }
        $dir = dirname($this->path);
        if (!is_dir($dir) && !@mkdir($dir, 0o755, true) && !is_dir($dir)) {
            $this->failed = true;
            fwrite(STDERR, "failed to open rocketmq client log dir: $dir\n");
            return false;
        }
        $this->fh = @fopen($this->path, 'ab');
        if ($this->fh === false) {
            $this->failed = true;
            fwrite(STDERR, "failed to open rocketmq client log file: {$this->path}\n");
            return false;
        }
        clearstatcache(true, $this->path);
        $this->size = (int)@filesize($this->path);
        return true;
    }

    /** Shift the backup window left: delete .N, .N-1->.N ... .1->.2, current->.1, then reopen a new file. */
    private function roll(): void
    {
        $this->close();
        $top = "{$this->path}.{$this->maxIndex}";
        if (is_file($top)) {
            @unlink($top);
        }
        for ($i = $this->maxIndex - 1; $i >= 1; $i--) {
            $src = "{$this->path}.$i";
            if (!is_file($src)) {
                continue;
            }
            $dst = "{$this->path}." . ($i + 1);
            if (is_file($dst)) {
                @unlink($dst);
            }
            @rename($src, $dst);
        }
        if (is_file($this->path)) {
            @rename($this->path, "{$this->path}.1");
        }
        $this->size = 0;
    }

    public function write(string $line): void
    {
        if (!$this->open()) {
            return;
        }
        $bytes = strlen($line);
        if ($this->maxSize > 0 && $this->maxIndex > 0 && $this->size + $bytes > $this->maxSize) {
            $this->roll();
            if (!$this->open()) {
                return;
            }
        }
        if (@fwrite($this->fh, $line) === false) {
            $this->failed = true;
            $this->close();
            return;
        }
        $this->size += $bytes;
    }

    public function close(): void
    {
        if (is_resource($this->fh)) {
            @fclose($this->fh);
        }
        $this->fh = null;
    }
}

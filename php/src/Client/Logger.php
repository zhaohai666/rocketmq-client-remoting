<?php

declare(strict_types=1);

namespace RocketMQ\Client;

/**
 * 轻量客户端日志（对应 Python python/rocketmq_logging.py、Node
 * nodeJs/src/logging.ts 与 Go go/common/logging.go）。
 *
 * **默认落盘**（与 Java 客户端一样会生成日志文件），但落盘位置刻意不同：
 *
 *   ROCKETMQ_CLIENT_LOG_LEVEL        DEBUG|INFO|WARN|ERROR（默认 INFO）
 *   ROCKETMQ_CLIENT_LOG_DIR          日志目录（默认 <当前工作目录>/logs/rocketmqlogs）
 *   ROCKETMQ_CLIENT_LOG_FILE         文件名（默认 rocketmq_php_client.log；
 *                                    置空 / OFF / NONE 关闭文件落盘；含路径分隔符时按整路径处理）
 *   ROCKETMQ_CLIENT_LOG_USE_STDOUT   任意非空值 → 只写 stderr，不写文件
 *   ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE   单文件上限，默认 64MB（Java logback 同值）
 *   ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX  备份份数，默认 10
 *
 * **为什么不跟 Java 一样写 $HOME/logs/rocketmqlogs**：PHP 客户端常在别人的宿主进程/CLI
 * 脚本里跑，在用户 HOME 下悄悄建目录、写文件是**越界**副作用；落到当前工作目录则跟着部署走，
 * 容器里天然可预期（与 python/rocketmq_logging.py 同一取舍）。需要 Java 口径时显式设
 * ROCKETMQ_CLIENT_LOG_DIR=$HOME/logs/rocketmqlogs 即可。
 *
 * 轮转是**按大小**的固定备份窗（同 csharp ClientLog.RollLocked / nodeJs roll）：
 * 删除 .N，.N-1→.N … .1→.2，当前文件→.1，再开新文件。
 *
 * **文件名为什么不叫 Java 的 rocketmq_client.log**：同机同时跑 Java 客户端时两个进程
 * 会往同一个文件里插行、并把对方的轮转结果改名（PHP 与 Rust 是按天/按大小重命名，JVM 仍
 * 持有旧 fd，之后 Java 日志会静默写进已 unlink 的 inode）。需要强制对齐时设
 * ROCKETMQ_CLIENT_LOG_FILE=rocketmq_client.log 即可。
 *
 * 静态可注入 callable 覆盖输出目标（单测用），默认「文件 + stderr 之外的注入」见 log()。
 */
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

    /** ROCKETMQ_CLIENT_LOG_LEVEL 只在首次用到时解析一次（同 sink 的一次性解析）。 */
    private static bool $levelResolved = false;

    public static function setLevel(int $level): void
    {
        self::$level = $level;
        self::$levelResolved = true;
    }

    /** @param (\Closure(string): void)|null $handler null 恢复默认（文件 + stderr） */
    public static function setHandler(?\Closure $handler): void
    {
        self::$handler = $handler;
    }

    /** 当前生效的日志文件绝对路径；未启用文件落盘时返回 null（诊断/用例用）。 */
    public static function logFilePath(): ?string
    {
        $sink = self::sink();
        return $sink === null ? null : $sink->path;
    }

    /** 关闭文件句柄（进程退出前调用；轮转与重开不依赖它）。 */
    public static function close(): void
    {
        if (self::$sink !== null) {
            self::$sink->close();
        }
    }

    /**
     * 生效级别：ROCKETMQ_CLIENT_LOG_LEVEL 优先，取不到或名字不认识回 INFO。
     * TRACE 与 Go/nodeJs 同归到最详细档（本端口最细档是 DEBUG）。
     * setLevel() 显式调用过则以调用方为准。
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

    /** 解析一次落盘目标（同 Go 的 sync.Once / nodeJs 的模块级 sink）。 */
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
        // 含路径分隔符的取值按完整路径处理，否则拼到日志目录下（与 Go/nodeJs 同）
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
     * 默认日志目录 = <当前工作目录>/logs/rocketmqlogs，刻意避开用户 HOME
     * （见类注释与 python/rocketmq_logging.py 的同名取舍）。
     * cwd 取不到（已删除的目录）时退回系统临时目录，而不是 HOME。
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
 * 按大小轮转的文件落盘点。写失败只降级（一次），绝不影响客户端主流程 ——
 * 与 nodeJs FileSink / csharp ClientLog 同一取舍。
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

    /** 备份窗左移：删 .N，.N-1→.N … .1→.2，当前→.1，然后重开新文件。 */
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

<?php

declare(strict_types=1);

namespace RocketMQ\Client;

/**
 * 轻量日志（对应 Python python/rocketmq_logging.py 的 logging 用法，见 php/PORTING.md）。
 *
 * 静态可注入 callable，默认写 stderr，级别 INFO 对齐 Python logging 默认。
 */
final class Logger
{
    public const DEBUG = 10;
    public const INFO = 20;
    public const WARNING = 30;
    public const ERROR = 40;

    private static int $level = self::INFO;

    /** @var (\Closure(string): void)|null */
    private static ?\Closure $handler = null;

    public static function setLevel(int $level): void
    {
        self::$level = $level;
    }

    /** @param (\Closure(string): void)|null $handler null 恢复默认 stderr */
    public static function setHandler(?\Closure $handler): void
    {
        self::$handler = $handler;
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
        if ($level < self::$level) {
            return;
        }
        $line = sprintf("[%s] %s: %s\n", date('Y-m-d H:i:s'), $name, $message);
        if (self::$handler !== null) {
            (self::$handler)($line);
            return;
        }
        fwrite(STDERR, $line);
    }
}

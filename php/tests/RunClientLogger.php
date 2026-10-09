<?php

declare(strict_types=1);

/**
 * 客户端日志落盘自测（对应 Java 客户端会生成 rocketmq_client.log 的能力）。
 *
 * 运行：php tests/RunClientLogger.php
 *
 * **本端口默认目录是 <cwd>/logs/rocketmqlogs，刻意不写 $HOME**（见 src/Client/Logger.php
 * 类注释）；第 1 组用例因此把「临时 HOME」和「临时 cwd」分开准备，并断言 HOME 下什么都没生成。
 *
 * 为什么要用子进程：sink 与 Go 的 sync.Once、nodeJs 的模块级 sink 一样**每进程只解析
 * 一次**，而环境变量口径（LOG_FILE / LOG_DIR / USE_STDOUT / MAX_SIZE）正是被测行为本身，
 * 同进程里改 putenv 不会影响已解析的 sink。
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Logger;

$checks = 0;
$failures = [];
/** @var list<string> 各用例自建的临时目录，末尾统一回收 */
$tempDirs = [];

function assertTrue(bool $cond, string $what): void
{
    global $checks, $failures;
    $checks++;
    if (!$cond) {
        $failures[] = $what;
        echo "FAIL $what\n";
    } else {
        echo "PASS $what\n";
    }
}

/**
 * 在「临时 cwd + 临时 HOME + 干净环境变量」下跑一段 PHP。
 *
 * @return array{0:int,1:string,2:?string,3:string,4:string,5:string} [rc, 输出, 默认日志文件内容或 null, cwd, 期望文件路径, 临时 HOME]
 */
function runInTempCwd(string $code, array $env, string $tag): array
{
    $cwd = sys_get_temp_dir() . '/rmq_php_log_' . $tag . '_cwd_' . getmypid();
    $home = sys_get_temp_dir() . '/rmq_php_log_' . $tag . '_home_' . getmypid();
    @mkdir($cwd, 0o755, true);
    @mkdir($home, 0o755, true);
    $GLOBALS['tempDirs'][] = $cwd;
    $GLOBALS['tempDirs'][] = $home;
    // 外层先归零本端口认得的全部开关：run_all.php 以及调用者的环境都可能带值，
    // 而“默认落盘路径/级别”正是被测行为。
    // 用 env(1) 而不是 `VAR=x php ...`：赋值前缀只对其紧跟的那条命令生效，而这里
    // php 前面还有个 cd（切 cwd 才能测“默认目录跟着部署走”）。
    $cmd = 'env HOME=' . escapeshellarg($home);
    $always = ['ROCKETMQ_CLIENT_LOG_DIR', 'ROCKETMQ_CLIENT_LOG_FILE', 'ROCKETMQ_CLIENT_LOG_LEVEL',
        'ROCKETMQ_CLIENT_LOG_USE_STDOUT', 'ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE',
        'ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX'];
    foreach ($always as $k) {
        $cmd .= ' ' . $k . '=' . escapeshellarg((string)($env[$k] ?? ''));
    }
    $file = $cwd . '/logs/rocketmqlogs/' . ($env['ROCKETMQ_CLIENT_LOG_FILE'] ?? Logger::DEFAULT_LOG_FILE);
    // 带路径分隔符的 LOG_FILE 按完整路径处理，被测用例都用默认目录
    $cmd .= ' sh -c ' . escapeshellarg('cd ' . escapeshellarg($cwd) . ' && '
        . escapeshellarg(PHP_BINARY) . ' ' . escapeshellarg(__DIR__ . '/_logger_child.php')
        . ' "$1"') . ' sh ' . escapeshellarg($code);
    // stderr 一并收集：USE_STDOUT 用例要断言日志确实走 stderr
    exec($cmd . ' 2>&1', $out, $rc);
    $contents = is_file($file) ? (string)file_get_contents($file) : null;
    return [$rc, implode("\n", $out), $contents, $cwd, $file, $home];
}

/** 递归删掉一个目录（只用于本用例自建的临时目录）。 */
function rmTree(string $dir): void
{
    if (!is_dir($dir)) {
        return;
    }
    foreach ((array)scandir($dir) as $name) {
        if ($name === '.' || $name === '..') {
            continue;
        }
        $p = $dir . DIRECTORY_SEPARATOR . $name;
        is_dir($p) ? rmTree($p) : @unlink($p);
    }
    @rmdir($dir);
}

// ---------------------------------------------------------------------------
// 1) 默认：不设置任何环境变量也必须落盘（对齐 Java）
// ---------------------------------------------------------------------------
[$rc, $stdout, $logBody, $cwd, $logPath, $home] = runInTempCwd(
    "Logger::info('hello-default'); Logger::warning('hello-warn');",
    [],
    'default'
);
assertTrue($rc === 0, '默认配置子进程正常退出');
assertTrue($logBody !== null && str_contains($logBody, 'hello-default'),
    '默认写到 <cwd>/logs/rocketmqlogs/rocketmq_php_client.log');
assertTrue($logBody !== null && str_contains($logBody, 'hello-warn'), 'WARN 级别同样落盘');
assertTrue(!str_contains($stdout, 'hello-default'),
    '默认落盘时不再重复输出到 stderr');
// 用户要求：php 客户端不得在 $HOME 下写日志（Java 会，本端口刻意不同）
assertTrue(!is_dir($home . '/logs'), '默认落盘不碰 $HOME');

// ---------------------------------------------------------------------------
// 2) 级别过滤：默认 INFO 丢弃 DEBUG
// ---------------------------------------------------------------------------
[, , $logBody] = runInTempCwd("Logger::debug('dbg-only');", [], 'level');
// 一条都不写时 sink 从不打开文件，因此「没有文件」与「文件里没有 DEBUG」都算通过
assertTrue($logBody === null || !str_contains($logBody, 'dbg-only'), '默认 INFO 不写 DEBUG');
[, , $logBody] = runInTempCwd(
    "Logger::debug('dbg-on');",
    ['ROCKETMQ_CLIENT_LOG_LEVEL' => 'DEBUG'],
    'debuglevel'
);
assertTrue($logBody !== null && str_contains($logBody, 'dbg-on'), 'ROCKETMQ_CLIENT_LOG_LEVEL=DEBUG 生效');

// ---------------------------------------------------------------------------
// 3) USE_STDOUT / LOG_FILE=OFF 两个关闭口径
// ---------------------------------------------------------------------------
[, $stdout, $logBody] = runInTempCwd(
    "Logger::info('to-stdout');",
    ['ROCKETMQ_CLIENT_LOG_USE_STDOUT' => '1'],
    'stdout'
);
assertTrue($logBody === null, 'ROCKETMQ_CLIENT_LOG_USE_STDOUT 置位时不建日志文件');
assertTrue(str_contains($stdout, 'to-stdout'), 'USE_STDOUT 时输出走 stderr');
[, , $logBody] = runInTempCwd("Logger::info('off');", ['ROCKETMQ_CLIENT_LOG_FILE' => 'OFF'], 'off');
assertTrue($logBody === null, 'ROCKETMQ_CLIENT_LOG_FILE=OFF 关闭文件落盘');

// ---------------------------------------------------------------------------
// 4) 自定义目录与文件名（含路径分隔符时按完整路径）
// ---------------------------------------------------------------------------
$dir = sys_get_temp_dir() . '/rmq_php_log_custom_' . getmypid();
[, , $logBody] = runInTempCwd(
    "Logger::info('custom-dir');",
    ['ROCKETMQ_CLIENT_LOG_DIR' => $dir, 'ROCKETMQ_CLIENT_LOG_FILE' => 'mine.log'],
    'customdir'
);
assertTrue(is_file($dir . '/mine.log') && str_contains((string)file_get_contents($dir . '/mine.log'), 'custom-dir'),
    'ROCKETMQ_CLIENT_LOG_DIR + 自定义文件名生效');
rmTree($dir);

$full = sys_get_temp_dir() . '/rmq_php_log_fullpath_' . getmypid() . '/deep.log';
runInTempCwd(
    "Logger::info('full-path');",
    ['ROCKETMQ_CLIENT_LOG_FILE' => $full],
    'fullpath'
);
assertTrue(is_file($full) && str_contains((string)file_get_contents($full), 'full-path'),
    'LOG_FILE 含分隔符时按完整路径处理');
rmTree(dirname($full));

// ---------------------------------------------------------------------------
// 5) 按大小轮转：备份窗左移，最老的 .N 被删
// ---------------------------------------------------------------------------
$rot = sys_get_temp_dir() . '/rmq_php_log_rot_' . getmypid();
$code = "Logger::info(str_repeat('x', 90)); for (\$i = 0; \$i < 40; \$i++) { Logger::info('row-' . \$i); }";
runInTempCwd(
    $code,
    ['ROCKETMQ_CLIENT_LOG_DIR' => $rot, 'ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE' => '500',
        'ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX' => '3'],
    'rotate'
);
assertTrue(is_file("$rot/rocketmq_php_client.log"), '轮转后当前日志文件仍在写');
$backups = array_filter([$rot . '/rocketmq_php_client.log.1', $rot . '/rocketmq_php_client.log.2',
    $rot . '/rocketmq_php_client.log.3', $rot . '/rocketmq_php_client.log.4'], 'is_file');
assertTrue(count($backups) >= 1, '超过大小上限后备份文件产生');
assertTrue(!is_file($rot . '/rocketmq_php_client.log.4'),
    '备份数不超过 ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX');
rmTree($rot);

// ---------------------------------------------------------------------------
// 6) 同进程：注入 handler 覆盖默认目标（单测/宿主接管日志时用）
// ---------------------------------------------------------------------------
$seen = [];
Logger::setHandler(static function (string $line) use (&$seen): void {
    $seen[] = $line;
});
Logger::error('injected');
Logger::debug('below-level');
assertTrue(count($seen) === 1 && str_contains($seen[0], 'injected'),
    'setHandler 接管输出且级别过滤仍生效');
Logger::setHandler(null);

// 清理各用例自建的临时 cwd / HOME
foreach ($tempDirs as $d) {
    rmTree($d);
}

echo str_repeat('-', 60) . "\n";
if ($failures) {
    echo 'FAILED (' . count($failures) . '/' . $checks . ")\n";
    foreach ($failures as $f) {
        echo "  - $f\n";
    }
    exit(1);
}
echo "ALL TESTS PASSED ($checks checks)\n";

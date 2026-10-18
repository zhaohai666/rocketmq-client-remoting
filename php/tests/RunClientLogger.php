<?php

declare(strict_types=1);

/**
 * Client log persistence self-test (rocketmq_client.log generation capability).
 *
 * Run: php tests/RunClientLogger.php
 *
 * **This port's default directory is <cwd>/logs/rocketmqlogs and deliberately never
 * writes to $HOME** (see the src/Client/Logger.php class docblock); group 1 therefore
 * prepares a "temp HOME" and a "temp cwd" separately and asserts that nothing was
 * created under HOME.
 *
 * Why a child process: the sink is a module-level singleton, resolved **once per
 * process**, and the env-var contract (LOG_FILE / LOG_DIR / USE_STDOUT / MAX_SIZE)
 * is exactly the behavior under test; putenv within the same process does not affect
 * an already-resolved sink.
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Logger;

$checks = 0;
$failures = [];
/** @var list<string> temp dirs created by individual cases, reclaimed together at the end */
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
 * Run a snippet of PHP under "temp cwd + temp HOME + clean env vars".
 *
 * @return array{0:int,1:string,2:?string,3:string,4:string,5:string} [rc, output, default log file contents or null, cwd, expected file path, temp HOME]
 */
function runInTempCwd(string $code, array $env, string $tag): array
{
    $cwd = sys_get_temp_dir() . '/rmq_php_log_' . $tag . '_cwd_' . getmypid();
    $home = sys_get_temp_dir() . '/rmq_php_log_' . $tag . '_home_' . getmypid();
    @mkdir($cwd, 0o755, true);
    @mkdir($home, 0o755, true);
    $GLOBALS['tempDirs'][] = $cwd;
    $GLOBALS['tempDirs'][] = $home;
    // Zero out every switch this port knows first: run_all.php and the caller's
    // environment may both carry values, and the "default persistence path/level"
    // is exactly the behavior under test.
    // proc_open with an argv array + explicit env + cwd instead of `env A=1 sh -c 'cd ... &&
    // php ...'`: the shell form only exists on POSIX, and on Windows it made every case
    // silently degrade into "no file written". Switching cwd is how "the default directory
    // follows the deployment" gets tested.
    $always = ['ROCKETMQ_CLIENT_LOG_DIR', 'ROCKETMQ_CLIENT_LOG_FILE', 'ROCKETMQ_CLIENT_LOG_LEVEL',
        'ROCKETMQ_CLIENT_LOG_USE_STDOUT', 'ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE',
        'ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX'];
    $childEnv = (array) getenv();
    $childEnv['HOME'] = $home;
    $childEnv['USERPROFILE'] = $home;
    foreach ($always as $k) {
        $childEnv[$k] = (string)($env[$k] ?? '');
    }
    $file = $cwd . '/logs/rocketmqlogs/' . ($env['ROCKETMQ_CLIENT_LOG_FILE'] ?? Logger::DEFAULT_LOG_FILE);
    $pipes = [];
    $proc = proc_open(
        [PHP_BINARY, __DIR__ . '/_logger_child.php', $code],
        [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
        $pipes,
        $cwd,
        $childEnv
    );
    if (!is_resource($proc)) {
        return [1, 'failed to spawn logger child', null, $cwd, $file, $home];
    }
    fclose($pipes[0]);
    // stderr collected too: the USE_STDOUT case must assert that logs actually go to stderr
    $out = (string)stream_get_contents($pipes[1]) . (string)stream_get_contents($pipes[2]);
    foreach ([1, 2] as $i) {
        fclose($pipes[$i]);
    }
    $rc = proc_close($proc);
    $contents = is_file($file) ? (string)file_get_contents($file) : null;
    return [$rc, $out, $contents, $cwd, $file, $home];
}

/** Recursively delete a directory (only used for temp dirs created by this test). */
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
// 1) Defaults: must persist even with no env vars set
// ---------------------------------------------------------------------------
[$rc, $stdout, $logBody, $cwd, $logPath, $home] = runInTempCwd(
    "Logger::info('hello-default'); Logger::warning('hello-warn');",
    [],
    'default'
);
assertTrue($rc === 0, 'default-config child exits normally');
assertTrue($logBody !== null && str_contains($logBody, 'hello-default'),
    'defaults write to <cwd>/logs/rocketmqlogs/rocketmq_php_client.log');
assertTrue($logBody !== null && str_contains($logBody, 'hello-warn'), 'WARN level also persists');
assertTrue(!str_contains($stdout, 'hello-default'),
    'default persistence does not duplicate output to stderr');
// Contract: the client must not write logs under $HOME (default directory is fixed to <cwd>/logs/rocketmqlogs)
assertTrue(!is_dir($home . '/logs'), 'default persistence never touches $HOME');

// ---------------------------------------------------------------------------
// 2) Level filtering: default INFO drops DEBUG
// ---------------------------------------------------------------------------
[, , $logBody] = runInTempCwd("Logger::debug('dbg-only');", [], 'level');
// When nothing at all is written the sink never opens a file, so both "no file" and
// "no DEBUG in the file" count as passing
assertTrue($logBody === null || !str_contains($logBody, 'dbg-only'), 'default INFO does not write DEBUG');
[, , $logBody] = runInTempCwd(
    "Logger::debug('dbg-on');",
    ['ROCKETMQ_CLIENT_LOG_LEVEL' => 'DEBUG'],
    'debuglevel'
);
assertTrue($logBody !== null && str_contains($logBody, 'dbg-on'), 'ROCKETMQ_CLIENT_LOG_LEVEL=DEBUG takes effect');

// ---------------------------------------------------------------------------
// 3) The two shutoff switches: USE_STDOUT / LOG_FILE=OFF
// ---------------------------------------------------------------------------
[, $stdout, $logBody] = runInTempCwd(
    "Logger::info('to-stdout');",
    ['ROCKETMQ_CLIENT_LOG_USE_STDOUT' => '1'],
    'stdout'
);
assertTrue($logBody === null, 'no log file created when ROCKETMQ_CLIENT_LOG_USE_STDOUT is set');
assertTrue(str_contains($stdout, 'to-stdout'), 'with USE_STDOUT, output goes to stderr');
[, , $logBody] = runInTempCwd("Logger::info('off');", ['ROCKETMQ_CLIENT_LOG_FILE' => 'OFF'], 'off');
assertTrue($logBody === null, 'ROCKETMQ_CLIENT_LOG_FILE=OFF disables file persistence');

// ---------------------------------------------------------------------------
// 4) Custom directory and file name (a value with a path separator is treated as a full path)
// ---------------------------------------------------------------------------
$dir = sys_get_temp_dir() . '/rmq_php_log_custom_' . getmypid();
[, , $logBody] = runInTempCwd(
    "Logger::info('custom-dir');",
    ['ROCKETMQ_CLIENT_LOG_DIR' => $dir, 'ROCKETMQ_CLIENT_LOG_FILE' => 'mine.log'],
    'customdir'
);
assertTrue(is_file($dir . '/mine.log') && str_contains((string)file_get_contents($dir . '/mine.log'), 'custom-dir'),
    'ROCKETMQ_CLIENT_LOG_DIR + custom file name take effect');
rmTree($dir);

$full = sys_get_temp_dir() . '/rmq_php_log_fullpath_' . getmypid() . '/deep.log';
runInTempCwd(
    "Logger::info('full-path');",
    ['ROCKETMQ_CLIENT_LOG_FILE' => $full],
    'fullpath'
);
assertTrue(is_file($full) && str_contains((string)file_get_contents($full), 'full-path'),
    'LOG_FILE with a separator is treated as a full path');
rmTree(dirname($full));

// ---------------------------------------------------------------------------
// 5) Size-based rotation: backup window shifts left, oldest .N is deleted
// ---------------------------------------------------------------------------
$rot = sys_get_temp_dir() . '/rmq_php_log_rot_' . getmypid();
$code = "Logger::info(str_repeat('x', 90)); for (\$i = 0; \$i < 40; \$i++) { Logger::info('row-' . \$i); }";
runInTempCwd(
    $code,
    ['ROCKETMQ_CLIENT_LOG_DIR' => $rot, 'ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE' => '500',
        'ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX' => '3'],
    'rotate'
);
assertTrue(is_file("$rot/rocketmq_php_client.log"), 'current log file still written after rotation');
$backups = array_filter([$rot . '/rocketmq_php_client.log.1', $rot . '/rocketmq_php_client.log.2',
    $rot . '/rocketmq_php_client.log.3', $rot . '/rocketmq_php_client.log.4'], 'is_file');
assertTrue(count($backups) >= 1, 'backup files produced once the size cap is exceeded');
assertTrue(!is_file($rot . '/rocketmq_php_client.log.4'),
    'backup count never exceeds ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX');
rmTree($rot);

// ---------------------------------------------------------------------------
// 6) Same process: injecting a handler overrides the default target (for unit tests / hosts taking over logs)
// ---------------------------------------------------------------------------
$seen = [];
Logger::setHandler(static function (string $line) use (&$seen): void {
    $seen[] = $line;
});
Logger::error('injected');
Logger::debug('below-level');
assertTrue(count($seen) === 1 && str_contains($seen[0], 'injected'),
    'setHandler takes over output and level filtering still applies');
Logger::setHandler(null);

// Clean up the temp cwd / HOME created by each case
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

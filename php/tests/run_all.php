<?php

declare(strict_types=1);

/**
 * PHP 端全部单测入口（无 phpunit 依赖）。
 *
 * 运行：C:/Users/zhaoh/.workbuddy/binaries/php/versions/8.3/php.exe tests/run_all.php
 *
 * 内容：
 *   0) 静态守卫——全量类名冲突扫描（PHP 无模块隔离，同名类必然踩踏）
 *   1) 各子套件顺序执行，任一失败即以非零码退出
 *
 * 真机联调用例不在本入口内，见 scripts/with_cluster.sh + examples/live_*.php。
 */

$suites = [
    'Common 层' => __DIR__ . '/RunCommon.php',
    'Remoting 层' => __DIR__ . '/RunRemoting.php',
    'Client 日志' => __DIR__ . '/RunClientLogger.php',
    'Client 叶子' => __DIR__ . '/RunClientLeaf.php',
    'Client 轨迹' => __DIR__ . '/RunClientTrace.php',
    'Client OpenTracing' => __DIR__ . '/RunClientOpenTracing.php',
    'Client 聚合器' => __DIR__ . '/RunClientAccumulator.php',
    'Client 实例' => __DIR__ . '/RunClientInstance.php',
    'Client 消费者' => __DIR__ . '/RunClientConsumer.php',
];

// 顶层先加载 autoloader，避免各子套件重复注册。
require_once __DIR__ . '/../bootstrap.php';

// 子套件一律把客户端日志钉到临时目录：本端口默认按 cwd 落盘（见 src/Client/Logger.php
// 的取舍说明），从仓库根跑测试就会在仓库里长出 logs/。真机联调不经过本入口，不受影响。
$suiteLogDir = sys_get_temp_dir() . '/rmq_php_suite_logs_' . getmypid();
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . $suiteLogDir);

$failed = 0;
$totalChecks = 0;

// ---------------------------------------------------------------------------
// 0) 类名冲突扫描
// ---------------------------------------------------------------------------
// 背景：PHP 没有命名空间级别的文件隔离，同一个 FQCN 若在两个文件里声明，先被
// autoload 命中的那个胜出，另一个永远拿不到——这是**静默**错误（不报 fatal，
// 只是方法签名在运行期爆炸或语义悄悄跑偏）。曾真实踩过：
//   RocketMQ\Client\TraceContext 同时被 TraceContext.php(W3C traceparent) 与
//   Trace.php(消息轨迹上下文) 声明。
// 所以把扫描做成常驻守卫：任何重复声明直接判失败。
$conflicts = scan_class_conflicts(__DIR__ . '/../src');

printf("=== 静态守卫：类名冲突扫描 (%s) ===\n", count($conflicts) === 0 ? 'CLEAN' : 'CONFLICT');
if ($conflicts !== []) {
    foreach ($conflicts as $fqcn => $files) {
        printf("  CONFLICT %s\n", $fqcn);
        foreach ($files as $f) {
            printf("      - %s\n", str_replace('\\', '/', substr($f, strlen(dirname(__DIR__)) + 1)));
        }
    }
    $failed++;
} else {
    printf("  no duplicate class/interface/trait/enum declarations\n");
}
echo "\n";

foreach ($suites as $name => $file) {
    if (!is_file($file)) {
        fwrite(STDERR, sprintf("[SKIP] %s: 未找到 %s\n", $name, basename($file)));
        continue;
    }
    printf("=== %s (%s) ===\n", $name, basename($file));
    $output = [];
    $code = 0;
    exec(
        escapeshellarg(PHP_BINARY) . ' ' . escapeshellarg($file) . ' 2>&1',
        $output,
        $code
    );
    $text = implode("\n", $output);
    echo $text, "\n";
    if (preg_match('/ALL TESTS PASSED \((\d+) checks\)/', $text, $m)) {
        $totalChecks += (int)$m[1];
    }
    if ($code !== 0) {
        $failed++;
    }
}

echo str_repeat('-', 60), "\n";
if ($failed === 0) {
    printf("ALL SUITES PASSED (%d checks)\n", $totalChecks);
    exit(0);
}
printf("SUITES FAILED: %d\n", $failed);
exit(1);

// ---------------------------------------------------------------------------

/**
 * 扫描 src/ 下所有 PHP 文件，按 token 解析出「已声明的 FQCN -> 文件列表」，
 * 返回出现次数 > 1 的项。
 *
 * 说明：必须用 token_get_all 而不是正则——`RocketMQ\Client\Foo` 这种字面量在
 * use 语句、docblock、字符串里大量出现，正则必然误报。
 *
 * @return array<string, list<string>>
 */
function scan_class_conflicts(string $srcDir): array
{
    $map = [];
    $it = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($srcDir));
    foreach ($it as $file) {
        if (!$file->isFile() || strtolower($file->getExtension()) !== 'php') {
            continue;
        }
        $path = $file->getPathname();
        $code = file_get_contents($path);
        if ($code === false) {
            continue;
        }
        [$namespace, $decls] = parse_declarations($code);
        foreach ($decls as $name) {
            $fqcn = $namespace === '' ? $name : $namespace . '\\' . $name;
            $map[$fqcn][] = $path;
        }
    }

    $out = [];
    foreach ($map as $fqcn => $files) {
        if (count($files) > 1) {
            $out[$fqcn] = $files;
        }
    }
    ksort($out);
    return $out;
}

/**
 * 从一个文件里解析出 namespace 与顶层 class/interface/trait/enum 短名。
 *
 * @return array{0: string, 1: list<string>}
 */
function parse_declarations(string $code): array
{
    $tokens = token_get_all($code);
    $namespace = '';
    $names = [];
    $count = count($tokens);
    $i = 0;

    while ($i < $count) {
        $tok = $tokens[$i];

        if (!is_array($tok)) {
            $i++;
            continue;
        }

        if ($tok[0] === T_NAMESPACE) {
            $namespace = '';
            $i++;
            // 组装 namespace 名（可能带裸名，如 namespace A\B;）
            while ($i < $count) {
                $t = $tokens[$i];
                if ($t === ';' || $t === '{' || $t === '(') {
                    break;
                }
                if (is_array($t) && in_array($t[0], [T_STRING, T_NS_SEPARATOR, T_NAME_QUALIFIED, T_NAME_FULLY_QUALIFIED], true)) {
                    $namespace .= $t[1];
                }
                $i++;
            }
            continue;
        }

        $declKinds = [T_CLASS, T_INTERFACE, T_TRAIT];
        if (defined('T_ENUM')) {
            $declKinds[] = T_ENUM;
        }

        if (in_array($tok[0], $declKinds, true)) {
            // T_CLASS 也可能是 `Foo::class`
            $prev = $i - 1;
            while ($prev >= 0 && is_array($tokens[$prev]) && $tokens[$prev][0] === T_WHITESPACE) {
                $prev--;
            }
            if ($prev >= 0 && (is_array($tokens[$prev]) ? $tokens[$prev][0] !== T_DOUBLE_COLON : $tokens[$prev] !== '::')) {
                $j = $i + 1;
                while ($j < $count) {
                    $t = $tokens[$j];
                    if (is_array($t) && $t[0] === T_WHITESPACE) {
                        $j++;
                        continue;
                    }
                    if (is_array($t) && $t[0] === T_STRING) {
                        $names[] = $t[1];
                    }
                    break;
                }
            }
        }

        $i++;
    }

    return [$namespace, $names];
}

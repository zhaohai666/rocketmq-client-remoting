<?php

declare(strict_types=1);

/**
 * Entry point for all PHP-side unit tests (no phpunit dependency).
 *
 * Run: php tests/run_all.php
 *
 * Contents:
 *   0) Static guard — a full class-name conflict scan (PHP has no module isolation; same-named
 *      classes inevitably stomp on each other)
 *   1) Each sub-suite runs in order; any failure exits with a non-zero code
 *
 * Live-cluster test cases are not part of this entry point; see scripts/with_cluster.sh +
 * examples/live_*.php.
 */

// Every tests/Run*.php is a suite, so the directory — not a hand-kept list that silently falls
// behind — decides what runs before a commit. (Two suites once sat here unlisted for exactly that
// reason.) The `_`-prefixed children are transport helpers, not suites.
$suites = [];
foreach (glob(__DIR__ . '/Run*.php') ?: [] as $file) {
    $suites[basename($file, '.php')] = $file;
}

// Load the autoloader at the top level so the sub-suites do not register it repeatedly.
require_once __DIR__ . '/../bootstrap.php';

// Sub-suites always pin the client log to a temp directory: this port writes logs relative to
// the cwd by default (see the trade-off notes in src/Client/Logger.php), so running tests from
// the repo root would grow logs/ inside the repo. Live-cluster runs do not go through this
// entry point and are unaffected.
$suiteLogDir = sys_get_temp_dir() . '/rmq_php_suite_logs_' . getmypid();
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . $suiteLogDir);

$failed = 0;
$totalChecks = 0;

// ---------------------------------------------------------------------------
// 0) Class-name conflict scan
// ---------------------------------------------------------------------------
// Background: PHP has no namespace-level file isolation. If the same FQCN is declared in two
// files, the one autoloaded first wins and the other never loads — a **silent** error (no fatal
// is reported; method signatures just explode at runtime or the semantics quietly drift). This
// really happened once:
//   RocketMQ\Client\TraceContext was declared by both TraceContext.php (W3C traceparent) and
//   Trace.php (the message-trace context).
// Hence the scan is a permanent guard: any duplicate declaration fails outright.
$conflicts = scan_class_conflicts(__DIR__ . '/../src');

printf("=== static guard: class conflict scan (%s) ===\n", count($conflicts) === 0 ? 'CLEAN' : 'CONFLICT');
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
        fwrite(STDERR, sprintf("[SKIP] %s: %s not found\n", $name, basename($file)));
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
 * Scans every PHP file under src/, parsing out "declared FQCN -> file list" by token,
 * returning the entries that occur more than once.
 *
 * Note: token_get_all must be used instead of a regex — literals like `RocketMQ\Client\Foo`
 * appear extensively in use statements, docblocks and strings; a regex would inevitably
 * false-positive.
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
 * Parses the namespace and top-level class/interface/trait/enum short names from one file.
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
            // Assemble the namespace name (may carry a bare name, e.g. namespace A\B;)
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
            // T_CLASS may also be `Foo::class`
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

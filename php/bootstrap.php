<?php

declare(strict_types=1);

// Self-guard: an entry point that loads this file with a plain `require` twice (or loads it
// through both a manual require and a composer-style path) used to die with
// "Cannot redeclare self_scan_classes()". With this guard the second load is a no-op.
if (defined('ROCKETMQ_CLIENT_BOOTSTRAP_LOADED')) {
    return;
}
define('ROCKETMQ_CLIENT_BOOTSTRAP_LOADED', true);

// Autoloader (composer-free: any entry point can include this file to get all classes).
//
// Directory layout: Common/*, Client/*, Remoting/Protocol/*.
// Note: the Header/Body classes under Remoting/Protocol are grouped by "domain" into a few
// files (Headers.php, Bodies.php, etc.), so one file holds multiple classes and PSR-4
// cannot derive the path -> on a miss we fall back to a full classmap scan
// (token_get_all scan of src/ once on first miss, cached per process).
spl_autoload_register(static function (string $class): void {
    static $classMap = null;

    $prefix = 'RocketMQ\\';
    if (!str_starts_with($class, $prefix)) {
        return;
    }
    $rel = str_replace('\\', '/', substr($class, strlen($prefix)));

    // 1) Direct PSR-4 hit (the regular one-class-per-file case, zero extra overhead).
    // require_once: bootstrap.php itself may be loaded more than once by an entry point
    // (require in, then autoloaded through a nested include), and a double declaration of
    // any class is a fatal error in PHP. require_once is a no-op after the first load.
    $direct = __DIR__ . '/src/' . $rel . '.php';
    if (is_file($direct)) {
        require_once $direct;
        return;
    }

    // 2) Full classmap fallback (multi-class domain files).
    if ($classMap === null) {
        $classMap = self_scan_classes(__DIR__ . '/src');
    }
    if (isset($classMap[$class]) && is_file($classMap[$class])) {
        require_once $classMap[$class];
    }
});

/**
 * Scan all .php files under a directory and build an FQCN -> file path map.
 *
 * Wrapped in function_exists: PHP registers unconditional top-level function
 * declarations at COMPILE time of the file, so even with the define-guard above
 * a second `require` of this file would still fatal with "Cannot redeclare".
 * A conditional declaration inside function_exists is only registered when the
 * branch executes at runtime, which makes the guard actually effective.
 *
 * @return array<string,string>
 */
if (!function_exists('self_scan_classes')) {
    function self_scan_classes(string $dir): array
    {
        if (!is_dir($dir)) {
            return [];
        }
        $map = [];
        $it = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS)
        );
        /** @var SplFileInfo $file */
        foreach ($it as $file) {
            if ($file->getExtension() !== 'php') {
                continue;
            }
            $path = $file->getPathname();
            $tokens = @token_get_all((string)file_get_contents($path));
            $ns = '';
            $n = count($tokens);
            for ($i = 0; $i < $n; $i++) {
                $t = $tokens[$i];
                if (!is_array($t)) {
                    continue;
                }
                if ($t[0] === T_NAMESPACE) {
                    $ns = '';
                    for ($j = $i + 1; $j < $n; $j++) {
                        $tt = $tokens[$j];
                        if ($tt === ';' || $tt === '{') {
                            break;
                        }
                        if (is_array($tt) && in_array($tt[0], [T_STRING, T_NS_SEPARATOR, T_NAME_QUALIFIED], true)) {
                            $ns .= $tt[1];
                        }
                    }
                    continue;
                }
                $isDecl = $t[0] === T_CLASS || $t[0] === T_INTERFACE || $t[0] === T_TRAIT
                    || $t[0] === T_ENUM;
                if (!$isDecl) {
                    continue;
                }
                // Skip Foo::class and anonymous classes (new class).
                $prev = $tokens[$i - 1] ?? null;
                if (is_array($prev) && $prev[0] === T_DOUBLE_COLON) {
                    continue;
                }
                for ($j = $i + 1; $j < $n; $j++) {
                    $tt = $tokens[$j];
                    if (is_array($tt) && $tt[0] === T_STRING) {
                        $fqcn = $ns !== '' ? $ns . '\\' . $tt[1] : $tt[1];
                        if (!isset($map[$fqcn])) {
                            $map[$fqcn] = $path;
                        }
                        break;
                    }
                    if ($tt === '{' || $tt === '(') {
                        break; // anonymous class / no name
                    }
                }
            }
        }
        return $map;
    }
}

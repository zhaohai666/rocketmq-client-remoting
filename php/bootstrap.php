<?php

declare(strict_types=1);

// 自动加载（不依赖 composer：任何入口 include 本文件即可用全部类）。
//
// 目录布局与 Python 端 1:1 对应：Common/*、Client/*、Remoting/Protocol/*。
// 注意：Remoting/Protocol 下的 Header/Body 类按「域」聚合在少数文件里（Headers.php、
// Bodies.php 等），一个文件包含多个类，PSR-4 推导不出 → 命中失败后回退到全量 classmap
// （首次 miss 时用 token_get_all 扫描 src/ 一次并缓存于进程内）。
spl_autoload_register(static function (string $class): void {
    static $classMap = null;

    $prefix = 'RocketMQ\\';
    if (!str_starts_with($class, $prefix)) {
        return;
    }
    $rel = str_replace('\\', '/', substr($class, strlen($prefix)));

    // 1) PSR-4 直接命中（一个文件一个类的常规情形，零额外开销）。
    $direct = __DIR__ . '/src/' . $rel . '.php';
    if (is_file($direct)) {
        require $direct;
        return;
    }

    // 2) 全量 classmap 回退（多类域文件）。
    if ($classMap === null) {
        $classMap = self_scan_classes(__DIR__ . '/src');
    }
    if (isset($classMap[$class]) && is_file($classMap[$class])) {
        require $classMap[$class];
    }
});

/**
 * 扫描目录下所有 .php，解析出 FQCN → 文件路径 的映射。
 *
 * @return array<string,string>
 */
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
            // 跳过 Foo::class 与匿名类（new class）。
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
                    break; // 匿名类 / 无名称
                }
            }
        }
    }
    return $map;
}

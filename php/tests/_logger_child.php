<?php

declare(strict_types=1);

/**
 * RunClientLogger.php 的子进程载体：在干净的环境变量下解析一次 sink，再执行父用例
 * 传入的一小段代码（只用于本目录的自测，代码来源固定、不接受外部输入）。
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Logger;

$code = $argv[1] ?? '';
if ($code === '') {
    fwrite(STDERR, "usage: _logger_child.php '<php snippet using Logger>'\n");
    exit(2);
}
// eval 的代码块是独立编译单元，文件顶部的 use 对它不生效，
// 所以在其内部再写一次导入。
eval("use RocketMQ\\Client\\Logger;\n" . $code);

<?php

declare(strict_types=1);

/**
 * Child-process carrier for RunClientLogger.php: resolves the sink once in a clean environment,
 * then executes the small snippet of code passed in by the parent test case (for self-testing in
 * this directory only; the code source is fixed and no external input is accepted).
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\Logger;

$code = $argv[1] ?? '';
if ($code === '') {
    fwrite(STDERR, "usage: _logger_child.php '<php snippet using Logger>'\n");
    exit(2);
}
// The eval'd code block is an independent compilation unit; the use statements at the top of
// the file do not apply to it, so the imports are repeated inside.
eval("use RocketMQ\\Client\\Logger;\n" . $code);

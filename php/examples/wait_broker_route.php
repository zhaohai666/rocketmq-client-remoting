<?php

declare(strict_types=1);

// 集群就绪探测：端口通了 ≠ broker 已把路由注册进 nameserver（broker 启动后
// 才注册，实测可达 30s+）。此前的真机脚本只等端口，跑下去全部死于
// "No route info of default topic TBW102"——那是时序问题，不是客户端 bug。
//
//   php examples/wait_broker_route.php [namesrv] [timeoutSec]
//
// 就绪输出 REGISTERED 并 exit 0；超时输出 ROUTE_WAIT_TIMEOUT 并 exit 1。
require __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;

$ns = $argv[1] ?? '127.0.0.1:9876';
$timeoutSec = (int) ($argv[2] ?? 60);

for ($i = 0; $i < $timeoutSec; $i++) {
    try {
        $a = new DefaultMQAdminExt();
        $a->setNamesrvAddr($ns);
        $a->start();
        $ok = false;
        try {
            $ci = $a->fetchBrokerClusterInfo();
            $ok = count($ci->brokerAddrTable) > 0;
        } finally {
            $a->shutdown();
        }
        if ($ok) {
            echo "REGISTERED after {$i}s" . PHP_EOL;
            exit(0);
        }
    } catch (Throwable) {
        // nameserver 未起 / 未注册，继续等
    }
    sleep(1);
}
echo 'ROUTE_WAIT_TIMEOUT' . PHP_EOL;
exit(1);

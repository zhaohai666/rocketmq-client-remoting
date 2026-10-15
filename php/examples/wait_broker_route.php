<?php

declare(strict_types=1);

// Cluster readiness probe: port open does not mean the broker has registered its route into the
// nameserver (registration happens after broker startup; measured up to 30s+). Earlier live
// scripts only waited for the port and then all died with
// "No route info of default topic TBW102" — a timing issue, not a client bug.
//
//   php examples/wait_broker_route.php [namesrv] [timeoutSec]
//
// Ready output REGISTERED and exit 0; timeout output ROUTE_WAIT_TIMEOUT and exit 1.
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
        // nameserver not up / not registered yet, keep waiting
    }
    sleep(1);
}
echo 'ROUTE_WAIT_TIMEOUT' . PHP_EOL;
exit(1);

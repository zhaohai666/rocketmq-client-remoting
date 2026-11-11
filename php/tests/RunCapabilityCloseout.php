<?php

declare(strict_types=1);

/**
 * The capability gaps closed against the Java 5.x reference client (no phpunit, no cluster).
 *
 * Run: php tests/RunCapabilityCloseout.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Each block pins one Java facade to this port and pairs the new accessor with the behavior it
 * steers, so a refactor that keeps the setter but drops the wiring goes red here:
 *
 * 1. ConsumeFromWhere: all six values of the Java enum exist. The three "deprecated" ones are not
 *    dead -- Java folds them into the CONSUME_FROM_LAST_OFFSET branch of the offset-init switch
 *    (RebalancePushImpl#computePullFromWhereWithException lines 171-173).
 * 2. LitePull connectBrokerByUser / defaultBrokerId: the only way to read from a slave
 *    (PullAPIWrapper#recalculatePullFromWhichNode). Off must keep the historic "no broker id on
 *    the request" shape.
 * 3. Admin adminExtGroup: default "admin_ext_group" plus the Java accessor pair.
 */

require_once __DIR__ . '/../bootstrap.php';

// Nothing here may resolve a name-server through the address server.
putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_ADDR');

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Remoting\Protocol\ConsumeFromWhere;

final class RunCapabilityCloseout
{
    private int $passed = 0;

    /** @var list<string> */
    private array $failures = [];

    public function check(bool $cond, string $label): void
    {
        if ($cond) {
            $this->passed++;
            return;
        }
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: {$label}\n");
    }

    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        if ($expected === $actual) {
            $this->passed++;
            return;
        }
        $detail = sprintf(
            "%s\n  expected: %s\n  actual:   %s",
            $label,
            var_export($expected, true),
            var_export($actual, true)
        );
        $this->failures[] = $label;
        fwrite(STDERR, "FAIL: {$detail}\n");
    }

    // ================================================================ ConsumeFromWhere

    public function testConsumeFromWhereHasAllSixJavaValues(): void
    {
        // Authoritative list: Java common/.../consumer/ConsumeFromWhere.java.
        $java = [
            'CONSUME_FROM_LAST_OFFSET',
            'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST',
            'CONSUME_FROM_MIN_OFFSET',
            'CONSUME_FROM_MAX_OFFSET',
            'CONSUME_FROM_FIRST_OFFSET',
            'CONSUME_FROM_TIMESTAMP',
        ];
        $reflection = new ReflectionClass(ConsumeFromWhere::class);
        $constants = $reflection->getConstants();
        foreach ($java as $value) {
            $this->check(
                array_key_exists($value, $constants) && $constants[$value] === $value,
                "ConsumeFromWhere::{$value} exists and equals its wire string"
            );
        }
        $this->checkSame(count($java), count($constants), 'no values beyond the Java enum');
    }

    // ================================================================ LitePull broker pin

    public function testLitePullBrokerPinDefaultsMatchJava(): void
    {
        $c = new DefaultLitePullConsumer('GID_CloseoutPin');
        $this->checkSame(false, $c->isConnectBrokerByUser(), 'the pin defaults to off');
        $this->checkSame(MixAll::MASTER_ID, $c->getDefaultBrokerId(), 'defaultBrokerId defaults to MASTER_ID');
    }

    public function testLitePullBrokerPinResolution(): void
    {
        $c = new DefaultLitePullConsumer('GID_CloseoutPinResolve');
        $mq = new MessageQueue('CloseoutTopic', 'broker-a', 0);
        $resolve = new ReflectionMethod($c, 'liteBrokerIdFor');
        $resolve->setAccessible(true);

        // Off means "no broker id on the request": MQClientInstance::pullMessage then resolves the
        // address from the route, which is what every lite round did before this switch existed.
        $this->checkSame(null, $resolve->invoke($c, $mq), 'pin off puts no broker id on the request');

        $c->setConnectBrokerByUser(true);
        $c->setDefaultBrokerId(1);
        $this->checkSame(true, $c->isConnectBrokerByUser(), 'the pin reports on after being set');
        $this->checkSame(1, $c->getDefaultBrokerId(), 'defaultBrokerId round-trips');
        $this->checkSame(1, $resolve->invoke($c, $mq), 'pin on asks the pinned broker id');

        $c->setConnectBrokerByUser(false);
        $this->checkSame(null, $resolve->invoke($c, $mq), 'unpinning restores the historic request shape');
    }

    // ================================================================ Admin group

    public function testAdminExtGroupMatchesJava(): void
    {
        $this->checkSame('admin_ext_group', DefaultMQAdminExt::DEFAULT_ADMIN_EXT_GROUP, 'the default constant matches Java');
        $admin = new DefaultMQAdminExt();
        $this->checkSame('admin_ext_group', $admin->getAdminExtGroup(), 'facade default');
        $admin->setAdminExtGroup('custom-admin-group');
        $this->checkSame('custom-admin-group', $admin->getAdminExtGroup(), 'set/get round-trip');
    }

    // ================================================================ entry

    public function run(): int
    {
        $this->testConsumeFromWhereHasAllSixJavaValues();
        $this->testLitePullBrokerPinDefaultsMatchJava();
        $this->testLitePullBrokerPinResolution();
        $this->testAdminExtGroupMatchesJava();
        return $this->summary();
    }

    private function summary(): int
    {
        if ($this->failures === []) {
            printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
            return 0;
        }
        printf("TESTS FAILED: %d / %d\n", count($this->failures), $this->passed + count($this->failures));
        return 1;
    }
}

exit((new RunCapabilityCloseout())->run());

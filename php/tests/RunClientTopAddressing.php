<?php

declare(strict_types=1);

/**
 * TopAddressing / nameserver-setting self-test (no phpunit dependency, no real cluster).
 *
 * Run: php tests/RunClientTopAddressing.php (or via tests/run_all.php)
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 *
 * Covers the Java-aligned nameserver configuration surface, end to end:
 *
 * 1. URL construction (MixAll.getWSAddr): the default :8080 is appended only when the domain
 *    carries no port; subgroup defaults to "nsaddr".
 * 2. Subgroup resolution chain: explicit setting > ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP env >
 *    "nsaddr" (Java: system property rocketmq.namesrv.domain.subgroup). A blank explicit value
 *    counts as unset.
 * 3. Domain resolution chain: explicit setting > ROCKETMQ_NAMESRV_DOMAIN env > unconfigured
 *    (wsAddr stays empty -> fetch is a no-op).
 * 4. fetchAndApply: only a *changed* address list is applied and reported.
 * 5. MixAll::namesrvAddrsFromEnv: the Java ClientConfig default chain maps the
 *    rocketmq.namesrv.addr property to ROCKETMQ_NAMESRV_ADDR, tried before NAMESRV_ADDR;
 *    addresses split on ';' / ',' with blank entries dropped.
 * 6. Facade surface: every facade (producer / push / pull / lite pull / admin) exposes
 *    setNameServerDomain / setNameServerSubgroup, normalises blank values to null, and start()
 *    accepts a domain-only configuration (the facade guard must pass; the instance then fails
 *    against an unreachable address server with the "returned none" text).
 */

require_once __DIR__ . '/../bootstrap.php';

putenv('ROCKETMQ_NAMESRV_DOMAIN');
putenv('ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP');
putenv('ROCKETMQ_NAMESRV_ADDR');
putenv('NAMESRV_ADDR');

use RocketMQ\Client\DefaultLitePullConsumer;
use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\DefaultMQProducer;
use RocketMQ\Client\DefaultTopAddressing;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\MixAll;

final class RunClientTopAddressing
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
        printf("  FAIL %s\n", $label);
    }

    /** @param mixed $expected */
    public function checkSame(mixed $expected, mixed $actual, string $label): void
    {
        $ok = $expected === $actual;
        if (!$ok) {
            printf("  FAIL %s\n    expected: %s\n    actual:   %s\n", $label, var_export($expected, true), var_export($actual, true));
        }
        $ok ? $this->passed++ : $this->failures[] = $label;
    }

    public function summary(): int
    {
        if ($this->failures !== []) {
            printf("FAILED (%d of %d checks failed)\n", count($this->failures), $this->passed + count($this->failures));
            return 1;
        }
        printf("ALL TESTS PASSED (%d checks)\n", $this->passed);
        return 0;
    }

    // ------------------------------------------------------------------ URL construction

    private function testUrlConstruction(): void
    {
        $this->checkSame('http://jmenv.tbsite.net:8080/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('jmenv.tbsite.net'), 'getWsAddr default port + default subgroup');
        $this->checkSame('http://host:9999/rocketmq/nsaddr', DefaultTopAddressing::getWsAddr('host:9999'), 'getWsAddr keeps an explicit port');
        $this->checkSame('http://d:8080/rocketmq/sub', DefaultTopAddressing::getWsAddr('d', 'sub'), 'getWsAddr explicit subgroup');
    }

    // ------------------------------------------------------------------ subgroup / domain chains

    private function testSubgroupChain(): void
    {
        // explicit > env > default; blank explicit counts as unset.
        putenv('ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP=envgrp');
        $this->checkSame('envgrp', DefaultTopAddressing::resolveSubgroup(null), 'resolveSubgroup env wins over default');
        $this->checkSame('envgrp', DefaultTopAddressing::resolveSubgroup(''), 'resolveSubgroup blank explicit counts as unset');
        $this->checkSame('explicit', DefaultTopAddressing::resolveSubgroup('explicit'), 'resolveSubgroup explicit wins over env');
        $this->checkSame('explicit', DefaultTopAddressing::resolveSubgroup('  explicit  '), 'resolveSubgroup explicit trimmed');
        putenv('ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP');
        $this->checkSame('nsaddr', DefaultTopAddressing::resolveSubgroup(null), 'resolveSubgroup default when nothing set');

        // The subgroup env flows into the constructed URL.
        putenv('ROCKETMQ_NAMESRV_DOMAIN=envdomain');
        putenv('ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP=envgrp');
        $ta = new DefaultTopAddressing();
        $this->checkSame('http://envdomain:8080/rocketmq/envgrp', $ta->wsAddr, 'constructor: env domain + env subgroup');
        // Explicit subgroup wins over the env; explicit domain wins over the env domain.
        $ta2 = new DefaultTopAddressing(null, '', null, 3000, null, 'explicitgrp');
        $this->checkSame('http://envdomain:8080/rocketmq/explicitgrp', $ta2->wsAddr, 'constructor: explicit subgroup wins over env');
        $ta3 = new DefaultTopAddressing(null, '', null, 3000, 'explicitdom', 'explicitgrp');
        $this->checkSame('http://explicitdom:8080/rocketmq/explicitgrp', $ta3->wsAddr, 'constructor: explicit domain + subgroup win over env');
        putenv('ROCKETMQ_NAMESRV_DOMAIN');
        putenv('ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP');
        $ta4 = new DefaultTopAddressing();
        $this->checkSame('', $ta4->wsAddr, 'constructor: unconfigured -> wsAddr empty (fetch no-op)');
        $this->check(false === DefaultTopAddressing::isConfigured(), 'isConfigured false when unconfigured');
        putenv('ROCKETMQ_NAMESRV_DOMAIN=envdomain');
        $this->check(true === DefaultTopAddressing::isConfigured(), 'isConfigured true when env configured');
        putenv('ROCKETMQ_NAMESRV_DOMAIN');
    }

    // ------------------------------------------------------------------ fetch / apply

    private function testFetchAndApply(): void
    {
        // DefaultTopAddressing::httpGet is the single transport exit point (same seam the
        // leaf suite mocks); override it to answer a scripted sequence.
        $ta = new class('http://mock/rocketmq/nsaddr') extends DefaultTopAddressing {
            /** @var list<string> */
            public array $answers;
            /** @var list<string> */
            public array $seenUrls = [];

            public function __construct(string $wsAddr)
            {
                parent::__construct($wsAddr);
                $this->answers = ['10.0.0.1:9876;10.0.0.2:9876', '10.0.0.1:9876;10.0.0.2:9876', '10.0.0.9:9876'];
            }

            public function httpGet(string $url, float $timeoutSeconds): ?string
            {
                $this->seenUrls[] = $url;
                return array_shift($this->answers);
            }
        };
        $first = $ta->fetchAndApply();
        $this->checkSame('10.0.0.1:9876;10.0.0.2:9876', $first, 'fetchAndApply: first address list is applied');
        $this->checkSame(null, $ta->fetchAndApply(), 'fetchAndApply: unchanged list is not re-applied');
        $this->checkSame('10.0.0.9:9876', $ta->fetchAndApply(), 'fetchAndApply: changed list is applied');
    }

    // ------------------------------------------------------------------ env address chain

    private function testNamesrvAddrsFromEnv(): void
    {
        putenv('ROCKETMQ_NAMESRV_ADDR');
        putenv('NAMESRV_ADDR');
        $this->checkSame([], MixAll::namesrvAddrsFromEnv(), 'namesrvAddrsFromEnv: unset -> []');

        putenv('NAMESRV_ADDR=10.0.0.1:9876');
        $this->checkSame(['10.0.0.1:9876'], MixAll::namesrvAddrsFromEnv(), 'namesrvAddrsFromEnv: NAMESRV_ADDR alone');

        putenv('ROCKETMQ_NAMESRV_ADDR=10.0.0.2:9876');
        $this->checkSame(['10.0.0.2:9876'], MixAll::namesrvAddrsFromEnv(), 'namesrvAddrsFromEnv: ROCKETMQ_NAMESRV_ADDR wins over NAMESRV_ADDR');

        putenv('ROCKETMQ_NAMESRV_ADDR=10.0.0.2:9876 ;10.0.0.3:9876, ,10.0.0.4:9876;');
        $this->checkSame(['10.0.0.2:9876', '10.0.0.3:9876', '10.0.0.4:9876'], MixAll::namesrvAddrsFromEnv(), 'namesrvAddrsFromEnv: splits ;/, and drops blanks');

        putenv('ROCKETMQ_NAMESRV_ADDR=   ');
        $this->checkSame(['10.0.0.1:9876'], MixAll::namesrvAddrsFromEnv(), 'namesrvAddrsFromEnv: blank property env falls through to NAMESRV_ADDR');

        putenv('ROCKETMQ_NAMESRV_ADDR');
        putenv('NAMESRV_ADDR');
    }

    // ------------------------------------------------------------------ facade surface

    private function testFacadeSetters(): void
    {
        $producer = new DefaultMQProducer('gid_top_addr');
        $producer->setNameServerDomain('  dom  ');
        $producer->setNameServerSubgroup('  grp  ');
        $this->checkSame('dom', $producer->nameServerDomain, 'producer: setNameServerDomain trims');
        $this->checkSame('grp', $producer->nameServerSubgroup, 'producer: setNameServerSubgroup trims');
        $producer->setNameServerDomain('   ');
        $this->checkSame(null, $producer->nameServerDomain, 'producer: blank domain normalises to null');
        $producer->setNameServerSubgroup(null);
        $this->checkSame(null, $producer->nameServerSubgroup, 'producer: null subgroup stays null');

        $push = new RocketMQ\Client\DefaultMQPushConsumer('gid_top_addr_push');
        $push->setNameServerDomain('dom');
        $push->setNameServerSubgroup('grp');
        $this->checkSame('dom', $push->nameServerDomain, 'push consumer: domain setter');
        $this->checkSame('grp', $push->nameServerSubgroup, 'push consumer: subgroup setter');

        $pull = new RocketMQ\Client\DefaultMQPullConsumer('gid_top_addr_pull');
        $pull->setNameServerDomain('dom');
        $pull->setNameServerSubgroup('grp');
        $this->checkSame('dom', $pull->nameServerDomain, 'pull consumer: domain setter');
        $this->checkSame('grp', $pull->nameServerSubgroup, 'pull consumer: subgroup setter');

        $lite = new DefaultLitePullConsumer('gid_top_addr_lite');
        $lite->setNameServerDomain('dom');
        $lite->setNameServerSubgroup('grp');
        $this->checkSame('dom', $lite->nameServerDomain, 'lite pull: domain setter');
        $this->checkSame('grp', $lite->nameServerSubgroup, 'lite pull: subgroup setter');

        $admin = new DefaultMQAdminExt();
        $admin->setNameServerDomain('dom');
        $admin->setNameServerSubgroup('grp');
        $this->checkSame('dom', $admin->nameServerDomain, 'admin: domain setter');
        $this->checkSame('grp', $admin->nameServerSubgroup, 'admin: subgroup setter');
    }

    private function testFacadeGuardAcceptsDomainOnly(): void
    {
        // A domain-only configuration must pass the facade guard ("name server address is not
        // set") and reach the instance, which then fails against the unreachable address
        // server with the "returned none" text. 127.0.0.1 with a closed port refuses fast.
        $producer = new DefaultMQProducer('gid_top_addr_guard');
        $producer->setNameServerDomain('127.0.0.1');
        try {
            $producer->start();
            $this->check(false, 'producer start with domain-only must not succeed without an address server');
        } catch (MQClientException $e) {
            $this->check(
                $e->getMessage() !== 'name server address is not set' && str_contains($e->getMessage(), 'returned none'),
                'producer: domain-only passes the facade guard, instance reports the address-server miss'
            );
        } finally {
            if ($producer->mqClient !== null) {
                $producer->mqClient->shutdown();
            }
        }

        $admin = new DefaultMQAdminExt();
        $admin->setNameServerDomain('127.0.0.1');
        try {
            $admin->start();
            $this->check(false, 'admin start with domain-only must not succeed without an address server');
        } catch (MQClientException $e) {
            $this->check(
                $e->getMessage() !== 'name server address is not set' && str_contains($e->getMessage(), 'returned none'),
                'admin: domain-only passes the facade guard, instance reports the address-server miss'
            );
        } finally {
            // Admin::$mqClient is private; reflect it for the shutdown.
            $prop = new ReflectionProperty(DefaultMQAdminExt::class, 'mqClient');
            $prop->setAccessible(true);
            /** @var RocketMQ\Client\MQClientInstance|null $client */
            $client = $prop->getValue($admin);
            if ($client !== null) {
                try {
                    $client->shutdown();
                } catch (\Throwable $e) {
                    // A half-started instance may refuse shutdown; nothing to assert on here.
                }
            }
        }
    }

    public function run(): int
    {
        $this->testUrlConstruction();
        $this->testSubgroupChain();
        $this->testFetchAndApply();
        $this->testNamesrvAddrsFromEnv();
        $this->testFacadeSetters();
        $this->testFacadeGuardAcceptsDomainOnly();
        return $this->summary();
    }
}

exit((new RunClientTopAddressing())->run());

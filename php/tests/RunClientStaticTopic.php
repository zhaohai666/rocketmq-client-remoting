<?php

declare(strict_types=1);

/**
 * 静态主题路由离线单测（对标 Go 端 static_topic_test.go 的五个用例）—— 不需要集群。
 *
 * 覆盖：mock broker 名推导、类型化映射解析缺省值、端点推导（未承载队列 ->
 * "__syslo__none__" 哨兵、scope 不合并、epoch 围栏）、getBrokerNameFromMessageQueue
 * 的重写/透传。
 *
 * 运行：php tests/RunClientStaticTopic.php
 * 全部通过输出 "ALL TESTS PASSED (N checks)"，任一失败列出明细并以非零码退出。
 */

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\MQClientInstance;
use RocketMQ\Client\StaticTopic;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\TopicRouteData;

$GLOBALS['ST_PASS'] = 0;
$GLOBALS['ST_FAIL'] = 0;

function check(bool $cond, string $label): void
{
    if ($cond) {
        ++$GLOBALS['ST_PASS'];
        return;
    }
    ++$GLOBALS['ST_FAIL'];
    printf("FAIL %s\n", $label);
}

function checkSame(mixed $expected, mixed $actual, string $label): void
{
    check($expected === $actual, sprintf('%s (expected %s, got %s)', $label,
        var_export($expected, true), var_export($actual, true)));
}

/** @param array<string, mixed> $currIdMap */
function rawMapping(string $scope, int $totalQueues, string $bname, int $epoch, array $currIdMap): array
{
    return [
        'scope' => $scope,
        'totalQueues' => $totalQueues,
        'bname' => $bname,
        'epoch' => $epoch,
        'dirty' => false,
        'currIdMap' => $currIdMap,
    ];
}

function mappingRoute(array $entries): TopicRouteData
{
    $route = new TopicRouteData();
    $route->topicQueueMappingByBroker = $entries;
    return $route;
}

// 1) mock broker 名推导
checkSame('__syslo__global__', StaticTopic::getMockBrokerName('__global__'), 'mock name strips global underscores');
checkSame('__syslo__zone-a', StaticTopic::getMockBrokerName('zone-a'), 'mock name prefixes other scopes verbatim');
check(StaticTopic::isLogicalQueueMockBrokerName('__syslo__global__'), 'mock prefix detected');
check(!StaticTopic::isLogicalQueueMockBrokerName('broker-a'), 'real broker name is not mock');

// 2) 类型化解析的缺省值
$info = StaticTopic::parseTopicQueueMappingInfo([
    'totalQueues' => 2,
    'bname' => 'b1',
    'epoch' => 3,
]);
check($info !== null, 'parse ok');
if ($info !== null) {
    checkSame('__global__', $info['scope'], 'scope defaults to __global__');
    checkSame(2, $info['totalQueues'], 'totalQueues kept');
    checkSame('b1', $info['bname'], 'bname kept');
    checkSame(3, $info['epoch'], 'epoch kept');
}
check(StaticTopic::parseTopicQueueMappingInfo('not-an-object') === null, 'non-object raw rejected');

// 3) 单 scope + 未承载队列 -> NOT_EXIST 哨兵
$route = mappingRoute([
    'broker-a' => rawMapping('__global__', 4, 'broker-a', 0, ['0' => 0, '1' => 1, '2' => 2]),
]);
$endpoints = StaticTopic::topicRouteData2EndpointsForStaticTopic('StaticTopic', $route);
check($endpoints !== null, 'static route yields endpoints');
if ($endpoints !== null) {
    checkSame(4, count($endpoints), 'hosted + unhosted = 4');
    for ($id = 0; $id < 3; ++$id) {
        $mq = new MessageQueue('StaticTopic', '__syslo__global__', $id);
        checkSame('broker-a', $endpoints[StaticTopic::queueKey($mq)], "global $id hosted");
    }
    $unhosted = new MessageQueue('StaticTopic', '__syslo__global__', 3);
    checkSame('__syslo__none__', $endpoints[StaticTopic::queueKey($unhosted)], 'unhosted -> NOT_EXIST sentinel');
}
check(StaticTopic::topicRouteData2EndpointsForStaticTopic('T', new TopicRouteData()) === null,
    'empty mapping yields no endpoints');

// 4) scope 之间不合并 + epoch 围栏
$route2 = mappingRoute([
    'broker-old' => rawMapping('__global__', 1, 'broker-old', 3, ['0' => 0]),
    'broker-new' => rawMapping('__global__', 1, 'broker-new', 7, ['0' => 0]),
    'broker-b' => rawMapping('zone-x', 1, 'broker-b', 9, ['0' => 0]),
]);
$endpoints2 = StaticTopic::topicRouteData2EndpointsForStaticTopic('T', $route2);
check($endpoints2 !== null, 'route2 yields endpoints');
if ($endpoints2 !== null) {
    checkSame('broker-new', $endpoints2[StaticTopic::queueKey(new MessageQueue('T', '__syslo__global__', 0))],
        'higher epoch fences');
    checkSame('broker-b', $endpoints2[StaticTopic::queueKey(new MessageQueue('T', '__syslo__zone-x', 0))],
        'zone scope kept apart');
    checkSame(2, count($endpoints2), 'scopes do not merge');
}

// 5) 实例的 getBrokerNameFromMessageQueue 重写/透传
$inst = new MQClientInstance('static-test@DEFAULT', ['127.0.0.1:9876']);
$inst->topicEndPointsTable['StaticTopic'] = [
    StaticTopic::queueKey(new MessageQueue('StaticTopic', '__syslo__global__', 0)) => 'broker-a',
    StaticTopic::queueKey(new MessageQueue('StaticTopic', '__syslo__global__', 1)) => 'broker-b',
];
checkSame('broker-a', $inst->getBrokerNameFromMessageQueue(new MessageQueue('StaticTopic', '__syslo__global__', 0)),
    'global 0 rewritten');
checkSame('broker-b', $inst->getBrokerNameFromMessageQueue(new MessageQueue('StaticTopic', '__syslo__global__', 1)),
    'global 1 rewritten');
// 表里没有的队列 / 非静态主题原样透传自己的 broker 名。
checkSame('__syslo__global__', $inst->getBrokerNameFromMessageQueue(new MessageQueue('StaticTopic', '__syslo__global__', 99)),
    'unknown queue passes mock name through');
checkSame('broker-a', $inst->getBrokerNameFromMessageQueue(new MessageQueue('Plain', 'broker-a', 0)),
    'non-static topic passes through');

printf(
    "\n%s (%d checks)\n",
    $GLOBALS['ST_FAIL'] === 0 ? 'ALL TESTS PASSED' : sprintf('FAILED (%d)', $GLOBALS['ST_FAIL']),
    $GLOBALS['ST_PASS'] + $GLOBALS['ST_FAIL']
);
exit($GLOBALS['ST_FAIL'] === 0 ? 0 : 1);

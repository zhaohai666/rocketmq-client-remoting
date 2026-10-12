<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\TopicRouteData;

/**
 * 静态主题（单元化主题）消费者侧路由支持，移植自 Java 客户端：
 *
 * - org.apache.rocketmq.remoting.rpc.ClientMetadata
 *   .topicRouteData2EndpointsForStaticTopic
 * - org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils
 *   .getMockBrokerName
 * - org.apache.rocketmq.client.impl.factory.MQClientInstance
 *   .getBrokerNameFromMessageQueue（见 MQClientInstance 同名方法）
 *
 * wire 事实（不得"简化"）：
 *
 * - 路由解码侧把 topicQueueMappingByBroker 保持为不透明的
 *   `array<brokerName, array>`；类型化视图在这里解析，不动协议层透传。
 * - TopicQueueMappingInfo.scope 缺省为 MixAll::METADATA_SCOPE_GLOBAL
 *   （"__global__"）；global scope 的 mock broker 名会剥掉开头两个下划线
 *   （"__syslo__global__"）。
 * - Java 的 maxTotalNumOfEpoch 读了但从不赋值（恒 -1），所以
 *   epoch >= maxTotalNumOfEpoch 守卫恒真，maxTotalNums 就是该 scope 的
 *   totalQueues 最大值 —— 这个 quirk 照抄。
 * - 同 epoch 的后到条目覆盖先到条目（Java `oldEpoch <= epoch`）。
 */
final class StaticTopic
{
    public const METADATA_SCOPE_GLOBAL = '__global__';
    public const LOGICAL_QUEUE_MOCK_BROKER_PREFIX = '__syslo__';
    public const LOGICAL_QUEUE_MOCK_NOT_EXIST = '__syslo__none__';

    /** Java MixAll::isLogicalQueueMockBrokerName（前缀判断）。 */
    public static function isLogicalQueueMockBrokerName(string $brokerName): bool
    {
        return str_starts_with($brokerName, self::LOGICAL_QUEUE_MOCK_BROKER_PREFIX);
    }

    /**
     * getMockBrokerName："__global__"（scope 本身）剥掉头两个字符，其余 scope 原样加前缀。
     */
    public static function getMockBrokerName(string $scope): string
    {
        if ($scope === self::METADATA_SCOPE_GLOBAL) {
            return self::LOGICAL_QUEUE_MOCK_BROKER_PREFIX . substr($scope, 2);
        }

        return self::LOGICAL_QUEUE_MOCK_BROKER_PREFIX . $scope;
    }

    /**
     * PHP 数组键只能用标量：逻辑队列一律以 "topic@brokerName@queueId" 字符串键
     * 进端点表（JS 端同款教训：引用身份的键查不到等值条目）。
     */
    public static function queueKey(MessageQueue $mq): string
    {
        return $mq->topic . '@' . $mq->brokerName . '@' . $mq->queueId;
    }

    /**
     * 从路由解码侧保留下来的原始 json 对象里解析类型化视图。
     * 畸形条目返回 null —— Java 遇到会 NPE；能抢救的部分映射好过整个崩溃。
     *
     * @return array{scope: string, totalQueues: int, bname: string, epoch: int, currIdMap: array<int, int>}|null
     */
    public static function parseTopicQueueMappingInfo(mixed $raw): ?array
    {
        if (!is_array($raw)) {
            return null;
        }
        $info = [
            'scope' => self::METADATA_SCOPE_GLOBAL,
            'totalQueues' => 0,
            'bname' => '',
            'epoch' => 0,
            'currIdMap' => [],
        ];
        if (is_string($raw['scope'] ?? null)) {
            $info['scope'] = $raw['scope'];
        }
        if (is_int($raw['totalQueues'] ?? null) || is_float($raw['totalQueues'] ?? null)) {
            $info['totalQueues'] = (int) $raw['totalQueues'];
        }
        if (is_string($raw['bname'] ?? null)) {
            $info['bname'] = $raw['bname'];
        }
        if (is_int($raw['epoch'] ?? null) || is_float($raw['epoch'] ?? null)) {
            $info['epoch'] = (int) $raw['epoch'];
        }
        if (is_array($raw['currIdMap'] ?? null)) {
            foreach ($raw['currIdMap'] as $k => $v) {
                if (!is_numeric($k) || !is_numeric($v)) {
                    continue;
                }
                $info['currIdMap'][(int) $k] = (int) $v;
            }
        }

        return $info;
    }

    /**
     * ClientMetadata.topicRouteData2EndpointsForStaticTopic：构建逻辑队列集
     * MessageQueue(topic, mockBrokerName(scope), globalId) -> 承载它的真实 broker 名
     * （total 范围内没有承载者的逻辑 id 指向 "__syslo__none__"）。
     * 空映射（非静态主题）返回 null。
     *
     * 返回 `array<queueKey, realBrokerName>`。
     *
     * @return array<string, string>|null
     */
    public static function topicRouteData2EndpointsForStaticTopic(
        string $topic,
        ?TopicRouteData $route,
    ): ?array {
        if ($route === null || $route->topicQueueMappingByBroker === null
            || $route->topicQueueMappingByBroker === []) {
            return null;
        }

        // 按 scope 分组（Java 里没有 scope 的条目不参与，同此）。
        $infosByScope = [];
        foreach ($route->topicQueueMappingByBroker as $broker => $raw) {
            $info = self::parseTopicQueueMappingInfo($raw);
            if ($info === null) {
                continue;
            }
            $infosByScope[$info['scope']][$broker] = $info;
        }

        $endpoints = [];
        foreach ($infosByScope as $scope => $infos) {
            // 先按 broker 名排序，再做稳定的 epoch 降序排序。Java 对
            // ConcurrentHashMap 的 entrySet 做 epoch 降序排序，同 epoch 的覆盖顺序本就
            // 未定义；钉死 broker 升序让结果可复现。
            ksort($infos, SORT_STRING);
            uasort($infos, static fn (array $a, array $b) => $b['epoch'] <=> $a['epoch']);

            $maxTotalNums = 0;
            // Java 从不更新 maxTotalNumOfEpoch —— 守卫恒真，照抄。
            $maxTotalNumOfEpoch = -1;
            $mockName = self::getMockBrokerName($scope);
            $mqEndPoints = []; // queueKey => [epoch, bname]
            foreach ($infos as $info) {
                if ($info['epoch'] >= $maxTotalNumOfEpoch && $info['totalQueues'] > $maxTotalNums) {
                    $maxTotalNums = $info['totalQueues'];
                }
                // 只有 global id（键）形状决定端点集；物理 id 是 broker 内部的。
                foreach (array_keys($info['currIdMap']) as $globalId) {
                    $mq = new MessageQueue($topic, $mockName, $globalId);
                    $key = self::queueKey($mq);
                    $old = $mqEndPoints[$key] ?? null;
                    if ($old === null || $old[0] <= $info['epoch']) {
                        $mqEndPoints[$key] = [$info['epoch'], $info['bname']];
                    }
                }
            }

            // 补齐静态逻辑队列：total 之下的每个 id 都存在；没有承载者的指向
            // NOT_EXIST 哨兵。
            for ($i = 0; $i < $maxTotalNums; ++$i) {
                $mq = new MessageQueue($topic, $mockName, $i);
                $key = self::queueKey($mq);
                $holder = $mqEndPoints[$key] ?? null;
                $endpoints[$key] = $holder !== null ? $holder[1] : self::LOGICAL_QUEUE_MOCK_NOT_EXIST;
            }
        }

        if ($endpoints === []) {
            return null;
        }

        return $endpoints;
    }
}

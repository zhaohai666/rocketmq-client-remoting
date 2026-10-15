<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Common\MessageQueue;
use RocketMQ\Remoting\Protocol\TopicRouteData;

final class StaticTopic
{
    public const METADATA_SCOPE_GLOBAL = '__global__';
    public const LOGICAL_QUEUE_MOCK_BROKER_PREFIX = '__syslo__';
    public const LOGICAL_QUEUE_MOCK_NOT_EXIST = '__syslo__none__';

    public static function isLogicalQueueMockBrokerName(string $brokerName): bool
    {
        return str_starts_with($brokerName, self::LOGICAL_QUEUE_MOCK_BROKER_PREFIX);
    }

    /**
     * getMockBrokerName: "__global__" (the scope itself) strips the first two characters;
     * other scopes get the prefix added as-is.
     */
    public static function getMockBrokerName(string $scope): string
    {
        if ($scope === self::METADATA_SCOPE_GLOBAL) {
            return self::LOGICAL_QUEUE_MOCK_BROKER_PREFIX . substr($scope, 2);
        }

        return self::LOGICAL_QUEUE_MOCK_BROKER_PREFIX . $scope;
    }

    /**
     * PHP array keys can only be scalars: logical queues always enter the endpoint table
     * with a "topic@brokerName@queueId" string key (a reference-identity key cannot find
     * an equal entry).
     */
    public static function queueKey(MessageQueue $mq): string
    {
        return $mq->topic . '@' . $mq->brokerName . '@' . $mq->queueId;
    }

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
     * ClientMetadata.topicRouteData2EndpointsForStaticTopic: builds the logical queue set
     * MessageQueue(topic, mockBrokerName(scope), globalId) -> the real broker name hosting
     * it (a logical id with no host within the total range points to "__syslo__none__").
     * An empty mapping (non-static topic) returns null.
     *
     * Returns `array<queueKey, realBrokerName>`.
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
            ksort($infos, SORT_STRING);
            uasort($infos, static fn (array $a, array $b) => $b['epoch'] <=> $a['epoch']);

            $maxTotalNums = 0;
            $maxTotalNumOfEpoch = -1;
            $mockName = self::getMockBrokerName($scope);
            $mqEndPoints = []; // queueKey => [epoch, bname]
            foreach ($infos as $info) {
                if ($info['epoch'] >= $maxTotalNumOfEpoch && $info['totalQueues'] > $maxTotalNums) {
                    $maxTotalNums = $info['totalQueues'];
                }
                // Only the global id (key) shape decides the endpoint set; the physical id is broker-internal.
                foreach (array_keys($info['currIdMap']) as $globalId) {
                    $mq = new MessageQueue($topic, $mockName, $globalId);
                    $key = self::queueKey($mq);
                    $old = $mqEndPoints[$key] ?? null;
                    if ($old === null || $old[0] <= $info['epoch']) {
                        $mqEndPoints[$key] = [$info['epoch'], $info['bname']];
                    }
                }
            }

            // Fill in the static logical queues: every id below total exists; those with
            // no host point to the NOT_EXIST sentinel.
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

<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MixAll;

/**
 * 订阅组相关模型（对应 org.apache.rocketmq.remoting.protocol.subscription 包，
 * 移植自 python/remoting/protocol/subscription.py）：
 * SubscriptionGroupConfig / GroupRetryPolicy / SimpleSubscriptionData。
 *
 * 字段名与默认值以 Java 5.x 探针实测为准（``JSON.toJSONString(new SubscriptionGroupConfig())``）：
 * {"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,"consumeEnable":true,
 * "consumeFromMinEnable":true,"consumeMessageOrderly":false,"consumeTimeoutMinute":15,
 * "groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},"groupSysFlag":0,
 * "notifyConsumerIdsChangedEnable":true,"retryMaxTimes":16,"retryQueueNums":1,
 * "whichBrokerWhenConsumeSlowly":1}
 * 注意 fastjson2 **跳过 null 字段**（``subscriptionDataSet`` 为 null 时整个键不出现），
 * 所以 fromDict 一律用 ``?? default``。
 */

/** 对应 org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType。 */
final class GroupRetryPolicyType
{
    public const EXPONENTIAL = 'EXPONENTIAL';
    public const CUSTOMIZED = 'CUSTOMIZED';
}

/**
 * 对应 org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy。
 *
 * 简化版：只保留 type 与两个子策略的原始 array（Java 侧为 null 时不序列化）。
 */
final class GroupRetryPolicy
{
    public string $type = GroupRetryPolicyType::CUSTOMIZED;

    /** @var array<string, mixed>|null */
    public ?array $exponentialRetryPolicy = null;

    /** @var array<string, mixed>|null */
    public ?array $customizedRetryPolicy = null;

    /** @param array<string, mixed>|null $exponentialRetryPolicy @param array<string, mixed>|null $customizedRetryPolicy */
    public function __construct(
        string $policyType = GroupRetryPolicyType::CUSTOMIZED,
        ?array $exponentialRetryPolicy = null,
        ?array $customizedRetryPolicy = null,
    ) {
        $this->type = $policyType;
        $this->exponentialRetryPolicy = $exponentialRetryPolicy;
        $this->customizedRetryPolicy = $customizedRetryPolicy;
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = ['type' => $this->type];
        // Java 默认 type=CUSTOMIZED 且 customizedRetryPolicy 为 null（字段有默认实例但
        // fastjson2 只序列化非 null 的），探针输出里只有 type，故这里同样只在显式赋值时输出。
        if ($this->exponentialRetryPolicy !== null) {
            $d['exponentialRetryPolicy'] = $this->exponentialRetryPolicy;
        }
        if ($this->customizedRetryPolicy !== null) {
            $d['customizedRetryPolicy'] = $this->customizedRetryPolicy;
        }
        return $d;
    }

    /** @param array<string, mixed>|null $d */
    public static function fromDict(?array $d): self
    {
        $d = $d ?? [];
        return new self(
            (string) ($d['type'] ?? GroupRetryPolicyType::CUSTOMIZED),
            isset($d['exponentialRetryPolicy']) && is_array($d['exponentialRetryPolicy'])
                ? $d['exponentialRetryPolicy'] : null,
            isset($d['customizedRetryPolicy']) && is_array($d['customizedRetryPolicy'])
                ? $d['customizedRetryPolicy'] : null,
        );
    }
}

/** 对应 org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData。 */
final class SimpleSubscriptionData
{
    public function __construct(
        public string $topic = '',
        public string $expressionType = 'TAG',
        public string $expression = '*',
        public int $version = 0,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'topic' => $this->topic,
            'expressionType' => $this->expressionType,
            'expression' => $this->expression,
            'version' => $this->version,
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            (string) ($d['topic'] ?? ''),
            (string) ($d['expressionType'] ?? 'TAG'),
            (string) ($d['expression'] ?? '*'),
            (int) ($d['version'] ?? 0),
        );
    }
}

/** 对应 org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig。 */
final class SubscriptionGroupConfig
{
    public string $groupName = '';
    public bool $consumeEnable = true;
    public bool $consumeFromMinEnable = true;
    public bool $consumeBroadcastEnable = true;
    public bool $consumeMessageOrderly = false;
    public int $retryQueueNums = 1;
    public int $retryMaxTimes = 16;
    public GroupRetryPolicy $groupRetryPolicy;
    /** MixAll.MASTER_ID（Python 模块级 MASTER_ID = 0）。 */
    public int $brokerId = MixAll::MASTER_ID;
    public int $whichBrokerWhenConsumeSlowly = 1;
    public bool $notifyConsumerIdsChangedEnable = true;
    public int $groupSysFlag = 0;
    public int $consumeTimeoutMinute = 15;
    /** @var list<SimpleSubscriptionData>|null */
    public ?array $subscriptionDataSet = null;
    /** @var array<string, string> */
    public array $attributes = [];

    public function __construct(string $groupName = '')
    {
        $this->groupName = $groupName;
        $this->groupRetryPolicy = new GroupRetryPolicy();
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [
            'groupName' => $this->groupName,
            'consumeEnable' => $this->consumeEnable,
            'consumeFromMinEnable' => $this->consumeFromMinEnable,
            'consumeBroadcastEnable' => $this->consumeBroadcastEnable,
            'consumeMessageOrderly' => $this->consumeMessageOrderly,
            'retryQueueNums' => $this->retryQueueNums,
            'retryMaxTimes' => $this->retryMaxTimes,
            'groupRetryPolicy' => $this->groupRetryPolicy->toDict(),
            'brokerId' => $this->brokerId,
            'whichBrokerWhenConsumeSlowly' => $this->whichBrokerWhenConsumeSlowly,
            'notifyConsumerIdsChangedEnable' => $this->notifyConsumerIdsChangedEnable,
            'groupSysFlag' => $this->groupSysFlag,
            'consumeTimeoutMinute' => $this->consumeTimeoutMinute,
            'attributes' => $this->attributes,
        ];
        // fastjson2 默认跳过 null
        if ($this->subscriptionDataSet !== null) {
            $d['subscriptionDataSet'] = array_map(
                static fn(SimpleSubscriptionData $s): array => $s->toDict(),
                $this->subscriptionDataSet,
            );
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $cfg = new self((string) ($d['groupName'] ?? ''));
        $cfg->consumeEnable = (bool) ($d['consumeEnable'] ?? true);
        $cfg->consumeFromMinEnable = (bool) ($d['consumeFromMinEnable'] ?? true);
        $cfg->consumeBroadcastEnable = (bool) ($d['consumeBroadcastEnable'] ?? true);
        $cfg->consumeMessageOrderly = (bool) ($d['consumeMessageOrderly'] ?? false);
        $cfg->retryQueueNums = (int) ($d['retryQueueNums'] ?? 1);
        $cfg->retryMaxTimes = (int) ($d['retryMaxTimes'] ?? 16);
        $cfg->groupRetryPolicy = GroupRetryPolicy::fromDict(
            isset($d['groupRetryPolicy']) && is_array($d['groupRetryPolicy']) ? $d['groupRetryPolicy'] : null
        );
        $cfg->brokerId = (int) ($d['brokerId'] ?? MixAll::MASTER_ID);
        $cfg->whichBrokerWhenConsumeSlowly = (int) ($d['whichBrokerWhenConsumeSlowly'] ?? 1);
        $cfg->notifyConsumerIdsChangedEnable = (bool) ($d['notifyConsumerIdsChangedEnable'] ?? true);
        $cfg->groupSysFlag = (int) ($d['groupSysFlag'] ?? 0);
        $cfg->consumeTimeoutMinute = (int) ($d['consumeTimeoutMinute'] ?? 15);
        $rawSub = $d['subscriptionDataSet'] ?? null;
        $cfg->subscriptionDataSet = is_array($rawSub) && $rawSub !== []
            ? array_map(static fn(array $x): SimpleSubscriptionData => SimpleSubscriptionData::fromDict($x), array_values($rawSub))
            : null;
        $cfg->attributes = (array) ($d['attributes'] ?? []);
        return $cfg;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::decodeJson($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }

    public function __toString(): string
    {
        return sprintf(
            'SubscriptionGroupConfig[groupName=%s, retryQueueNums=%d, brokerId=%d]',
            $this->groupName,
            $this->retryQueueNums,
            $this->brokerId,
        );
    }
}

/**
 * 对应 org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper。
 *
 * 探针输出：{"dataVersion":{...},"forbiddenTable":{},"subscriptionGroupTable":{...}}
 */
final class SubscriptionGroupWrapper
{
    /** @var array<string, SubscriptionGroupConfig> */
    public array $subscriptionGroupTable = [];

    /** @var array<string, mixed> */
    public array $forbiddenTable = [];

    /** @var array<string, mixed> */
    public array $dataVersion = [];

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        return [
            'dataVersion' => (object) $this->dataVersion,
            'forbiddenTable' => (object) $this->forbiddenTable,
            'subscriptionGroupTable' => (object) array_map(
                static fn(SubscriptionGroupConfig $v): array => $v->toDict(),
                $this->subscriptionGroupTable,
            ),
        ];
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $w = new self();
        $table = (array) ($d['subscriptionGroupTable'] ?? []);
        foreach ($table as $k => $v) {
            $w->subscriptionGroupTable[(string) $k] = SubscriptionGroupConfig::fromDict((array) $v);
        }
        $w->forbiddenTable = (array) ($d['forbiddenTable'] ?? []);
        $w->dataVersion = (array) ($d['dataVersion'] ?? []);
        return $w;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::decodeJson($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }
}

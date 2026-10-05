<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * TopicFilterType（对应 org.apache.rocketmq.common.TopicFilterType，移植自 topic_config.py）。
 */
final class TopicFilterType
{
    public const SINGLE_TAG = 'SINGLE_TAG';
    public const MULTI_TAG = 'MULTI_TAG';
}

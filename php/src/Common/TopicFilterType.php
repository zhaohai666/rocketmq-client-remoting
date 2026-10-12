<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Topic 的 tag 过滤类型：单 tag 还是多 tag（建 topic 时上报的字段）。
 */
final class TopicFilterType
{
    public const SINGLE_TAG = 'SINGLE_TAG';
    public const MULTI_TAG = 'MULTI_TAG';
}

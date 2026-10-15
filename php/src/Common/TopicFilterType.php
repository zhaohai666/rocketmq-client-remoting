<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Tag filter type for a topic: single tag or multi tag (field reported when creating a topic).
 */
final class TopicFilterType
{
    public const SINGLE_TAG = 'SINGLE_TAG';
    public const MULTI_TAG = 'MULTI_TAG';
}

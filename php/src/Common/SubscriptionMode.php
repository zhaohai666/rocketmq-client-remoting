<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 订阅模式：POP 投递按逻辑队列还是物理队列。
 */
final class SubscriptionMode
{
    public const GROUP = 0;
    public const BROADCASTING = 1;
}

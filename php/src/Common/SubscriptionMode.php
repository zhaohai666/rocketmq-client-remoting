<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 订阅模式（对应 Java SubscriptionMode，移植自 sysflag.py 的 SubscriptionMode）。
 */
final class SubscriptionMode
{
    public const GROUP = 0;
    public const BROADCASTING = 1;
}

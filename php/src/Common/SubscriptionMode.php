<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Subscription mode: whether POP delivery is by logical queue or physical queue.
 */
final class SubscriptionMode
{
    public const GROUP = 0;
    public const BROADCASTING = 1;
}

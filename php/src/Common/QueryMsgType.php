<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * The query mode submitted to the broker when querying messages by key.
 *
 * This is not the same as `MixAll::UNIQUE_MSG_QUERY_FLAG`: the latter is the **key name** in extFields.
 */
final class QueryMsgType
{
    public const ALL_MESSAGE = 0;
    public const UNIQUE_KEY = 1;
    public const NORMAL = 2;
}

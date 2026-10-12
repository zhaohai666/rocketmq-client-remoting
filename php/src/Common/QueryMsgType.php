<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 按 key 查消息时提交给 broker 的查询模式。
 *
 * 与 `MixAll::UNIQUE_MSG_QUERY_FLAG` 不是一个东西：后者是 extFields 里的**键名**。
 */
final class QueryMsgType
{
    public const ALL_MESSAGE = 0;
    public const UNIQUE_KEY = 1;
    public const NORMAL = 2;
}

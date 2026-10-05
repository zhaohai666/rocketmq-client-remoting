<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 按 key 查消息的三种模式（对应 tools 的 QueryMsgByKeySubCommand.QueryMsgType，
 * 移植自 mix_all.py 的模块级类）。
 *
 * 与 `MixAll::UNIQUE_MSG_QUERY_FLAG` 不是一个东西：后者是 extFields 里的**键名**。
 */
final class QueryMsgType
{
    public const ALL_MESSAGE = 0;
    public const UNIQUE_KEY = 1;
    public const NORMAL = 2;
}

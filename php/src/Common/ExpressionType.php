<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 订阅表达式的类型：broker 按它选择 tag 过滤还是 SQL92 过滤。
 */
final class ExpressionType
{
    public const TAG = 'TAG';
    public const SQL92 = 'SQL92';
    public const CLASS_FILTER = 'CLASS_FILTER';
}

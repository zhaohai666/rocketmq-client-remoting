<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 表达式类型（对应 org.apache.rocketmq.common.filter.ExpressionType，移植自 subscription_data.py）。
 */
final class ExpressionType
{
    public const TAG = 'TAG';
    public const SQL92 = 'SQL92';
    public const CLASS_FILTER = 'CLASS_FILTER';
}

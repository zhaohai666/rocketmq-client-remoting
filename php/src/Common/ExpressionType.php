<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Type of the subscription expression: the broker uses it to choose tag filtering or SQL92 filtering.
 */
final class ExpressionType
{
    public const TAG = 'TAG';
    public const SQL92 = 'SQL92';
    public const CLASS_FILTER = 'CLASS_FILTER';
}

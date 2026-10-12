<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 时间戳查位点的边界语义：LOWER 取右边界，UPPER 取左边界。
 *
 * 报文侧的关键事实（别按字面直觉改）：
 *
 *   * 入网的是**大写枚举名** ``LOWER`` / ``UPPER``——header 序列化输出的是枚举名，
 *     不是那个小写的显示名；小写名只用于本地比对与日志，从不上报文。
 *   * broker 侧解 header 时的转换是**宽松**的：只有忽略大小写等于 ``"upper"`` 才认
 *     UPPER，其余（null / 空串 / 未知值 / 小写 "lower"）一律落到 LOWER。
 *     所以发什么大小写都能被认出来，但本端口固定发大写枚举名。
 *
 * broker 侧语义（按 storeTime 在 ConsumeQueue 里二分）：
 *
 *   * 时间戳大于队列里最后一条的 storeTime ⇒ LOWER = maxOffset（下一个可写位点），
 *     UPPER = maxOffset - 1（最后一条自己的位点）；
 *   * 时间戳小于最老一条 ⇒ LOWER = 0，UPPER = 0；
 *   * 命中 ⇒ LOWER = 共享该 storeTime 的第一条，UPPER = 最后一条；
 *   * 落在空档 ⇒ LOWER = 下一条（右边界），UPPER = 上一条（左边界）。
 */
enum BoundaryType: string
{
    /** backed value 就是入网文本：大写枚举名。 */
    case LOWER = 'LOWER';
    case UPPER = 'UPPER';

    /**
     * 小写显示名（"lower"/"upper"），只用于本地比对与日志，不上报文。
     */
    public function lowercaseName(): string
    {
        return strtolower($this->value);
    }

    /**
     * 宽松解析：非 "upper"（大小写不敏感）一律落到 LOWER。
     */
    public static function getType(?string $name): self
    {
        if ($name !== null && strtolower($name) === self::UPPER->lowercaseName()) {
            return self::UPPER;
        }
        return self::LOWER;
    }
}

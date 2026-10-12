<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 订阅串的解析入口：把 tag 表达式拆成 tagsSet / codeSet。
 */
final class FilterAPI
{
    public const SUB_ALL = '*';

    /**
     * 解析订阅串（下面每张向量都是硬要求，改实现必须逐条对上）。
     *
     * 解析结果：
     *   "*" / null / ""  → subString 归一为 "*"，**tagsSet 与 codeSet 都保持空**
     *   "TagA"           → tagsSet={TagA}, codeSet={2598919}
     *   "TagA||TagB"     → tagsSet={TagA,TagB}, codeSet={2598919,2598920}
     *   " TagA || TagB " → **subString 原样保留空格**，标签各自 trim
     *   "   "（纯空白）   → tagsSet 空、**subString 原样保留**（判空只认 null/""，不认空白）
     *   "|||"            → tagsSet={|}, codeSet={124}（拆分只丢**末尾**空串）
     *   "||" / "||||"    → 抛 "subString split error"（拆完一个有效标签都不剩）
     *
     * ⚠ 曾经的实现给 "*" 塞了 tagsSet={"*"}，并且从不填 codeSet —— 两处都是偏差：
     * ① tagsSet 非空是"客户端二次 tag 过滤"的开关（拉取结果按 tagsSet 是否为空
     * 决定过不过滤），塞了 "*" 会让订阅全量时把所有正常 tag 的消息客户端自己过滤掉；
     * ② codeSet 是 broker 侧按 tag 哈希过滤的依据（ConsumeQueue 里只存哈希，
     * 比对的就是 codeSet）。
     */
    public static function buildSubscriptionData(string $topic, ?string $subString): SubscriptionData
    {
        $sub = new SubscriptionData(topic: $topic, subString: $subString);
        // 只认 null/"" 或正好是 "*"，命中就归一为 "*" 直接返回；
        // 不 trim —— 纯空白会走进下面的 split 分支。
        if ($subString === null || $subString === '' || $subString === self::SUB_ALL) {
            $sub->setSubString(self::SUB_ALL);
            return $sub;
        }
        // 按 "||" 拆分：丢弃**末尾**空串（与 explode 后逐个 trim 不等价）
        $parts = explode('||', $subString);
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        if ($parts === []) {
            // 这条文案要原样抛给调用方：上层按它判定订阅串非法
            throw new \InvalidArgumentException('subString split error');
        }
        foreach ($parts as $part) {
            $tag = trim($part);
            if ($tag !== '') {
                $sub->addTag($tag);
                $sub->addCode(UtilAll::wireStringHash($tag));
            }
        }
        return $sub;
    }
}

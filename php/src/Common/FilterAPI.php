<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * FilterAPI（对应 org.apache.rocketmq.common.filter.FilterAPI，移植自 subscription_data.py）。
 */
final class FilterAPI
{
    public const SUB_ALL = '*';

    /**
     * 对齐 Java `FilterAPI.buildSubscriptionData`（探针实测向量见下）。
     *
     * Java 行为（`/tmp/subprobe/SubProbe.java` + `BlankProbe.java` + `EdgeProbe.java` 实测）：
     *   "*" / null / ""  → subString 归一为 "*"，**tagsSet 与 codeSet 都保持空**
     *   "TagA"           → tagsSet={TagA}, codeSet={2598919}
     *   "TagA||TagB"     → tagsSet={TagA,TagB}, codeSet={2598919,2598920}
     *   " TagA || TagB " → **subString 原样保留空格**，标签各自 trim
     *   "   "（纯空白）   → tagsSet 空、**subString 原样保留**（StringUtils.isEmpty 只认 null/""）
     *   "|||"            → tagsSet={|}, codeSet={124}（Java-split 只丢**末尾**空串）
     *   "||" / "||||"    → 抛 "subString split error"（Java-split 结果数组长度为 0）
     *
     * ⚠ 曾经的实现给 "*" 塞了 tagsSet={"*"}，并且从不填 codeSet —— 两处都是偏差：
     * ① Java 里 tagsSet 非空是"客户端二次 tag 过滤"的开关（`PullAPIWrapper
     * .processPullResult` 的 `!tagsSet.isEmpty()`），塞了 "*" 会让订阅全量时把
     * 所有正常 tag 的消息客户端自己过滤掉；② codeSet 是 broker 侧按 tag 哈希过滤的依据
     * （`ExpressionMessageFilter.isMatchedByConsumeQueue` 走 `codeSet.contains`）。
     */
    public static function buildSubscriptionData(string $topic, ?string $subString): SubscriptionData
    {
        $sub = new SubscriptionData(topic: $topic, subString: $subString);
        // Java: StringUtils.isEmpty(subString) || subString.equals("*") -> setSubString("*") 后直接 return
        // 注意是 isEmpty（只认 null/""）而非 isBlank —— 纯空白会走进下面的 split 分支。
        if ($subString === null || $subString === '' || $subString === self::SUB_ALL) {
            $sub->setSubString(self::SUB_ALL);
            return $sub;
        }
        // Java String.split("\\|\\|")：丢弃**末尾**空串
        $parts = explode('||', $subString);
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        if ($parts === []) {
            // Java 这里是 throw new Exception("subString split error")
            throw new \InvalidArgumentException('subString split error');
        }
        foreach ($parts as $part) {
            $tag = trim($part);
            if ($tag !== '') {
                $sub->addTag($tag);
                $sub->addCode(UtilAll::javaStringHash($tag));
            }
        }
        return $sub;
    }
}

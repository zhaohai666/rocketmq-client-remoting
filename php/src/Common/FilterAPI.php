<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Entry point for parsing subscription strings: splits a tag expression into tagsSet / codeSet.
 */
final class FilterAPI
{
    public const SUB_ALL = '*';

    /**
     * Parse the subscription string (each row below is a hard requirement; any reimplementation must match them all).
     *
     * Parsing results:
     *   "*" / null / ""  -> subString normalized to "*", **both tagsSet and codeSet stay empty**
     *   "TagA"           -> tagsSet={TagA}, codeSet={2598919}
     *   "TagA||TagB"     -> tagsSet={TagA,TagB}, codeSet={2598919,2598920}
     *   " TagA || TagB " -> **subString keeps spaces as-is**, each tag is trimmed individually
     *   "   " (whitespace only) -> tagsSet empty, **subString kept as-is** (empty check only accepts null/"", not whitespace)
     *   "|||"            -> tagsSet={|}, codeSet={124} (split drops only the **trailing** empty strings)
     *   "||" / "||||"    -> throws "subString split error" (no valid tag left after splitting)
     *
     * WARNING: A previous implementation filled tagsSet={"*"} for "*" and never populated codeSet — both were deviations:
     * 1. A non-empty tagsSet is the switch for "client-side secondary tag filtering" (pull results are filtered or not
     *    based on whether tagsSet is empty). Filling "*" would make the client filter out all normal-tag messages when
     *    subscribed to everything;
     * 2. codeSet is the broker-side basis for tag-hash filtering (ConsumeQueue only stores hashes, and the comparison
     *    is against codeSet).
     */
    public static function buildSubscriptionData(string $topic, ?string $subString): SubscriptionData
    {
        $sub = new SubscriptionData(topic: $topic, subString: $subString);
        // Only null/"" or exactly "*" are recognized; on a match normalize to "*" and return directly.
        // Do not trim —— pure whitespace falls into the split branch below.
        if ($subString === null || $subString === '' || $subString === self::SUB_ALL) {
            $sub->setSubString(self::SUB_ALL);
            return $sub;
        }
        // Split by "||": drop only the **trailing** empty strings (not equivalent to trim after explode)
        $parts = explode('||', $subString);
        while ($parts !== [] && end($parts) === '') {
            array_pop($parts);
        }
        if ($parts === []) {
            // This message must be thrown to the caller as-is: the upper layer uses it to judge the subscription string invalid
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

<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Boundary semantics for querying offsets by timestamp: LOWER takes the right boundary, UPPER takes the left boundary.
 *
 * Key facts on the wire side (do not change based on literal intuition):
 *
 *   * What goes on the wire is the **upper-case enum name** ``LOWER`` / ``UPPER`` — header serialization outputs the
 *     enum name, not the lower-case display name; the lower-case name is only used for local comparison and logging,
 *     and never goes on the wire.
 *   * The broker side's parsing of the header is **lenient**: only a case-insensitive match of ``"upper"`` is
 *     recognized as UPPER; everything else (null / empty string / unknown value / lower-case "lower") falls back to
 *     LOWER. So any casing sent can be recognized, but this port always sends the upper-case enum name.
 *
 * Broker side semantics (binary search in ConsumeQueue by storeTime):
 *
 *   * Timestamp greater than the storeTime of the last message in the queue ⇒ LOWER = maxOffset (next writable offset),
 *     UPPER = maxOffset - 1 (the offset of the last message itself);
 *   * Timestamp less than the oldest message ⇒ LOWER = 0, UPPER = 0;
 *   * Exact match ⇒ LOWER = the first message sharing that storeTime, UPPER = the last one;
 *   * Falls in a gap ⇒ LOWER = the next message (right boundary), UPPER = the previous message (left boundary).
 */
enum BoundaryType: string
{
    /** The backed value is the wire text: upper-case enum name. */
    case LOWER = 'LOWER';
    case UPPER = 'UPPER';

    /**
     * Lower-case display name ("lower"/"upper"), only used for local comparison and logging, never on the wire.
     */
    public function lowercaseName(): string
    {
        return strtolower($this->value);
    }

    /**
     * Lenient parsing: anything other than "upper" (case-insensitive) falls back to LOWER.
     */
    public static function getType(?string $name): self
    {
        if ($name !== null && strtolower($name) === self::UPPER->lowercaseName()) {
            return self::UPPER;
        }
        return self::LOWER;
    }
}

<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Broker-side internal message wrapper: on top of MessageExt it carries the storage layer's own
 * propertiesString / tagsCode / encode buffer / encode version.
 *
 * This port only uses its fields when parsing broker responses, and does not participate in the send path.
 *
 * msgId semantics: inherited from MessageExt — the wire msgId = storeHost(ip+port) + commitLogOffset
 * as a hex string (see MessageDecoder::createMessageId), and offsetMsgId equals it.
 */
class MessageExtBrokerInner extends MessageExt
{
    public ?string $propertiesString = null;

    public int $tagsCode = 0;

    /** One-shot encode buffer for the whole message; this port carries it as a raw byte string, null means not yet encoded */
    public ?string $encodedBuff = null;

    public bool $encodeCompleted = false;

    /** Message body encode version: 1 = V1, 2 = V2; determines how the magic number and topic length are interpreted on decode */
    public int $version = 1;

    public function getPropertiesString(): ?string
    {
        return $this->propertiesString;
    }

    public function setPropertiesString(?string $propertiesString): void
    {
        $this->propertiesString = $propertiesString;
    }

    public function getTagsCode(): int
    {
        return $this->tagsCode;
    }

    public function setTagsCode(int $tagsCode): void
    {
        $this->tagsCode = $tagsCode;
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    public function setVersion(int $version): void
    {
        $this->version = $version;
    }

    /**
     * tag text -> tagsCode: empty string yields 0, otherwise take the protocol-side string hash
     * (32-bit signed wraparound, see UtilAll::wireStringHash). The broker's tag filtering
     * compares against this number; a different hash algorithm would silently miss deliveries.
     */
    public static function tagsString2tagsCode(?string $tags): int
    {
        if ($tags === null || $tags === '') {
            return 0;
        }
        return UtilAll::wireStringHash($tags);
    }

    /**
     * Delete a property: both the properties table and propertiesString representations must be deleted,
     * otherwise a later path that serializes propertiesString directly would still carry the old value.
     */
    public function deleteProperty(string $name): void
    {
        $this->removeProperty($name);
        if ($this->propertiesString !== null) {
            // Remove the key from the k\x01v\x02 string (equivalent implementation: parse back to map, delete, re-serialize;
            // the encoding format is fully consistent with MessageDecoder)
            $props = MessageDecoder::string2MessageProperties($this->propertiesString);
            unset($props[$name]);
            $this->propertiesString = MessageDecoder::messageProperties2String($props);
        }
    }

    /**
     * Do not write "WAIT=true" into propertiesString (saves 9 bytes/message); refill the properties table instead,
     * because isWaitStoreMsgOK() is still called after serialization.
     */
    public function removeWaitStorePropertyString(): void
    {
        if (array_key_exists(MessageConst::PROPERTY_WAIT_STORE_MSG_OK, $this->properties)) {
            $waitStoreMsgOkValue = $this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK];
            unset($this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK]);
            $this->setPropertiesString(MessageDecoder::messageProperties2String($this->properties));
            // The value is only omitted from the string; the properties table must keep a copy for isWaitStoreMsgOK() to read
            $this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = $waitStoreMsgOkValue;
        } else {
            $this->setPropertiesString(MessageDecoder::messageProperties2String($this->properties));
        }
    }
}

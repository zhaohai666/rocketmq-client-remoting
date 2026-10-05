<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 对应 org.apache.rocketmq.store.MessageExtBrokerInner（broker 侧内部消息包装）。
 *
 * Python common 层没有对应模块（消费/发送路径用不到）；按用户要求按 Java 原类
 * （common/src/main/java/org/apache/rocketmq/common/message/MessageExtBrokerInner.java）
 * 的字段与方法补齐，供后续 store 相关移植复用。
 *
 * msgId 语义：继承 MessageExt——线上 msgId = storeHost(ip+port) + commitLogOffset
 * 的十六进制串（见 MessageDecoder::createMessageId），offsetMsgId 与之相等。
 */
class MessageExtBrokerInner extends MessageExt
{
    public ?string $propertiesString = null;

    public int $tagsCode = 0;

    /** Java 里的 ByteBuffer encodedBuff；PHP 用原始字节串承载 */
    public ?string $encodedBuff = null;

    public bool $encodeCompleted = false;

    /** Java MessageVersion.MESSAGE_VERSION_V1；1 = V1，2 = V2 */
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
     * Java `tagsString2tagsCode`：`Strings.isNullOrEmpty(tags) ? 0 : tags.hashCode()`。
     */
    public static function tagsString2tagsCode(?string $tags): int
    {
        if ($tags === null || $tags === '') {
            return 0;
        }
        return UtilAll::javaStringHash($tags);
    }

    /**
     * Java `deleteProperty`：同时从 properties 表与 propertiesString 里删除。
     */
    public function deleteProperty(string $name): void
    {
        $this->removeProperty($name);
        if ($this->propertiesString !== null) {
            // MessageUtils.deleteProperty：从 k\x01v\x02 串里摘掉该键（等价实现：
            // 解析回 map、删除、重序列化，格式与 MessageDecoder 完全一致）
            $props = MessageDecoder::string2MessageProperties($this->propertiesString);
            unset($props[$name]);
            $this->propertiesString = MessageDecoder::messageProperties2String($props);
        }
    }

    /**
     * Java `removeWaitStorePropertyString`：propertiesString 里不落 "WAIT=true"
     *（省 9 字节/条），properties 表回填以便后续 isWaitStoreMsgOK 判定。
     */
    public function removeWaitStorePropertyString(): void
    {
        if (array_key_exists(MessageConst::PROPERTY_WAIT_STORE_MSG_OK, $this->properties)) {
            $waitStoreMsgOkValue = $this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK];
            unset($this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK]);
            $this->setPropertiesString(MessageDecoder::messageProperties2String($this->properties));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            $this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = $waitStoreMsgOkValue;
        } else {
            $this->setPropertiesString(MessageDecoder::messageProperties2String($this->properties));
        }
    }
}

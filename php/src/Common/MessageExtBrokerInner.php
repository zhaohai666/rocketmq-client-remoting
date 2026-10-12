<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * broker 侧内部消息包装：在 MessageExt 之外再带上存储层自己维护的
 * propertiesString / tagsCode / 编码缓冲 / 编码版本。
 *
 * 本端口只在解析 broker 回包时用到它的字段，不参与发送路径。
 *
 * msgId 语义：继承 MessageExt——线上 msgId = storeHost(ip+port) + commitLogOffset
 * 的十六进制串（见 MessageDecoder::createMessageId），offsetMsgId 与之相等。
 */
class MessageExtBrokerInner extends MessageExt
{
    public ?string $propertiesString = null;

    public int $tagsCode = 0;

    /** 整条消息的一次性编码缓冲；本端口用原始字节串承载，null 表示尚未编码 */
    public ?string $encodedBuff = null;

    public bool $encodeCompleted = false;

    /** 消息体编码版本：1 = V1，2 = V2；决定 decode 时如何解释魔数与 topic 长度 */
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
     * tag 文本 -> tagsCode：空串得 0，否则取协议侧字符串哈希
     * （32 位有符号回绕，见 UtilAll::wireStringHash）。broker 的 tag 过滤
     * 就按这个数值比对，哈希算法不同会静默漏投。
     */
    public static function tagsString2tagsCode(?string $tags): int
    {
        if ($tags === null || $tags === '') {
            return 0;
        }
        return UtilAll::wireStringHash($tags);
    }

    /**
     * 删除属性：properties 表与 propertiesString 两份表示都要删，
     * 否则后续直接序列化 propertiesString 的路径仍会带上旧值。
     */
    public function deleteProperty(string $name): void
    {
        $this->removeProperty($name);
        if ($this->propertiesString !== null) {
            // 从 k\x01v\x02 串里摘掉该键（等价实现：解析回 map、删除、重序列化，
            // 编码格式与 MessageDecoder 完全一致）
            $props = MessageDecoder::string2MessageProperties($this->propertiesString);
            unset($props[$name]);
            $this->propertiesString = MessageDecoder::messageProperties2String($props);
        }
    }

    /**
     * propertiesString 里不落 "WAIT=true"（省 9 字节/条）；properties 表回填，
     * 因为 isWaitStoreMsgOK() 在序列化之后还要被调用。
     */
    public function removeWaitStorePropertyString(): void
    {
        if (array_key_exists(MessageConst::PROPERTY_WAIT_STORE_MSG_OK, $this->properties)) {
            $waitStoreMsgOkValue = $this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK];
            unset($this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK]);
            $this->setPropertiesString(MessageDecoder::messageProperties2String($this->properties));
            // 值只是从串里省掉，属性表必须留一份供 isWaitStoreMsgOK() 读取
            $this->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = $waitStoreMsgOkValue;
        } else {
            $this->setPropertiesString(MessageDecoder::messageProperties2String($this->properties));
        }
    }
}

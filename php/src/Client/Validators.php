<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\TopicValidator;
use RocketMQ\Remoting\Protocol\ResponseCode;

/**
 * 发送/订阅入口上的名字校验（对应 ``org.apache.rocketmq.client.Validators``），
 * 移植自 validators.py。
 *
 * 逐条对齐 Java：
 *   * ``checkTopic`` / ``checkGroup``：blank → 长度（127 / 120）→ 字符表，顺序与文案照抄。
 *     这三步走 ``MQClientException(String, null)``，responseCode 为 null（纯客户端错误）。
 *   * ``checkMessage``：只有它带 ``ResponseCode.MESSAGE_ILLEGAL``(13)，且**顺序**要对 ——
 *     Java 先查 topic、再查禁发 topic、最后才查 body。
 *   * ``isNotAllowedSendTopic``：只禁 broker 内部流水那 8 个；**``%RETRY%`` 不在名单里**。
 */
final class Validators
{
    /** 对应 Java ``Validators.CHARACTER_MAX_LENGTH``，本文件里没用到但保留常量口径。 */
    public const CHARACTER_MAX_LENGTH = 255;

    /** 对应 Python ``os.sep``：多队列分发（LMQ）路径里不允许出现文件系统分隔符。 */
    public const FILE_SEPARATOR = DIRECTORY_SEPARATOR;

    /** 对应 ``Validators.checkGroup``。 */
    public static function checkGroup(?string $group): void
    {
        if ($group === null || trim($group) === '') {
            throw new MQClientException('the specified group is blank');
        }
        if (mb_strlen($group, 'UTF-8') > TopicValidator::GROUP_MAX_LENGTH) {
            throw new MQClientException(sprintf(
                'the specified group[%s] is longer than group max length: %s.',
                $group,
                TopicValidator::GROUP_MAX_LENGTH
            ));
        }
        if (TopicValidator::isTopicOrGroupIllegal($group)) {
            throw new MQClientException(sprintf(
                'the specified group[%s] contains illegal characters, allowing only %s',
                $group,
                TopicValidator::VALID_CHAR_PATTERN
            ));
        }
    }

    /** 对应 ``Validators.checkTopic``。 */
    public static function checkTopic(?string $topic): void
    {
        if ($topic === null || trim($topic) === '') {
            throw new MQClientException('The specified topic is blank');
        }
        if (mb_strlen($topic, 'UTF-8') > TopicValidator::TOPIC_MAX_LENGTH) {
            throw new MQClientException(sprintf(
                'The specified topic is longer than topic max length %d.',
                TopicValidator::TOPIC_MAX_LENGTH
            ));
        }
        if (TopicValidator::isTopicOrGroupIllegal($topic)) {
            throw new MQClientException(sprintf(
                'The specified topic[%s] contains illegal characters, allowing only %s',
                $topic,
                TopicValidator::VALID_CHAR_PATTERN
            ));
        }
    }

    /** 对应 ``Validators.isSystemTopic``：命中系统 topic 直接抛，正常时返回。 */
    public static function isSystemTopic(string $topic): void
    {
        if (TopicValidator::isSystemTopic($topic)) {
            throw new MQClientException(sprintf('The topic[%s] is conflict with system topic.', $topic));
        }
    }

    /** 对应 ``Validators.isNotAllowedSendTopic``。 */
    public static function isNotAllowedSendTopic(string $topic): void
    {
        if (TopicValidator::isNotAllowedSendTopic($topic)) {
            throw new MQClientException(sprintf('Sending message to topic[%s] is forbidden.', $topic));
        }
    }

    /**
     * 对应 ``Validators.checkMessage(msg, producer)``（顺序与文案逐条照抄）。
     *
     * 码值口径：null message / null body / zero body / oversize body /
     * INNER_MULTI_DISPATCH 这五条带 MESSAGE_ILLEGAL(13)；禁发 topic、系统 topic、
     * checkTopic、checkGroup 走纯客户端错误（responseCode 为 null）。
     */
    public static function checkMessage(?Message $msg, int $maxMessageSize): void
    {
        if ($msg === null) {
            throw new MQClientException('the message is null', ResponseCode::MESSAGE_ILLEGAL);
        }
        self::checkTopic($msg->topic);
        self::isNotAllowedSendTopic($msg->topic);
        $body = $msg->body;
        if ($body === null) {
            throw new MQClientException('the message body is null', ResponseCode::MESSAGE_ILLEGAL);
        }
        if (strlen($body) === 0) {
            throw new MQClientException('the message body length is zero', ResponseCode::MESSAGE_ILLEGAL);
        }
        if (strlen($body) > $maxMessageSize) {
            throw new MQClientException(
                sprintf('the message body size over max value, MAX: %d', $maxMessageSize),
                ResponseCode::MESSAGE_ILLEGAL
            );
        }
        // 多队列分发（LMQ）的路径里带文件系统分隔符会让 broker 侧建队列时拼出越界路径
        $lmqPath = $msg->getProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
        if ($lmqPath !== null && $lmqPath !== '' && str_contains($lmqPath, self::FILE_SEPARATOR)) {
            throw new MQClientException(
                sprintf(
                    'INNER_MULTI_DISPATCH %s can not contains %s character',
                    $lmqPath,
                    self::FILE_SEPARATOR
                ),
                ResponseCode::MESSAGE_ILLEGAL
            );
        }
    }
}

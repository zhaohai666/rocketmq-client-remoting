<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\Message;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\TopicValidator;
use RocketMQ\Remoting\Protocol\ResponseCode;

final class Validators
{
    public const CHARACTER_MAX_LENGTH = 255;

    /** Filesystem separator: must not appear in multi-queue dispatch (LMQ) paths. */
    public const FILE_SEPARATOR = DIRECTORY_SEPARATOR;

    /** Corresponds to ``Validators.checkGroup``. */
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

    /** Corresponds to ``Validators.checkTopic``. */
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

    /** Corresponds to ``Validators.isSystemTopic``: throws immediately when the system topic is hit, returns normally otherwise. */
    public static function isSystemTopic(string $topic): void
    {
        if (TopicValidator::isSystemTopic($topic)) {
            throw new MQClientException(sprintf('The topic[%s] is conflict with system topic.', $topic));
        }
    }

    /** Corresponds to ``Validators.isNotAllowedSendTopic``. */
    public static function isNotAllowedSendTopic(string $topic): void
    {
        if (TopicValidator::isNotAllowedSendTopic($topic)) {
            throw new MQClientException(sprintf('Sending message to topic[%s] is forbidden.', $topic));
        }
    }

    /**
     * Corresponds to ``Validators.checkMessage(msg, producer)`` (order and wording copied verbatim).
     *
     * Code value conventions: null message / null body / zero body / oversize body /
     * INNER_MULTI_DISPATCH carry MESSAGE_ILLEGAL(13); forbidden-send topic, system topic,
     * checkTopic and checkGroup use pure client-side errors (responseCode is null).
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
        // A filesystem separator in the multi-queue dispatch (LMQ) path makes the broker side build an out-of-bounds path when creating queues
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


import path from 'node:path';
import { MQClientException } from '../remoting/exception.ts';
import {
  GROUP_MAX_LENGTH, TOPIC_MAX_LENGTH, VALID_CHAR_PATTERN, isTopicOrGroupIllegal,
  isNotAllowedSendTopic,
} from './topic_validator.ts';
import { Message } from './message.ts';

export const CHARACTER_MAX_LENGTH = 255;
export const CHARSET_ILLEGAL = '{}\n';
export const VALID_PATTERN = '^[%|a-zA-Z0-9_-]+$';

export const GROUP_NOT_EXIST = 'the specified group[%s] does not exist';
export const GROUP_ALREADY_EXIST = 'the specified group[%s] already exists';
export const CLIENT_ID_NOT_EXIST = 'the specified clientId[%s] does not exist';
export const TOPIC_NOT_EXIST = 'the specified topic[%s] does not exist';

// Group names reserved for broker-internal retry / dlq routing.
export const RESERVED_GROUP_PREFIXES = ['%RETRY%', '%DLQ%'];

function checkTopicNotAllowed(topic: string | null): void {
  if (topic != null && isNotAllowedSendTopic(topic)) {
    throw new MQClientException(
      `The topic[${topic}] is conflict with system reserved keywords.`);
  }
}

// Returns true when the group name contains illegal characters (consistent with topic rule).
export function isGroupIllegal(str: string | null): boolean {
  return isTopicOrGroupIllegal(str);
}

export const Validators = {
  CHARACTER_MAX_LENGTH,
  CHARSET_ILLEGAL,
  VALID_PATTERN,
  GROUP_NOT_EXIST,
  GROUP_ALREADY_EXIST,
  CLIENT_ID_NOT_EXIST,
  TOPIC_NOT_EXIST,
  RESERVED_GROUP_PREFIXES,

  isGroupIllegal,

  checkGroup(group: string | null): void {
    if (group == null || !String(group).trim()) {
      throw new MQClientException('the specified group is blank');
    }
    if (String(group).length > GROUP_MAX_LENGTH) {
      throw new MQClientException(
        `the specified group[${group}] is longer than group max length: ${GROUP_MAX_LENGTH}.`);
    }
    if (isTopicOrGroupIllegal(group)) {
      throw new MQClientException(
        `the specified group[${group}] contains illegal characters, allowing only ${VALID_CHAR_PATTERN}`);
    }
  },

  checkTopic(topic: string | null): void {
    if (topic == null || !String(topic).trim()) {
      throw new MQClientException('The specified topic is blank');
    }
    if (String(topic).length > TOPIC_MAX_LENGTH) {
      throw new MQClientException(
        `The specified topic is longer than topic max length ${TOPIC_MAX_LENGTH}.`);
    }
    if (isTopicOrGroupIllegal(topic)) {
      throw new MQClientException(
        `The specified topic[${topic}] contains illegal characters, allowing only ${VALID_CHAR_PATTERN}`);
    }
  },

  checkMessage(msg: Message | null, maxMessageSize: number): void {
    if (msg == null) {
      throw new MQClientException('the message is null');
    }
    const topic = msg.getTopic();
    this.checkTopic(topic);
    checkTopicNotAllowed(msg.getTopic());

    const body = msg.getBody();
    if (body == null) {
      throw new MQClientException('the message body is null');
    }
    if (body.length === 0) {
      throw new MQClientException('the message body length is zero');
    }
    if (body.length > maxMessageSize) {
      throw new MQClientException(
        `the message body size over max value, MAX: ${maxMessageSize}`);
    }

    const lmqPath = msg.getUserProperty
      ? msg.getUserProperty('INNER_MULTI_DISPATCH')
      : (msg.getProperty ? msg.getProperty('INNER_MULTI_DISPATCH') : null);
    if (lmqPath != null && String(lmqPath).length > 0
      && (String(lmqPath).includes('/') || String(lmqPath).includes(path.sep))) {
      throw new MQClientException(
        `INNER_MULTI_DISPATCH ${lmqPath} can not contains ${path.sep} character`);
    }
  },
};

export default Validators;

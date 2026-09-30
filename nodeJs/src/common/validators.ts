// -*- coding: utf-8 -*-
// Validators (org.apache.rocketmq.client.Validators).
// Faithful port of python/rocketmq/client/validators.py.
//
// checkGroup mirrors Python check_group: blank -> length (GROUP_MAX_LENGTH) -> illegal chars,
// throwing MQClientException. The character check reuses topic_validator's allow-set. The
// RESERVED_GROUP_PREFIXES / GROUP_NOT_EXIST / GROUP_ALREADY_EXIST constants follow Java Validators.

import { MQClientException } from '../remoting/exception.ts';
import { GROUP_MAX_LENGTH, VALID_CHAR_PATTERN, isTopicOrGroupIllegal } from './topic_validator.ts';

// Java Validators.CHARACTER_MAX_LENGTH (kept for parity even though unused here).
export const CHARACTER_MAX_LENGTH = 255;
// Java Validators.CHARSET_ILLEGAL / VALID_PATTERN.
export const CHARSET_ILLEGAL = '{}\n';
export const VALID_PATTERN = '^[%|a-zA-Z0-9_-]+$';

export const GROUP_NOT_EXIST = 'the specified group[%s] does not exist';
export const GROUP_ALREADY_EXIST = 'the specified group[%s] already exists';
export const CLIENT_ID_NOT_EXIST = 'the specified clientId[%s] does not exist';
export const TOPIC_NOT_EXIST = 'the specified topic[%s] does not exist';

// Group names reserved for broker-internal retry / dlq routing.
export const RESERVED_GROUP_PREFIXES = ['%RETRY%', '%DLQ%'];

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
};

export default Validators;

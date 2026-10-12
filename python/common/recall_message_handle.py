# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
from typing import Optional

from client.exception import MQClientException

SEPARATOR = " "
VERSION_1 = "v1"
INVALID_HANDLE = "recall handle is invalid"


class HandleV1:
    pass

    def __init__(self, topic: Optional[str] = None, broker_name: Optional[str] = None,
                 timestamp_str: Optional[str] = None, message_id: Optional[str] = None):
        self.topic = topic
        self.broker_name = broker_name
        self.timestamp_str = timestamp_str
        self.message_id = message_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HandleV1):
            return NotImplemented
        return (self.topic, self.broker_name, self.timestamp_str, self.message_id) == \
               (other.topic, other.broker_name, other.timestamp_str, other.message_id)

    def __repr__(self) -> str:
        return ("HandleV1(topic=%r, broker_name=%r, timestamp_str=%r, message_id=%r)"
                % (self.topic, self.broker_name, self.timestamp_str, self.message_id))


def build_handle(topic: str, broker_name: str, timestamp_str: str, message_id: str) -> str:
    pass
    raw = SEPARATOR.join([VERSION_1, topic, broker_name, timestamp_str, message_id])
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_handle(handle: str) -> HandleV1:
    pass
    if not handle:
        raise MQClientException(INVALID_HANDLE)
    padded = handle + "=" * (-len(handle) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded)
    except Exception:
        raise MQClientException(INVALID_HANDLE)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise MQClientException(INVALID_HANDLE)
    items = text.split(SEPARATOR)
    if len(items) < 5 or items[0] != VERSION_1:
        raise MQClientException(INVALID_HANDLE)
    return HandleV1(items[1], items[2], items[3], items[4])


from typing import Dict, List, Optional

from common.mix_all import MixAll

KEY_SEPARATOR = " "
#: 队列之间的分隔符
QUEUE_SEPARATOR = ";"
#: ``msgOffsetInfo`` 里同一队列多条 offset 的分隔符
OFFSET_SEPARATOR = ","

NORMAL_TOPIC = "0"
RETRY_TOPIC = "1"
RETRY_TOPIC_V2 = "2"
QUEUE_OFFSET = "qo"

POP_ORDER_REVIVE_QUEUE = 999

_POP_RETRY_SEPARATOR_V1 = "_"
_POP_RETRY_SEPARATOR_V2 = "+"


def build_pop_retry_topic_v1(topic: str, cid: str) -> str:
    pass
    return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + _POP_RETRY_SEPARATOR_V1 + topic


def build_pop_retry_topic_v2(topic: str, cid: str) -> str:
    pass
    return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + _POP_RETRY_SEPARATOR_V2 + topic


def build_pop_retry_topic(topic: str, cid: str, enable_retry_v2: bool = False) -> str:
    pass
    if enable_retry_v2:
        return build_pop_retry_topic_v2(topic, cid)
    return build_pop_retry_topic_v1(topic, cid)


def is_pop_retry_topic_v2(retry_topic: Optional[str]) -> bool:
    pass
    if not retry_topic:
        return False
    return retry_topic.startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX) and (
        _POP_RETRY_SEPARATOR_V2 in retry_topic
    )


def _split_drop_trailing(value: str, sep: str) -> List[str]:
    pass
    parts = value.split(sep)
    while parts and parts[-1] == "":
        parts.pop()
    return parts


def split(extra_info: Optional[str]) -> List[str]:
    pass
    if extra_info is None:
        raise ValueError("split extraInfo is null")
    return _split_drop_trailing(extra_info, KEY_SEPARATOR)


def _require(segments: Optional[List[str]], need: int, what: str) -> None:
    if segments is None or len(segments) < need:
        raise ValueError(
            "%s fail, extraInfoStrs length %d" % (what, 0 if segments is None else len(segments))
        )


def get_ck_queue_offset(segments: Optional[List[str]]) -> int:
    _require(segments, 1, "getCkQueueOffset")
    return int(segments[0])


def get_pop_time(segments: Optional[List[str]]) -> int:
    _require(segments, 2, "getPopTime")
    return int(segments[1])


def get_invisible_time(segments: Optional[List[str]]) -> int:
    _require(segments, 3, "getInvisibleTime")
    return int(segments[2])


def get_revive_qid(segments: Optional[List[str]]) -> int:
    _require(segments, 4, "getReviveQid")
    return int(segments[3])


def get_retry(segments: Optional[List[str]]) -> str:
    _require(segments, 5, "getRetry")
    return segments[4]


def get_broker_name(segments: Optional[List[str]]) -> str:
    _require(segments, 6, "getBrokerName")
    return segments[5]


def get_queue_id(segments: Optional[List[str]]) -> int:
    _require(segments, 7, "getQueueId")
    return int(segments[6])


def get_queue_offset(segments: Optional[List[str]]) -> int:
    _require(segments, 8, "getQueueOffset")
    return int(segments[7])


def retry_of_topic(topic: str) -> str:
    pass
    if is_pop_retry_topic_v2(topic):
        return RETRY_TOPIC_V2
    if topic.startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX):
        return RETRY_TOPIC
    return NORMAL_TOPIC


def build_extra_info(ck_queue_offset: int, pop_time: int, invisible_time: int,
                     revive_qid: int, topic: str, broker_name: str, queue_id: int,
                     msg_queue_offset: Optional[int] = None) -> str:
    pass
    parts = [
        str(ck_queue_offset),
        str(pop_time),
        str(invisible_time),
        str(revive_qid),
        retry_of_topic(topic),
        str(broker_name),
        str(queue_id),
    ]
    if msg_queue_offset is not None:
        parts.append(str(msg_queue_offset))
    return KEY_SEPARATOR.join(parts)


def parse_start_offset_info(start_offset_info: Optional[str]) -> Optional[Dict[str, int]]:
    """解析 ``startOffsetInfo``，形如 ``"0 3 0;0 2 0"``。

    key 是 ``"<retryFlag>@<queueId>"``，value 是该队列本次弹出的起始 offset。
    """
    if not start_offset_info:
        return None
    out: Dict[str, int] = {}
    segments = (
        [start_offset_info]
        if QUEUE_SEPARATOR not in start_offset_info
        else _split_drop_trailing(start_offset_info, QUEUE_SEPARATOR)
    )
    for one in segments:
        parts = one.split(KEY_SEPARATOR)
        if len(parts) != 3:
            raise ValueError("parse startOffsetInfo error, " + str(start_offset_info))
        key = parts[0] + "@" + parts[1]
        if key in out:
            raise ValueError("parse startOffsetInfo error, duplicate, " + str(start_offset_info))
        out[key] = int(parts[2])
    return out


def parse_msg_offset_info(msg_offset_info: Optional[str]) -> Optional[Dict[str, List[int]]]:
    """解析 ``msgOffsetInfo``，形如 ``"0 3 0,1,2;0 2 0"``。

    key 同 ``parse_start_offset_info``，value 是该队列本次弹出的各条消息 offset 列表。
    """
    if not msg_offset_info:
        return None
    out: Dict[str, List[int]] = {}
    segments = (
        [msg_offset_info]
        if QUEUE_SEPARATOR not in msg_offset_info
        else _split_drop_trailing(msg_offset_info, QUEUE_SEPARATOR)
    )
    for one in segments:
        parts = one.split(KEY_SEPARATOR)
        if len(parts) != 3:
            raise ValueError("parse msgOffsetInfo error, " + str(msg_offset_info))
        key = parts[0] + "@" + parts[1]
        if key in out:
            raise ValueError("parse msgOffsetInfo error, duplicate, " + str(msg_offset_info))
        out[key] = [int(x) for x in _split_drop_trailing(parts[2], OFFSET_SEPARATOR)]
    return out


def parse_order_count_info(order_count_info: Optional[str]) -> Optional[Dict[str, int]]:
    """解析 ``orderCountInfo``（顺序消费用），第三段是计数。"""
    if not order_count_info:
        return None
    out: Dict[str, int] = {}
    segments = (
        [order_count_info]
        if QUEUE_SEPARATOR not in order_count_info
        else _split_drop_trailing(order_count_info, QUEUE_SEPARATOR)
    )
    for one in segments:
        parts = one.split(KEY_SEPARATOR)
        if len(parts) != 3:
            raise ValueError("parse orderCountInfo error, " + str(order_count_info))
        key = parts[0] + "@" + parts[1]
        if key in out:
            raise ValueError("parse orderCountInfo error, duplicate, " + str(order_count_info))
        out[key] = int(parts[2])
    return out


def get_start_offset_info_map_key(topic: str, key) -> str:
    return retry_of_topic(topic) + "@" + str(key)


def get_queue_offset_key_value_key(queue_id, queue_offset) -> str:
    return QUEUE_OFFSET + str(queue_id) + "%" + str(queue_offset)


def get_queue_offset_map_key(topic: str, queue_id, queue_offset) -> str:
    return retry_of_topic(topic) + "@" + get_queue_offset_key_value_key(queue_id, queue_offset)


def is_order(segments: List[str]) -> bool:
    pass
    return get_revive_qid(segments) == POP_ORDER_REVIVE_QUEUE


def get_real_topic(topic: str, cid: str, retry: str) -> str:
    pass
    if retry == NORMAL_TOPIC:
        return topic
    if retry == RETRY_TOPIC:
        return build_pop_retry_topic_v1(topic, cid)
    if retry == RETRY_TOPIC_V2:
        return build_pop_retry_topic_v2(topic, cid)
    raise ValueError("getRetry fail, format is wrong")

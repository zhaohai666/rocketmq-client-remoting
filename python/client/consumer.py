# -*- coding: utf-8 -*-
from __future__ import annotations

import bisect
import hashlib
import json
import os
import queue
import re
import threading
import time
from collections import deque
from typing import Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

from common.message import Message, MessageExt, MessageQueue
from common.message_accessor import MessageAccessor
from common.message_const import MessageConst
from common.mix_all import MixAll
from common.subscription_data import ExpressionType, FilterAPI, SubscriptionData
from common.sysflag import MessageSysFlag, PullSysFlag
from rocketmq_logging import get_logger
from remoting.exception import (RemotingException, RemotingTimeoutException,
                                RemotingTooMuchRequestException)
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.heartbeat import (ConsumeFromWhere, ConsumeType,
                                           ConsumerData, HeartbeatData, MessageModel)
from remoting.protocol.namespace_util import NamespaceUtil
from remoting.protocol.body import (CMResult, ConsumeMessageDirectlyResult,
                                       ConsumeStatus, ConsumerRunningInfo,
                                       ProcessQueueInfo)
from remoting.protocol import extra_info as extra_info_util
from remoting.protocol.extra_info import split
from remoting.rpchook import RPCHook
from remoting.socks_proxy import SocksProxyConfig
from . import validators
from .consume_executor import ConsumeExecutor, RejectedExecutionError
from .consumer_result import (ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus,
                              ConsumeOrderlyContext, ConsumeOrderlyStatus,
                              ConsumeReturnType, consume_status_name,
                              MessageListener, MessageListenerConcurrently,
                              MessageListenerOrderly, PopResult, PopStatus,
                              PullResult, PullStatus)
from .consumer_stats import ConsumerStatsManager
from .exception import MQBrokerException, MQClientException
from .hook import (ConsumeMessageContext, ConsumeMessageHook,
                   FilterMessageContext, FilterMessageHook)
from .mq_client import MQClientInstance
from .mq_client_manager import MQClientManager
from .top_addressing import DefaultTopAddressing
from .trace import AccessChannel
from .trace_dispatcher import AsyncTraceDispatcher, TraceDispatcherType
from .trace_hook import ConsumeMessageTraceHook

logger = get_logger()

# POP consumption failure delay gradient (seconds): increases step by step with retry count, up to 2 hours max.
POP_DELAY_LEVEL = (10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600,
                   1200, 1800, 3600, 7200)

# Valid range for POP invisibleTime (ms): anything out of range falls back to 60000.
MIN_POP_INVISIBLE_TIME = 5000
MAX_POP_INVISIBLE_TIME = 300000

# Broker-side initial offset mode for POP: 0=start from min offset, 1=start from max offset.
CONSUME_INIT_MODE_MIN = 0
CONSUME_INIT_MODE_MAX = 1

# Upper bound of a 32-bit signed int: the default value for the orderly-consumption "retry count exhausted" criterion
# (config -1 is read as this on this path, meaning "no upper limit").
_INT32_MAX = 0x7FFFFFFF

# All valid return values for orderly consumption: SUCCESS/ROLLBACK/COMMIT/suspend.
# The listener may only return one of these four: null is treated as suspend, and an unknown value under
# dynamic typing must take the same fallback, otherwise it would be silently acked as SUCCESS.
_ORDERLY_STATUSES = (ConsumeOrderlyStatus.SUCCESS, ConsumeOrderlyStatus.ROLLBACK,
                     ConsumeOrderlyStatus.COMMIT,
                     ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT)

# Pull idle upper bound (``PULL_MAX_IDLE_TIME``, 120s = 2 minutes by default):
# if a queue still owned by this instance hasn't started any pull/pop for longer than this, its loop is
# dead (or stuck), and rebalance removes and rebuilds it by this criterion.
PULL_MAX_IDLE_TIME = 120.0

# Per-round backoff while suspended (default 1000ms):
# after suspend(), the pull/pop loop does not exit; it just backs off this long each round before checking the gate again.
PULL_TIME_DELAY_WHEN_SUSPEND = 1.0

# Per-round backoff when the **broker** answers FLOW_CONTROL(215) to a pull/pop (default 20ms):
# broker-side throttling is transient, so the retry stays short. This is deliberately distinct
# from the pause taken when the client's own receive-buffer thresholds hit (the _flow_control_hit
# branch); sharing one delay there would either hammer a throttled broker or stall a healthy queue.
PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL = 0.02

# Per-round backoff for any other broker/client error on the pull/pop path (default 500ms):
# long enough to avoid a hot retry loop, short enough to recover quickly.
PULL_TIME_DELAY_WHEN_EXCEPTION = 0.5


def _mq_sort_key(mq: MessageQueue):
    """Queue sort key, comparison semantics: topic -> brokerName -> queueId.

    Allocation sorts mq_all/cid_all before deciding; inconsistent ordering makes different instances compute
    different allocation results (the same queue consumed by two instances at once).
    """
    return (mq.topic, mq.broker_name, mq.queue_id)


def _safe_hook_name(hook) -> str:
    try:
        return str(hook.hook_name())
    except Exception:  # noqa: BLE001
        return type(hook).__name__


def client_side_tag_filter(sub: Optional[SubscriptionData],
                           msgs: List[MessageExt]) -> List[MessageExt]:
    """Client-side second-pass tag filtering.

    The broker filters by the tag's **hash (codeSet)**, which can have collisions and let extra messages
    through, so the client must verify again by string. The guard `!tagsSet.isEmpty() && !isClassFilterMode`
    means: when subscribing `"*"` (SUB_ALL) no filtering applies -- so `FilterAPI.build_subscription_data`
    must keep tags_set empty for SUB_ALL (detailed comment there).
    """
    if not msgs or sub is None or not sub.tags_set or sub.class_filter_mode:
        return msgs
    return [m for m in msgs if m.get_tags() is not None and m.get_tags() in sub.tags_set]


def execute_filter_hooks(hook_list: List["FilterMessageHook"], context: FilterMessageContext) -> None:
    """Execute the filter hooks in order; **all exceptions are swallowed** and logged as errors.

    Unlike send/consume hooks: a failing filter hook must not affect consumption, it only means that particular filtering does not apply.
    """
    for hook in hook_list:
        try:
            hook.filter_message(context)
        except Exception as e:  # noqa: BLE001
            logger.error("execute hook error. hookName=%s: %s", _safe_hook_name(hook), e)


def filter_messages_for_delivery(consumer_group: str, hook_list: List["FilterMessageHook"],
                                 mq: MessageQueue, sub: Optional[SubscriptionData],
                                 msgs: List[MessageExt],
                                 unit_mode: bool = False) -> List[MessageExt]:
    """Pre-delivery filtering = client-side second-pass tag filtering + FilterMessageHook (shared by pull/POP paths).

    The hook receives the **mutable** ``msg_list``; how dropped messages are handled is decided by the caller:
    pull path = silently skipped (the offset still advances, no ack);
    POP path = must be acked immediately, otherwise the message is revived and redelivered once invisibleTime expires.
    """
    out = client_side_tag_filter(sub, list(msgs))
    if out and hook_list:
        context = FilterMessageContext(consumer_group, out, mq)
        context.unit_mode = unit_mode
        execute_filter_hooks(hook_list, context)
        out = list(context.msg_list)
    return out


class MessageSelector:
    """Message selector: filters by tag or SQL92 expression."""

    def __init__(self, selector_type: str = ExpressionType.TAG, expression: str = "*"):
        self.type = selector_type
        self.expression = expression

    @staticmethod
    def by_tag(tag: str) -> "MessageSelector":
        return MessageSelector(ExpressionType.TAG, tag)

    @staticmethod
    def by_sql(sql: str) -> "MessageSelector":
        return MessageSelector(ExpressionType.SQL92, sql)


class MessageQueueListener:
    """Queue change listener: receives the set of queues assigned to this instance when the allocation result changes."""

    def message_queue_changed(self, topic: str, mq_all: Set[MessageQueue],
                              mq_divided: Set[MessageQueue]) -> None:
        raise NotImplementedError


class TopicMessageQueueChangeListener:
    """Topic queue set change listener.

    Callback signature ``on_changed(topic, message_queues)``: after the queue count is **scaled up/down**,
    the callback fires only if the set changed relative to the previous snapshot, triggered by a periodic background comparison.
    """

    def on_changed(self, topic: str, message_queues: Set[MessageQueue]) -> None:
        raise NotImplementedError


class AllocateMessageQueueStrategy:
    """Queue allocation strategy interface.

    Contract: on invalid input, **always return an empty list** instead of raising -- rebalance is a periodic background task,
    and one bad input should not take the consumer down.
    """

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        raise NotImplementedError

    def get_name(self) -> str:
        """Algorithm name (AVG, etc.), used for logging and operational identification."""
        raise NotImplementedError


def _strategy_name(strategy: AllocateMessageQueueStrategy) -> str:
    """Get the strategy name for logging.

    Callers may duck-type in a custom strategy object that only has ``allocate``,
    so when ``get_name`` is missing this degrades to the class name instead of raising AttributeError.
    """
    getter = getattr(strategy, "get_name", None)
    if callable(getter):
        try:
            return str(getter())
        except Exception:  # noqa: BLE001 - logging a value must not affect rebalance
            pass
    return strategy.__class__.__name__


class AllocateMessageQueueAveragely(AllocateMessageQueueStrategy):
    """Average allocation: splits the queues into consecutive segments, one segment per consumer."""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        # Guard order: current_cid -> mq_all -> cid_all. All three cases only return an empty list without raising
        # (rebalance is a periodic background task, bad input should not take the consumer down);
        # when current_cid is not in cid_all, leave a [BUG] info log.
        if not current_cid or not mq_all or not cid_all:
            return []
        if current_cid not in cid_all:
            logger.info("[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s",
                        consumer_group, current_cid, cid_all)
            return []
        index = cid_all.index(current_cid)
        mod = len(mq_all) % len(cid_all)
        average_size = len(mq_all) // len(cid_all)
        if average_size == 0:
            return [mq_all[index]] if index < len(mq_all) else []
        start_index = 0
        end_index = 0
        result = []
        if mod > 0 and index < mod:
            start_index = index * (average_size + 1)
            end_index = start_index + average_size + 1
        else:
            start_index = mod * (average_size + 1) + (index - mod) * average_size
            end_index = start_index + average_size
        return mq_all[start_index:end_index]

    def get_name(self) -> str:
        return "AVG"


class AllocateMessageQueueAveragelyByCircle(AllocateMessageQueueStrategy):
    """Round-robin average allocation: takes queues by consumer index, one per ``len(cid_all)`` step."""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        # Same guard as Averagely, also only returns an empty list.
        if not current_cid or not mq_all or not cid_all:
            return []
        if current_cid not in cid_all:
            logger.info("[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s",
                        consumer_group, current_cid, cid_all)
            return []
        index = cid_all.index(current_cid)
        result = []
        for i in range(index, len(mq_all), len(cid_all)):
            result.append(mq_all[i])
        return result

    def get_name(self) -> str:
        return "AVG_BY_CIRCLE"


class AllocateMessageQueueByConfig(AllocateMessageQueueStrategy):
    """Allocation by explicit config: ignores mq_all/cid_all and directly returns the configured queue list.

    __init__ normalizes None into an empty list, and allocate returns a copy of the list so callers cannot mutate internal state.
    ``allocate`` does **not** validate input, so an empty group / empty cid_all still returns the configured value.
    """

    def __init__(self, message_queue_list: Optional[List[MessageQueue]] = None):
        self.message_queue_list = list(message_queue_list) if message_queue_list else []

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        return list(self.message_queue_list)

    def get_name(self) -> str:
        return "CONFIG"


# ------------------------------------------------------ Consistent hash ring
#
# Only AllocateMessageQueueConsistentHash uses it, but the whole thing is implemented in a fixed form rather than as an "equivalent rewrite":
# the ring positions are jointly determined by three details -- how MD5 takes bytes, virtual node naming, and whether the ring's range lookup includes its endpoints --
# any difference would move every queue to a different owner, which shows up as duplicate/missed consumption when mixed with other consistent-hash clients.


class HashFunction:
    """Hash function interface: maps a key on the ring to an integer. Use it to inject a custom hash strategy."""

    def hash(self, key: str) -> int:
        raise NotImplementedError


class MD5Hash(HashFunction):
    """Default hash: the **first 4 bytes** of the MD5 digest concatenated big-endian into an integer.

    WARNING: only the first 4 bytes are taken (not the full 128 bits); any other way of taking them yields a different ring.
    """

    def hash(self, key: str) -> int:
        digest = hashlib.md5(key.encode("utf-8")).digest()
        value = 0
        for i in range(4):
            value = (value << 8) | digest[i]
        return value


class Node:
    """An addressable node on the ring, physical or virtual; `get_key` provides the string to be hashed."""

    def get_key(self) -> str:
        raise NotImplementedError


class ClientNode(Node):
    """Physical node of the consistent hash: the key is the clientId."""

    def __init__(self, client_id: str):
        self.client_id = client_id

    def get_key(self) -> str:
        return self.client_id


class VirtualNode(Node):
    """Virtual node: key = physical node key + "-" + replica index."""

    def __init__(self, physical_node: Node, replica_index: int):
        self.physical_node = physical_node
        self.replica_index = replica_index

    def get_key(self) -> str:
        return "%s-%d" % (self.physical_node.get_key(), self.replica_index)

    def is_virtual_node_of(self, p_node: Node) -> bool:
        return self.physical_node.get_key() == p_node.get_key()

    def get_physical_node(self) -> Node:
        return self.physical_node


class ConsistentHashRouter:
    """Consistent hash router: hashes nodes into a ring and routes to the nearest physical node clockwise.

    Implemented with "an ascending key list + dict". The lookup semantics are "take the first key >= hash" (endpoint included,
    an equal hash belongs to itself), hence `bisect_left`; when the ring is empty or past the end, it wraps around to the first node.
    """

    def __init__(self, p_nodes: Optional[List[Node]] = None, v_node_count: int = 0,
                 hash_function: Optional[HashFunction] = None):
        if hash_function is None:
            hash_function = MD5Hash()
        self.hash_function = hash_function
        self._ring: Dict[int, VirtualNode] = {}
        self._keys: List[int] = []
        if p_nodes is not None:
            for p_node in p_nodes:
                self.add_node(p_node, v_node_count)

    def add_node(self, p_node: Node, v_node_count: int) -> None:
        # Existing replicas must continue the numbering, otherwise the v virtual nodes of the same
        # physical node would all land on the same hash (when a node is added twice, i + the replica count already on the ring distinguishes them).
        if v_node_count < 0:
            raise ValueError("illegal virtual node counts :%d" % v_node_count)
        existing_replicas = self.get_existing_replicas(p_node)
        for i in range(v_node_count):
            v_node = VirtualNode(p_node, i + existing_replicas)
            key = self.hash_function.hash(v_node.get_key())
            if key not in self._ring:
                bisect.insort(self._keys, key)
            # On the same hash the later one overwrites; the ring position stays unchanged
            self._ring[key] = v_node

    def remove_node(self, p_node: Node) -> None:
        for key in [k for k in self._keys
                    if self._ring[k].is_virtual_node_of(p_node)]:
            self._keys.remove(key)
            del self._ring[key]

    def route_node(self, object_key: str) -> Optional[Node]:
        if not self._ring:
            return None
        index = bisect.bisect_left(self._keys, self.hash_function.hash(object_key))
        if index == len(self._keys):
            index = 0  # Past the end of the ring -> wrap around to the first
        return self._ring[self._keys[index]].get_physical_node()

    def get_existing_replicas(self, p_node: Node) -> int:
        return sum(1 for v_node in self._ring.values() if v_node.is_virtual_node_of(p_node))


def _message_queue_ring_key(mq: MessageQueue) -> str:
    """The ring key of the consistent hash (**it** is hashed, not the repr).

    WARNING: must equal `"MessageQueue [topic=.., brokerName=.., queueId=..]"` character for character:
    Python's own `__repr__` is a different format; hashing with it yields a completely different ring,
    moving every queue to a different owner. Changing this format breaks compatibility.
    """
    return "MessageQueue [topic=%s, brokerName=%s, queueId=%d]" % (
        mq.topic, mq.broker_name, mq.queue_id)


def _split_dropping_trailing_empty(text: str, sep: str) -> List[str]:
    """Splits by the separator and **drops trailing empty segments**.

    Python's `str.split` keeps trailing empty segments, and the two splits differ in the result for broker names, while
    `AllocateMessageQueueByMachineRoom` judges validity exactly by `len(...) == 2`, so the difference directly changes
    whether a queue participates in the allocation:

    | Input | This function | Python native `str.split` |
    |---|---|---|
    | `"room1@"` | `["room1"]` (1 segment, rejected) | `["room1", ""]` (2 segments, wrongly accepted) |
    | `"room1@b@"` | `["room1", "b"]` (2 segments, accepted) | `["room1", "b", ""]` (3 segments, wrongly rejected) |
    | `"@"` | `[]` | `["", ""]` |
    | `""` / `"broker-a"` | When no separator matches, the **whole string is returned as-is**, even if empty | Same |

    The early return on the last line must not be dropped: on no match, trailing empties must not be trimmed unconditionally (otherwise `""` would become `[]`).
    """
    parts = text.split(sep)
    if len(parts) == 1:
        return parts  # No separator matched: return the whole string as-is
    while parts and parts[-1] == "":
        parts.pop()
    return parts


class AllocateMessageQueueConsistentHash(AllocateMessageQueueStrategy):
    """Consistent-hash allocation, `get_name()` is `CONSISTENT_HASH`.

    The difference from AVG / AVG_BY_CIRCLE is not "evenness of the split" but **stability**: when the number of queues or consumers changes,
    only the queues whose hash falls on the arc between the added/removed nodes move to another owner, whereas AVG shifts everyone's boundaries wholesale.

    Same guard policy as the other strategies: invalid input always returns an empty list.
    The only remaining raise is `virtual_node_cnt < 0` in the constructor (an explicit config error, not on the rebalance background path).
    """

    def __init__(self, virtual_node_cnt: int = 10,
                 custom_hash_function: Optional[HashFunction] = None):
        # Default 10 virtual nodes, default MD5Hash
        if virtual_node_cnt < 0:
            raise ValueError("illegal virtualNodeCnt :%d" % virtual_node_cnt)
        self.virtual_node_cnt = virtual_node_cnt
        self.custom_hash_function = custom_hash_function

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        if not current_cid or not mq_all or not cid_all:
            return []
        if current_cid not in cid_all:
            logger.info("[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s",
                        consumer_group, current_cid, cid_all)
            return []
        cid_nodes = [ClientNode(cid) for cid in cid_all]
        if self.custom_hash_function is not None:
            router = ConsistentHashRouter(cid_nodes, self.virtual_node_cnt,
                                          self.custom_hash_function)
        else:
            router = ConsistentHashRouter(cid_nodes, self.virtual_node_cnt)
        result = []
        for mq in mq_all:
            node = router.route_node(_message_queue_ring_key(mq))
            if node is not None and node.get_key() == current_cid:
                result.append(mq)
        return result

    def get_name(self) -> str:
        return "CONSISTENT_HASH"


class AllocateMessageQueueByMachineRoom(AllocateMessageQueueStrategy):
    """Allocation by machine room, `get_name()` is `MACHINE_ROOM` (scenario: multi logical machine room deployment).

    Convention: broker names are written as `<room>@<brokerName>`, only queues whose prefix falls in `consumeridcs` participate
    in the allocation, and then an "average allocation" runs among those queues (the sharding algorithm is line-by-line identical to AVG, but the ownership criterion for rem
    is `rem > currentIndex`, i.e. the remainder queues go to the first rem consumers).

    WARNING: broker names are split via [`_split_dropping_trailing_empty`], not Python native `split`:
    under the trailing-trimming split, `"room1@"` is 1 segment (does not participate) and `"room1@b@"` is 2 segments (participates).

    WARNING: `consumeridcs` defaults to an empty set, which behaves as "allocate nothing", consistent with the
    allocation strategy's usual "guard returns an empty result" policy.
    """

    def __init__(self, consumeridcs: Optional[Set[str]] = None):
        self.consumeridcs: Set[str] = set(consumeridcs) if consumeridcs else set()

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        if not current_cid or not mq_all or not cid_all:
            return []
        if current_cid not in cid_all:
            logger.info("[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s",
                        consumer_group, current_cid, cid_all)
            return []
        current_index = cid_all.index(current_cid)
        if current_index < 0:
            return []
        premq_all = []
        for mq in mq_all:
            temp = _split_dropping_trailing_empty(mq.broker_name, "@")
            if len(temp) == 2 and temp[0] in self.consumeridcs:
                premq_all.append(mq)
        mod = len(premq_all) // len(cid_all)
        rem = len(premq_all) % len(cid_all)
        start_index = mod * current_index
        end_index = start_index + mod
        result = list(premq_all[start_index:end_index])
        if rem > current_index:
            result.append(premq_all[current_index + mod * len(cid_all)])
        return result

    def get_name(self) -> str:
        return "MACHINE_ROOM"

    def get_consumeridcs(self) -> Set[str]:
        return self.consumeridcs

    def set_consumeridcs(self, consumeridcs: Set[str]) -> None:
        self.consumeridcs = consumeridcs


class MachineRoomResolver:
    """Machine room resolver: tells the strategy "who is in which room".

    Neither method **may return None** (otherwise that room is treated as empty and its queues are taken away).
    """

    def broker_deploy_in(self, message_queue: MessageQueue) -> str:
        raise NotImplementedError

    def consumer_deploy_in(self, client_id: str) -> str:
        raise NotImplementedError


class AllocateMachineRoomNearby(AllocateMessageQueueStrategy):
    """Machine room nearby allocation: a layer of room grouping on top of the inner strategy.

    Proxy mode: first group the queues and the consumers by room,
    1. queues in this consumer's room are only allocated to consumers in the **same room** (using the inner strategy);
    2. queues in rooms that have **no live consumer** are shared by all consumers via the inner strategy --
       otherwise nobody would consume them.

    `get_name()` is `"MACHINE_ROOM_NEARBY" + "-" + inner strategy name`, because the logs
    must show which allocation algorithm is actually in use.

    Both missing constructor arguments, and an empty room from the resolver, **raise** (`ValueError`):
    silently returning an empty list would take away the queues of the whole topic, whereas rebalance catching the exception preserves the existing allocation.
    """

    def __init__(self, allocate_message_queue_strategy: AllocateMessageQueueStrategy,
                 machine_room_resolver: MachineRoomResolver):
        if allocate_message_queue_strategy is None:
            raise ValueError("allocateMessageQueueStrategy is null")
        if machine_room_resolver is None:
            raise ValueError("machineRoomResolver is null")
        self.allocate_message_queue_strategy = allocate_message_queue_strategy
        self.machine_room_resolver = machine_room_resolver

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        if not current_cid or not mq_all or not cid_all:
            return []
        if current_cid not in cid_all:
            logger.info("[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %s",
                        consumer_group, current_cid, cid_all)
            return []

        # When grouping by room, iterate in **lexicographic order** of room names, otherwise the processing order
        # of same-named rooms would follow insertion order (the result set is a union, and the order also enters logs/assertions).
        mr_2_mq: Dict[str, List[MessageQueue]] = {}
        for mq in mq_all:
            room = self.machine_room_resolver.broker_deploy_in(mq)
            if room:
                mr_2_mq.setdefault(room, []).append(mq)
            else:
                raise ValueError("Machine room is null for mq %s"
                                 % _message_queue_ring_key(mq))
        mr_2_c: Dict[str, List[str]] = {}
        for cid in cid_all:
            room = self.machine_room_resolver.consumer_deploy_in(cid)
            if room:
                mr_2_c.setdefault(room, []).append(cid)
            else:
                raise ValueError("Machine room is null for consumer id %s" % cid)

        allocate_results: List[MessageQueue] = []
        # 1. Queues in this consumer's room: only distributed among consumers in the same room
        current_machine_room = self.machine_room_resolver.consumer_deploy_in(current_cid)
        mq_in_this_machine_room = mr_2_mq.pop(current_machine_room, None)
        consumer_in_this_machine_room = mr_2_c.get(current_machine_room)
        if mq_in_this_machine_room:
            allocate_results += self.allocate_message_queue_strategy.allocate(
                consumer_group, current_cid, mq_in_this_machine_room,
                consumer_in_this_machine_room)
        # 2. Rooms with no live consumer: queues must not be left unconsumed, hand them to all consumers
        for room in sorted(mr_2_mq):
            if room not in mr_2_c:
                allocate_results += self.allocate_message_queue_strategy.allocate(
                    consumer_group, current_cid, mr_2_mq[room], cid_all)
        return allocate_results

    def get_name(self) -> str:
        return "MACHINE_ROOM_NEARBY-%s" % _strategy_name(
            self.allocate_message_queue_strategy)


# ---------------- Local offset file format ----------------
# The on-disk `offsetTable` writes the MessageQueue object **directly as a JSON key** -- strictly invalid JSON,
# but this client produces and consumes it itself and can read it back. The field order is fixed as brokerName/queueId/topic (alphabetical).
# Queue keys are still concatenated as topic+broker+queueId. Changing the on-disk format would make historical offset files unreadable.
def _build_local_offsets_json(items: Dict[str, int],
                              mq_map: Dict[str, MessageQueue]) -> str:
    parts = []
    for key, off in items.items():
        mq = mq_map.get(key)
        if mq is None:
            # Only write entries that still carry queue info
            continue
        obj = json.dumps({"brokerName": mq.broker_name, "queueId": int(mq.queue_id),
                          "topic": mq.topic}, separators=(",", ":"))
        parts.append(obj + ":" + str(int(off)))
    return '{"offsetTable":{' + ",".join(parts) + "}}"


def _parse_local_offsets_json(text: str) -> Optional[Dict[str, int]]:
    if not text or not text.strip():
        return None
    stripped = text.strip()
    # First parse as strict JSON (the flat map written by historical versions); the "object-as-key" format
    # always fails strict parsing (including the pretty version) and falls through to the tolerant scanner below.
    try:
        return {k: int(v) for k, v in json.loads(stripped).items()}
    except (ValueError, AttributeError):
        pass
    body = _scan_offset_table_body(stripped)
    if body is None:
        return None
    return {topic + broker + str(qid): off
            for (topic, broker, qid), off in body}


def _scan_offset_table_body(text: str) -> Optional[list]:
    """Tolerantly parse {"offsetTable":{{..}:off,{..}:off}}; returns None on failure."""
    m = re.match(r'^\{\s*"offsetTable"\s*:\s*\{(.*)\}\s*\}\s*$', text, re.S)
    if not m:
        return None
    body = m.group(1).strip()
    entries = []
    if not body:
        return entries
    pos = 0
    for mo in _OBJ_AS_KEY_RE.finditer(body):
        if body[pos:mo.start()].strip() not in ("", ","):
            return None
        fields = {}
        for fo in _FIELD_RE.finditer(mo.group(1)):
            raw = fo.group(2)
            if raw.startswith('"'):
                try:
                    fields[fo.group(1)] = json.loads(raw)
                except ValueError:
                    return None
            else:
                fields[fo.group(1)] = int(raw)
        if not {"topic", "brokerName", "queueId"} <= fields.keys():
            return None
        entries.append(((fields["topic"], fields["brokerName"],
                         int(fields["queueId"])), int(mo.group(2))))
        pos = mo.end()
    if body[pos:].strip():
        return None
    return entries


_OBJ_AS_KEY_RE = re.compile(r'(\{[^{}]*\})\s*:\s*(-?\d+)')
_FIELD_RE = re.compile(r'"(\w+)"\s*:\s*("(?:[^"\\]|\\.)*"|-?\d+)')


class PopProcessQueue:
    """Queue state in POP mode.

    Unlike pull mode's per-queue buffer, POP **has no "pulled but not consumed" buffer**: once a message is popped it goes to the
    consume thread, and confirmation is by ack. Only two things are tracked here:

    - ``wait_ack_counter``: the count of popped messages not yet acked / with unextended invisibility, used for flow control;
    - ``dropped``: whether the queue has already been taken away by rebalance (once dropped, this batch is neither consumed nor acked,
      leaving it to the broker to automatically revive and redeliver once invisibleTime expires).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._wait_ack_counter = 0
        self._dropped = False
        self.last_pop_timestamp = time.time()

    def inc_found_msg(self, count: int) -> None:
        with self._lock:
            self._wait_ack_counter += count

    def dec_found_msg(self, count: int) -> None:
        """The input is a **signed delta** (the caller passes `-len(msgs)`); only addition happens here."""
        with self._lock:
            self._wait_ack_counter += count

    def ack(self) -> int:
        with self._lock:
            self._wait_ack_counter -= 1
            return self._wait_ack_counter

    def wait_ack_count(self) -> int:
        with self._lock:
            return self._wait_ack_counter

    def is_dropped(self) -> bool:
        return self._dropped

    def set_dropped(self, dropped: bool) -> None:
        self._dropped = dropped


class DefaultMQPushConsumer:
    """Push-mode consumer: subscription table + background pull/POP + listener callbacks."""

    # ---------------- POP mode (5.x lightweight consumption) config ----------------
    #
    # POP has two sources of queues: broker-side allocation (QUERY_ASSIGNMENT(400) returns
    # a POP-mode assignment list, no client-side rebalance), or client rebalance.
    # This port uses **client rebalance** (see _rebalance_pull_threads): queues are computed locally per the allocation
    # strategy, then each queue is POPped independently -- one POP loop per queue + ack confirmation.
    # The only difference is "who decides which queues are allocated"; read this comment block before changing it.

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING, **kwargs):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        # TLS switch (None = defer to the env ROCKETMQ_TLS_ENABLE)
        self.tls_enable: Optional[bool] = kwargs.pop("tls_enable", None)
        # TLS details (caCert = strict CA validation; clientCert/clientKey = mTLS; serverName = hostname override)
        self.tls_options: Optional[Dict[str, str]] = kwargs.pop("tls_options", None)
        self.socks_proxy_config: Optional[SocksProxyConfig] = kwargs.pop("socks_proxy_config", None)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id: Optional[str] = None
        # ---- unit / stream config ----
        # unitName takes part in the clientId's `@<unitName>` suffix and the dynamic addressing URL; unitMode is
        # reported to the broker along with the ConsumerData heartbeat, message redelivery request header and filter hooks;
        # ``enable_stream_request_type`` both appends the `@STREAM` suffix to clientId (to
        # "prevent unexpected reuses of MQClientInstance") and adds
        # the `ReqT=0` extension field to every request. Like the producer, push consumers default to off.
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = False
        # namespaceV2 (5.x server-side namespace): when non-empty, every request carries
        # nsd=true / ns=<value>, see the NamespaceRpcHook in remoting/rpchook.py.
        self.namespace_v2 = ""
        self.message_model = message_model
        self.consume_from_where = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        self.consume_timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time() - 30 * 60))
        # ---- Consume thread pool ----
        # Default min=20 **and** max=20 -- both sides are 20 (the early min=20/max=64 was the previous generation's default).
        # The **pull** path is "one pull thread per queue" (not a shared thread pool); only the **POP**
        # path really creates a thread pool.
        # Both values are simultaneously "declared values": consume_thread_max is both the POP thread pool's max and the
        # upper bound of update_core_pool_size (the guard is n < consume_thread_max).
        # Because the thread pool uses an **unbounded** queue (real concurrency == core), under the default config core can only go **down**
        # (n < 20) -- this is pre-existing thread-pool model behavior, not an extra restriction.
        self.consume_thread_min = 20
        self.consume_thread_max = 20
        # Auto-elastic threshold (when the backlog count exceeds it, concurrency adjustment is attempted; default 100000).
        # WARNING: the automatic inc/dec is a **no-op**, see adjust_thread_pool():
        # the computation and config are kept for observability, do not "fix it while you are at it".
        self.adjust_thread_pool_nums_threshold = 100000
        # Declarative core pool size, default = consume_thread_min
        self._core_pool_size = self.consume_thread_min
        # Whether the executor is self-built: this implementation does not support externally injected executors, so it is always True
        # -- the guard of update_core_pool_size therefore always holds.
        self._owns_consume_executor = True
        # Per-queue "accumulated message count" (by queue key): the backlog count computed by the most recent pull
        # = the MAX_OFFSET property on the message - that message's queueOffset (updated only when > 0)
        self._msg_acc_cnt_table: Dict[str, int] = {}
        self.consume_concurrently_max_span = 2000
        self.pull_threshold_for_queue = 1000
        self.pull_threshold_size_for_queue = 100
        self.pull_threshold_for_topic = -1
        self.pull_threshold_size_for_topic = -1
        self.pull_interval = 0
        self.pull_timeout_millis = 30000
        # The suspend duration of long polling (the broker's own max suspend time defaults to 20000).
        # Note: this property must be initialized here -- the pull loop reads it, and if missing it throws
        # AttributeError every round and is swallowed by the fallback except of _consume_loop, presenting as "the consumer receives nothing at all".
        self.pull_suspend_timeout_millis = 20000
        self.consume_message_batch_max_size = 1
        self.pull_batch_size = 32
        self.pull_batch_size_in_bytes = 256 * 1024
        # Default false:
        # when true, every pull puts the subscription expression into the request header's subscription field (broker-side filtering).
        # Not sent by default -- the broker does not filter, the client's tag filtering is the fallback.
        self.post_subscription_when_pull = False
        self.max_reconsume_times = -1
        self.suspend_current_queue_time_millis = 1000
        self.consume_timeout = 15
        self.client_rebalance = True
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self.allocate_strategy: AllocateMessageQueueStrategy = AllocateMessageQueueAveragely()
        # Subscription table topic -> SubscriptionData
        self.subscription_data: Dict[str, SubscriptionData] = {}
        self.message_listener: Optional[MessageListener] = None
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._pulling = False
        self._lock = threading.RLock()
        self._consume_threads: List[threading.Thread] = []
        # In-flight batch count (the part in the buffer **currently being consumed by the listener**):
        # +1 when _dispatch_loop takes a batch, -1 at consumption completion (including exception re-enqueue).
        # _pending together with it is the "queue-held message count"; _correct_tags_offset_locked's gate watches both.
        self._msg_queue_inflight: Dict[str, int] = {}
        # The **message bodies** of in-flight batches (key -> list, registered when a batch is taken, removed at consumption completion).
        # Corresponds to the part "dispatched to the listener but not yet acked":
        # _pending (pulled but not dispatched) union _inflight_msgs (dispatched but not settled) = the full set of queue-held messages.
        # _clean_expired_queue's "only look at the head (minimum offset)" criterion takes the first entry of this union.
        self._inflight_msgs: Dict[str, List[MessageExt]] = {}
        self._offset_table: Dict[str, int] = {}
        self._stop = threading.Event()
        # ---- Consume progress / buffer / lock state ----
        # _offset_table is the "pull cursor" (nextBeginOffset); _consume_offsets is
        # "consumed offset" (the commitOffset after a batch finishes consuming),
        # and it is the latter that is periodically persisted to the broker. _pending is the pulled-but-unconsumed buffer.
        self._pending: Dict[str, Deque[MessageExt]] = {}
        self._mq_map: Dict[str, MessageQueue] = {}
        self._consume_offsets: Dict[str, int] = {}
        # Frozen offsets (further updates refused): after OFFSET_ILLEGAL correction writes the
        # corrected value into _consume_offsets, in-flight ack / _correct_tags_offset_locked must not change it again,
        # until this queue is rebuilt at the corrected offset (unfrozen when _rebalance_pull_threads creates a new loop).
        self._frozen_offsets: Set[str] = set()
        # The queue-buffer epoch of each queue: incremented when the queue is revoked (dropped).
        # _dispatch_loop takes the epoch along with the batch and compares before acking -- a mismatch means this batch belongs
        # to an already discarded old queue buffer and must be voided as a whole (without it, the old batch's ack would push the offset
        # just corrected by the broker back to an illegal value, bouncing back and forth between client and broker).
        self._queue_epoch: Dict[str, int] = {}
        # Ordered consumption: the set of queue keys that broker LOCK_BATCH_MQ(41) confirmed as locked
        self._lock_ok: Set[str] = set()
        self._flow_control_triggered = 0
        self._dispatch_thread: Optional[threading.Thread] = None
        self._clean_expire_thread: Optional[threading.Thread] = None
        self._persist_thread: Optional[threading.Thread] = None
        self._lock_thread: Optional[threading.Thread] = None
        self._rebalance_thread: Optional[threading.Thread] = None
        self._queue_threads: Dict[str, threading.Thread] = {}
        # The most recent moment of "initiating a pull/pop" per queue.
        # rebalance uses it to judge whether pulling has stalled (PULL_MAX_IDLE_TIME).
        self._last_pull_table: Dict[str, float] = {}
        # The **whole-consumer**-level suspend switch (flipped by suspend()/resume()), unrelated to per-queue _lock_ok/flow control.
        # Loop threads must see it immediately; here this relies on CPython's attribute read/write atomicity.
        self._paused = False
        # MessageQueue -> which brokerId the next
        # pull should pick (driven by the response header suggestWhichBrokerId, master 0 by default)
        self._pull_from_which_node: Dict[MessageQueue, int] = {}
        # ---- Real rebalance ----
        # _assigned is "the set of queues currently assigned to this instance",
        # computed by _do_rebalance() per the allocation strategy; _rebalance_now is used for
        # the immediate rebalance triggered by NOTIFY_CONSUMER_IDS_CHANGED(40).
        self._assigned: List[MessageQueue] = []
        self._rebalance_now = threading.Event()
        # start() moment: the rebalance loop retries with a shorter interval during startup (see _rebalance_loop)
        self._start_time = time.time()
        # ---- Consumer heartbeat (``heartbeat_interval_millis`` defaults to 30s) ----
        # Required: the broker's consumer table only records the group's clientId after receiving a heartbeat,
        # otherwise rebalance's GET_CONSUMER_LIST_BY_GROUP has no return.
        self.heartbeat_enabled = True
        self.heartbeat_interval_millis = 30000
        # ---- Heartbeat V2 and the VIP channel switches (both default false) ----
        # ``use_heartbeat_v2``: on switches this consumer's HEART_BEAT(34)
        # rounds to the fingerprint-gated path (client/heartbeat_v2.py); off keeps heartbeatFingerprint=0,
        # i.e. the broker's V1 registration path.
        # ``vip_channel_enabled``: on makes the offset /
        # lock / consumer-list / send-re-back RPCs target the broker's VIP port (listen port - 2).
        self.use_heartbeat_v2 = False
        self.vip_channel_enabled = False
        # Received message bodies are materialised by default; switching this off decodes only
        # the headers and leaves every body unset.
        self.decode_read_body = True
        # Concurrent heartbeat: one round fans out over the instance's worker pool and waits
        # for every broker. The pool is created when the client instance starts.
        self.enable_concurrent_heartbeat = False
        self.concurrent_heartbeat_thread_pool_size = os.cpu_count() or 1
        self.mq_client_api_timeout = 3000
        # Route refresh interval (``poll_name_server_interval`` defaults to 30000ms).
        # Only passed through once when start() builds the MQClientInstance; later changes take no effect
        # (the background scheduling is arranged with the interval at startup).
        self.poll_name_server_interval = 30000
        # Persist interval for consumed offsets (``persist_consumer_offset_interval`` defaults to 5000ms;
        # first delay 1000 * 10, then periodic at this value).
        # Also read only once by ``_offset_persist_loop`` at start().
        self.persist_consumer_offset_interval = 5000
        self._heartbeat_count = 0
        self._heartbeat_thread: Optional[threading.Thread] = None
        # ---- POP mode (5.x lightweight consumption) ----
        # When off, it goes entirely through the original pull long-polling path, with behavior identical to before the change.
        self.pop_mode = False
        # The duration the message stays invisible to other instances after being popped (popInvisibleTime defaults to 60000)
        self.pop_invisible_time = 60000
        # Max messages per POP (popBatchNums defaults to 32; the broker rejects >32)
        self.pop_batch_nums = 32
        # Upper bound of this queue's "popped but not acked" counter; POP is paused when exceeded (pop_threshold_for_queue defaults to 96)
        self.pop_threshold_for_queue = 96
        # The delay levels (in seconds) for extending invisibility time on consume failure
        self.pop_delay_level = list(POP_DELAY_LEVEL)
        # POP long-polling suspend duration. 0 = short polling (the broker returns immediately or NO_NEW_MSG).
        # WARNING: when non-0, the request timeout must be greater than it, otherwise the client times out first.
        self.pop_poll_time_millis = 15000
        self.pop_timeout_millis = 25000
        # Queue key -> PopProcessQueue (popped-but-unacked count + whether it has been revoked by rebalance)
        self._pop_queues: Dict[str, PopProcessQueue] = {}
        self._pop_executor: Optional[ConsumeExecutor] = None
        # awaitTerminationMillisWhenShutdown: the ceiling (in milliseconds) that shutdown() spends
        # letting the consume executors drain their in-flight batches before returning.
        # <= 0 (default) adds **no** extra ceiling: the POP executor keeps the fire-and-forget
        # shutdown and the thread joins keep their fixed timeouts. > 0 bounds both the POP
        # executor's bounded join and the dispatch-thread join (the path concurrent/orderly
        # consumption runs on) with this value.
        self.await_termination_millis_when_shutdown = 0
        # POP ordered-request dedup set, equality =
        # (shard index, pq reference, mq); the shard index is always 0 here, pq uses object identity
        self._pop_orderly_requests: set = set()
        # ---- Message trace (enable_trace / trace_topic) ----
        # When enabled, start() registers ConsumeMessageTraceHook, recording two trace segments SubBefore/SubAfter
        self.enable_trace = False
        self.trace_topic: Optional[str] = None      # None -> use RMQ_SYS_TRACE_TOPIC
        # Consumption stats, bound to the instance's manager at start()
        self._stats_manager: Optional[ConsumerStatsManager] = None
        self.trace_msg_batch_num = 10
        self.consume_message_hook_list: List[ConsumeMessageHook] = []
        # Pre-delivery filter hooks
        self.filter_message_hook_list: List[FilterMessageHook] = []
        self.trace_dispatcher = None

    # ---------------- Config ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        """Set the unit name: affects the clientId suffix and the dynamic addressing URL."""
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        """Set unit mode: passed through to the broker with the heartbeat/request headers."""
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        """Toggle the stream request type: affects the clientId suffix and every request's ReqT extension field."""
        self.enable_stream_request_type = bool(enable)

    def set_namespace_v2(self, namespace_v2: Optional[str]) -> None:
        """Set the 5.x server-side namespace: when non-empty, every request carries nsd=true / ns=<value>."""
        self.namespace_v2 = namespace_v2

    def get_namespace_v2(self) -> Optional[str]:
        return self.namespace_v2

    def set_vip_channel_enabled(self, enable: bool) -> None:
        """Turn the VIP channel on or off. With it on, the RPCs the broker answers on its fast
        channel -- offsets, queue locks, the group consumer list, message re-send-back,
        earliest-msg-store-time and every send, reply messages included -- target **the broker's
        listen port minus 2** instead of the plain one. Off by default.

        The rewrite itself lives on ``MQClientInstance``, so an
        already-started facade updates the shared instance too.
        """
        self.vip_channel_enabled = bool(enable)
        if self._mq_client is not None:
            self._mq_client.vip_channel_enabled = bool(enable)

    def is_vip_channel_enabled(self) -> bool:
        return self.vip_channel_enabled

    def set_use_heartbeat_v2(self, enable: bool) -> None:
        """Opt into the fingerprint-gated heartbeat V2 path; off by default here, so a client
        that never asks for it keeps the broker on its V1 registration path."""
        self.use_heartbeat_v2 = bool(enable)

    def set_decode_read_body(self, enable: bool) -> None:
        """Control whether received message bodies are materialised; on by default.

        The shared client instance is updated too, because decoding happens there.
        """
        self.decode_read_body = bool(enable)
        if self._mq_client is not None:
            self._mq_client.decode_read_body = bool(enable)

    def set_enable_concurrent_heartbeat(self, enable: bool) -> None:
        """Send one heartbeat per broker in parallel once the client instance starts."""
        self.enable_concurrent_heartbeat = bool(enable)
        if self._mq_client is not None:
            self._mq_client.enable_concurrent_heartbeat = bool(enable)

    def set_concurrent_heartbeat_thread_pool_size(self, size: int) -> None:
        """Worker count of the concurrent-heartbeat pool; applied when the instance starts."""
        self.concurrent_heartbeat_thread_pool_size = max(1, int(size))
        if self._mq_client is not None:
            self._mq_client.concurrent_heartbeat_thread_pool_size = max(1, int(size))

    def set_mq_client_api_timeout(self, timeout_millis: int) -> None:
        """Timeout of the client's internal API calls (heartbeat, route fetch, unregister,
        consumer-list lookup); per-call explicit timeouts still win."""
        self.mq_client_api_timeout = int(timeout_millis)
        if self._mq_client is not None:
            self._mq_client.mq_client_api_timeout = int(timeout_millis)

    def set_socks_proxy_config(self, socks_proxy_config: Optional[SocksProxyConfig]) -> None:
        pass
        self.socks_proxy_config = socks_proxy_config

    def get_socks_proxy_config(self) -> Optional[SocksProxyConfig]:
        return self.socks_proxy_config

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_consume_from_where(self, where: str) -> None:
        self.consume_from_where = where

    def set_consume_timestamp(self, timestamp: str) -> None:
        """Set the start time of CONSUME_FROM_TIMESTAMP.

        Format ``yyyyMMddHHmmss`` (14-digit local time), default 30 minutes ago.
        """
        self.consume_timestamp = timestamp

    def _consume_timestamp_millis(self) -> int:
        # Failure to parse must be a hard failure -- silently falling back to "now - 30 minutes"
        # would leave the start point misaligned unnoticed; start() also unconditionally validates this field.
        try:
            if len(str(self.consume_timestamp)) != 14:
                raise ValueError(self.consume_timestamp)
            return int(time.mktime(time.strptime(self.consume_timestamp, "%Y%m%d%H%M%S")) * 1000)
        except (ValueError, TypeError, OverflowError):
            raise MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received %s"
                % self.consume_timestamp)

    def set_consume_thread_nums(self, n: int) -> None:
        """Convenience method: sets min and max together.

        For finer adjustment use ``set_consume_thread_min`` / ``set_consume_thread_max``;
        this method is kept for compatibility with existing call sites.
        """
        n = max(1, int(n))
        self.consume_thread_min = n
        self.consume_thread_max = n
        self._core_pool_size = n
        self._apply_core_pool_size()

    def set_consume_thread_min(self, n: int) -> None:
        self.consume_thread_min = max(1, int(n))
        self._core_pool_size = self.consume_thread_min
        self._apply_core_pool_size()

    def set_consume_thread_max(self, n: int) -> None:
        self.consume_thread_max = max(1, int(n))

    def get_consume_thread_min(self) -> int:
        return self.consume_thread_min

    def get_consume_thread_max(self) -> int:
        return self.consume_thread_max

    def set_adjust_thread_pool_nums_threshold(self, value: int) -> None:
        """Set the backlog threshold for auto-elasticity."""
        self.adjust_thread_pool_nums_threshold = int(value)

    def get_adjust_thread_pool_nums_threshold(self) -> int:
        return self.adjust_thread_pool_nums_threshold

    # ---------------- Thread elasticity ----------------

    def update_core_pool_size(self, core_pool_size: int) -> bool:
        """Adjust the consume concurrency at runtime.

        Guard::

            owns_consume_executor && core_pool_size > 0
                && core_pool_size <= 32767 (Short.MAX_VALUE)
                && core_pool_size < consume_thread_max

        Any unsatisfied condition is **silently ignored** without raising.
        The return value means "whether it actually took effect", for callers and test assertions.
        """
        if not self._owns_consume_executor:
            return False
        if not (0 < int(core_pool_size) <= 32767):    # Short.MAX_VALUE
            return False
        if int(core_pool_size) >= self.consume_thread_max:
            return False
        self._core_pool_size = int(core_pool_size)
        self._apply_core_pool_size()
        return True

    def get_core_pool_size(self) -> int:
        """Current core pool size; returns -1 when the executor is not self-built."""
        if not self._owns_consume_executor:
            return -1
        if self._pop_executor is not None:
            return self._pop_executor.get_core_pool_size()
        return self._core_pool_size

    def _apply_core_pool_size(self) -> None:
        """Apply the declared core size to the real executor (only records the declared value when there is no executor)."""
        if self._pop_executor is not None and self._owns_consume_executor:
            self._pop_executor.set_core_pool_size(self._core_pool_size)

    def compute_accumulation_total(self) -> int:
        """The total backlog count across all queues.

        That is, the sum of all ``msg_acc_cnt`` entries: the backlog count from the most recent
        pull recorded "per queue" (see ``_update_msg_acc_cnt``).
        """
        with self._lock:
            return sum(self._msg_acc_cnt_table.values())

    def adjust_thread_pool(self) -> None:
        """Attempt to adjust concurrency per the backlog threshold (scheduled once per minute).

        WARNING **this is a no-op**: the underlying ``inc_core_pool_size()`` / ``dec_core_pool_size()``
        are **empty method bodies**. The threshold comparison and logging are kept here only to make ``msg_acc_cnt`` / threshold config
        observable and assertable; **do not "fix" it** -- what actually takes effect is the explicit ``update_core_pool_size()``.
        """
        acc_total = self.compute_accumulation_total()
        threshold = self.adjust_thread_pool_nums_threshold
        inc_threshold = threshold * 1.0
        dec_threshold = threshold * 0.8
        if acc_total >= inc_threshold:
            # inc_core_pool_size() is a no-op
            logger.debug("adjustThreadPool: acc=%d >= incThreshold=%d (inc is a no-op upstream)",
                         acc_total, int(inc_threshold))
        if acc_total < dec_threshold:
            # dec_core_pool_size() is a no-op
            logger.debug("adjustThreadPool: acc=%d < decThreshold=%d (dec is a no-op upstream)",
                         acc_total, int(dec_threshold))

    def set_pull_suspend_timeout_millis(self, millis: int) -> None:
        self.pull_suspend_timeout_millis = int(millis)

    def set_await_termination_millis_when_shutdown(self, millis: int) -> None:
        """Bound how long shutdown() waits for the consume executors to drain in-flight batches.

        <= 0 (default) adds no extra ceiling; a positive value is that ceiling in milliseconds.
        """
        self.await_termination_millis_when_shutdown = int(millis)

    def get_await_termination_millis_when_shutdown(self) -> int:
        return self.await_termination_millis_when_shutdown

    def set_post_subscription_when_pull(self, enable: bool) -> None:
        """Toggle "bring the subscription expression into the request header when pulling"."""
        self.post_subscription_when_pull = bool(enable)

    def set_message_listener(self, listener) -> None:
        self.message_listener = listener

    def set_allocate_message_queue_strategy(self, strategy: AllocateMessageQueueStrategy) -> None:
        self.allocate_strategy = strategy

    # ---------------- Message trace ----------------
    def set_enable_msg_trace(self, enable: bool) -> None:
        """Toggle consumer-side message tracing (the consumer's ``enable_trace`` flag)."""
        self.enable_trace = enable

    def set_enable_trace(self, enable: bool) -> None:
        self.enable_trace = enable

    def is_enable_trace(self) -> bool:
        return self.enable_trace

    def set_customized_trace_topic(self, trace_topic: Optional[str]) -> None:
        """Custom trace topic; empty means use the system default RMQ_SYS_TRACE_TOPIC."""
        self.trace_topic = trace_topic

    def set_trace_topic(self, trace_topic: Optional[str]) -> None:
        self.trace_topic = trace_topic

    def set_trace_msg_batch_num(self, n: int) -> None:
        self.trace_msg_batch_num = n

    def register_consume_message_hook(self, hook: ConsumeMessageHook) -> None:
        """Register a consume hook (before/after callbacks, for tracing and auditing)."""
        if hook is not None:
            self.consume_message_hook_list.append(hook)

    def has_consume_message_hook(self) -> bool:
        return len(self.consume_message_hook_list) > 0

    # ---------------- Pre-delivery filter hooks ----------------
    def register_filter_message_hook(self, hook: FilterMessageHook) -> None:
        """Register a pre-delivery filter hook: can drop some messages before they go to the listener."""
        if hook is not None:
            self.filter_message_hook_list.append(hook)

    def has_filter_message_hook(self) -> bool:
        return len(self.filter_message_hook_list) > 0

    def execute_filter_message_hook(self, context: FilterMessageContext) -> None:
        """Hook exceptions are always swallowed and logged as errors."""
        execute_filter_hooks(self.filter_message_hook_list, context)

    def _filter_messages_for_delivery(self, mq: MessageQueue, sub: Optional[SubscriptionData],
                                      msgs: List[MessageExt]) -> List[MessageExt]:
        """Pre-delivery filtering (see the module-level filter_messages_for_delivery for details)."""
        return filter_messages_for_delivery(self.consumer_group, self.filter_message_hook_list,
                                            mq, sub, msgs, self.unit_mode)

    def execute_consume_hook_before(self, context: ConsumeMessageContext) -> None:
        for hook in self.consume_message_hook_list:
            try:
                hook.consume_message_before(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("consumeMessageHook executeHookBefore exception: %s", e)

    def execute_consume_hook_after(self, context: ConsumeMessageContext) -> None:
        for hook in self.consume_message_hook_list:
            try:
                hook.consume_message_after(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("consumeMessageHook executeHookAfter exception: %s", e)

    def _build_consume_hook_context(self, msgs: List[MessageExt],
                                    mq: MessageQueue) -> ConsumeMessageContext:
        """Build the consume hook context: success=False, props is an empty dict, access_channel=LOCAL."""
        context = ConsumeMessageContext(self.consumer_group, msgs, mq)
        context.success = False
        context.props = {}
        context.access_channel = AccessChannel.LOCAL
        return context

    def _consume_return_type(self, status, has_exception: bool, consume_rt_ms: float,
                             failed: bool, succeeded: bool) -> ConsumeReturnType:
        """returnType determination (decides the trace's contextCode).

        Order of determination: status == null -> EXCEPTION/RETURNNULL; RT >= consumeTimeout
        (minutes) -> TIME_OUT; RECONSUME_LATER -> FAILED; CONSUME_SUCCESS -> SUCCESS.
        For ordered consumption, "suspend" is equivalent to FAILED and success is equivalent to SUCCESS, passed in by the caller via
        the two flags failed/succeeded.
        """
        if status is None:
            return ConsumeReturnType.EXCEPTION if has_exception else ConsumeReturnType.RETURNNULL
        if consume_rt_ms >= self.consume_timeout * 60 * 1000:
            return ConsumeReturnType.TIME_OUT
        if failed:
            return ConsumeReturnType.FAILED
        if succeeded:
            return ConsumeReturnType.SUCCESS
        return ConsumeReturnType.SUCCESS

    def _record_consume_rt(self, topic: str, begin_ms: float) -> None:
        """Consume RT counting: once per batch, **unconditionally**.

        Separate from the ok/failed counts: the RT histogram is recorded on every consume, while TPS is only recorded when the result falls into some branch
        (ordered consumption's ``COMMIT``/``ROLLBACK`` branches add no TPS at all, but RT is still recorded).
        """
        if self._stats_manager is None:
            return
        self._stats_manager.inc_consume_rt(self.consumer_group, topic,
                                           int(time.time() * 1000 - begin_ms))

    def _record_consume_stats(self, topic: str, msg_count: int, begin_ms: float,
                              failed: bool, ack_count: Optional[int] = None) -> None:
        """Consumer-side TPS counting: the two counts OK / FAILED.

        ``ack_count`` is simply ``ok = ackIndex + 1``:
        on partial ack the prefix counts as OK and the tail counts as FAILED. If not passed, the whole batch is counted (the old policy for ordered/POP paths).
        """
        if self._stats_manager is None:
            return
        if failed:
            self._stats_manager.inc_consume_failed_tps(self.consumer_group, topic, msg_count)
        else:
            ok = msg_count if ack_count is None else ack_count
            self._stats_manager.inc_consume_ok_tps(self.consumer_group, topic, ok)
            if msg_count > ok:
                self._stats_manager.inc_consume_failed_tps(self.consumer_group, topic,
                                                           msg_count - ok)
        self._record_consume_rt(topic, begin_ms)

    def _finish_consume_hook(self, hook_ctx: Optional[ConsumeMessageContext], status,
                             has_exception: bool, begin_ms: float, failed: bool,
                             succeeded: bool, hook_status=None) -> None:
        """Write returnType/status/success back into the context and trigger the after hook.

        ``status`` is the one that decides returnType; ``hook_status`` is the one written into the context --
        these two steps do **not** use the same value: returnType is computed **before** status normalization (null ->
        RECONSUME_LATER / suspend), while
        context.status takes the value **after** normalization.
        When ``hook_status`` is not passed, the two are the same.
        """
        if hook_ctx is None:
            return
        rt = time.time() * 1000 - begin_ms
        ret = self._consume_return_type(status, has_exception, rt, failed, succeeded)
        hook_ctx.props["ConsumeContextType"] = ret.name
        hook_ctx.status = consume_status_name(status if hook_status is None else hook_status)
        hook_ctx.success = succeeded
        self.execute_consume_hook_after(hook_ctx)

    def get_consumer_group(self) -> str:
        return self.consumer_group

    def subscriptions(self) -> List[SubscriptionData]:
        """The consumer's current subscription set.

        Used by the client instance's ``check_client_in_broker`` (it iterates by instance and does not look at the specific consumer type).
        """
        return list(self.subscription_data.values())

    # ---------------- Subscription ----------------
    def subscribe(self, topic: str, sub_expression: str = "*") -> None:
        """Subscribe to a topic.

        Can **still** be called after ``start()``: the subscription table is live (the heartbeat and rebalance re-read it every round),
        and an immediate heartbeat is sent once it is put in (see ``_notify_subscription_changed``).
        """
        topic = self._with_namespace(topic)
        sub = FilterAPI.build_subscription_data(topic, sub_expression)
        with self._lock:
            self.subscription_data[topic] = sub
        self._notify_subscription_changed(topic)

    def subscribe_with_selector(self, topic: str, selector: MessageSelector) -> None:
        """Subscribe to a topic by selector (tag / SQL92 / class filter).

        Same as ``subscribe``: callable after start(), and an immediate heartbeat is sent.
        """
        topic = self._with_namespace(topic)
        # Build the subscription data by expression type (FilterAPI.build policy):
        #   TAG (or empty type) -> build_subscription_data (fills tagsSet + codeSet)
        #   others (SQL92 / CLASS_FILTER) -> only set topic/subString/expressionType, leaving both sets empty
        if selector.type == ExpressionType.TAG:
            sub = FilterAPI.build_subscription_data(topic, selector.expression)
            sub.expression_type = selector.type
        else:
            if not selector.expression:
                raise ValueError("Expression can't be null! %s" % selector.type)
            sub = SubscriptionData(topic=topic, sub_string=selector.expression)
            sub.expression_type = selector.type
        with self._lock:
            self.subscription_data[topic] = sub
        self._notify_subscription_changed(topic)

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe: **only removes the table entry, no heartbeat**.

        The next heartbeat round naturally carries the new subscription set, so this method
        does not call ``_notify_subscription_changed``; only ``subscribe`` pushes a heartbeat immediately.
        """
        with self._lock:
            self.subscription_data.pop(self._with_namespace(topic), None)

    def _notify_subscription_changed(self, topic: str) -> None:
        """Immediate actions after the subscription table is added/updated: synchronously push one heartbeat round + register the topic as in use.

        Why it must be pushed immediately: the broker only records topic->group into
        **its own** topicGroupTable when a heartbeat is received (its consumer-registration step), and
        ``QUERY_TOPIC_CONSUME_BY_WHO(300)`` reads exactly that table; one round late is a 30s blind spot by default.

        Also register the new topic as "in use" so the background route refresh task covers it --
        the refreshed topic set comes from the consumer's **current** subscription table, not a snapshot from startup.
        When not started there is no client instance, and only the subscription table is updated.
        """
        if not self._started or self._mq_client is None:
            return
        self._mq_client.register_topic_in_use(topic)
        try:
            self._send_heartbeat_to_all_broker()
        except Exception as e:  # noqa: BLE001
            logger.debug("immediate heartbeat after subscribe(%s) failed: %s", topic, e)

    def _with_namespace(self, topic: str) -> str:
        """Prefix the topic with the namespace; returns as-is when no namespace is configured."""
        if not self.namespace:
            return topic
        return NamespaceUtil.wrap_namespace(self.namespace, topic)

    # ---------------- Config value gates ----------------
    def _check_config_ranges(self) -> None:
        """Config value gates: validates the ranges item by item.

        The order, ranges, and wording are all deliberate (error strings by convention carry no FAQ suffix).
        Comparisons are always the strict inequality ``< lo || > hi`` -- the ``pop_batch_nums`` one is written as ``<= 0``,
        which for integers is equivalent to ``< 1``.

        Why gate it in ``start()``: these values directly determine buffer levels and thread pool size. Without **any gate**,
        writing 0 or ``2**31-1`` would either have every pull judged as backlog by ``threshold <= 0`` and stop pulling forever
        (the consumer silently receives no messages), or integer overflow would bypass the drop/flow-control checks entirely.
        Reporting the error on the broker side is already several beats late, and the error message only reaches the logs.

        Note: the ``-1`` of ``pull_threshold_for_topic`` / ``pull_threshold_size_for_topic`` is
        a "not set, use the queue-level threshold" sentinel, which is **not** in range and also does **not** error.
        """
        # consumeThreadMin
        if self.consume_thread_min < 1 or self.consume_thread_min > 1000:
            raise MQClientException("consumeThreadMin Out of range [1, 1000]")
        # consumeThreadMax
        if self.consume_thread_max < 1 or self.consume_thread_max > 1000:
            raise MQClientException("consumeThreadMax Out of range [1, 1000]")
        # consumeThreadMin can't be larger than consumeThreadMax
        if self.consume_thread_min > self.consume_thread_max:
            raise MQClientException(
                "consumeThreadMin (%d) is larger than consumeThreadMax (%d)"
                % (self.consume_thread_min, self.consume_thread_max))
        # consumeConcurrentlyMaxSpan
        if self.consume_concurrently_max_span < 1 or self.consume_concurrently_max_span > 65535:
            raise MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]")
        # pullThresholdForQueue
        if self.pull_threshold_for_queue < 1 or self.pull_threshold_for_queue > 65535:
            raise MQClientException("pullThresholdForQueue Out of range [1, 65535]")
        # pullThresholdForTopic
        if self.pull_threshold_for_topic != -1:
            if self.pull_threshold_for_topic < 1 or self.pull_threshold_for_topic > 6553500:
                raise MQClientException("pullThresholdForTopic Out of range [1, 6553500]")
        # pullThresholdSizeForQueue
        if self.pull_threshold_size_for_queue < 1 or self.pull_threshold_size_for_queue > 1024:
            raise MQClientException("pullThresholdSizeForQueue Out of range [1, 1024]")
        # pullThresholdSizeForTopic
        if self.pull_threshold_size_for_topic != -1:
            if self.pull_threshold_size_for_topic < 1 or self.pull_threshold_size_for_topic > 102400:
                raise MQClientException("pullThresholdSizeForTopic Out of range [1, 102400]")
        # pullInterval
        if self.pull_interval < 0 or self.pull_interval > 65535:
            raise MQClientException("pullInterval Out of range [0, 65535]")
        # consumeMessageBatchMaxSize
        if self.consume_message_batch_max_size < 1 or self.consume_message_batch_max_size > 1024:
            raise MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]")
        # pullBatchSize
        if self.pull_batch_size < 1 or self.pull_batch_size > 1024:
            raise MQClientException("pullBatchSize Out of range [1, 1024]")
        # popInvisibleTime
        if (self.pop_invisible_time < MIN_POP_INVISIBLE_TIME
                or self.pop_invisible_time > MAX_POP_INVISIBLE_TIME):
            raise MQClientException("popInvisibleTime Out of range [%d, %d]"
                                    % (MIN_POP_INVISIBLE_TIME, MAX_POP_INVISIBLE_TIME))
        # popBatchNums
        if self.pop_batch_nums <= 0 or self.pop_batch_nums > 32:
            raise MQClientException("popBatchNums Out of range [1, 32]")

    # ---------------- Lifecycle ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            # The consumer group is also namespaced (stored as the result of _with_namespace on the group).
            # Must happen before computing the retry topic: retry topic = %RETRY% + the prefixed group name.
            if self.namespace:
                self.consumer_group = NamespaceUtil.wrap_namespace(self.namespace, self.consumer_group)
            # First validators.check_group (blank / 120 length / character table), then block DEFAULT_CONSUMER
            # -- sharing the default group would make the broker's subscription judgment mix the two groups, scrambling redelivery and rebalance.
            # Group name validation comes before the subscription/address checks.
            validators.check_group(self.consumer_group)
            if self.consumer_group == MixAll.DEFAULT_CONSUMER_GROUP:
                raise MQClientException(
                    "consumerGroup can not equal %s, please specify another one."
                    % MixAll.DEFAULT_CONSUMER_GROUP)
            if not self.name_server_addrs and not DefaultTopAddressing.is_configured():
                # Static addresses and dynamic addressing (ROCKETMQ_NAMESRV_DOMAIN): one of the two must be available
                raise MQClientException("name server address is not set")
            if not self.subscription_data:
                raise MQClientException("subscription is not set, call subscribe() first")
            if self.message_listener is None:
                raise MQClientException("message listener is not set")
            # The start time is unconditionally validated at startup,
            # rather than waiting for rebalance to throw and be swallowed by compute_pull_from_where's fallback.
            self._consume_timestamp_millis()
            # A None allocation strategy rejects startup directly.
            if self.allocate_strategy is None:
                raise MQClientException("allocateMessageQueueStrategy is null")
            # The value gates run after all null checks and before any network action: bad config must fail
            # before the MQClientInstance is built, otherwise background threads would be started and then leak on error.
            self._check_config_ranges()
            # Only CLUSTERING renames instance_name to the PID (BROADCASTING keeps "DEFAULT",
            # so broadcasting consumers in the same process can reuse the same client instance), then
            # ``MixAll.client_id_for`` builds `<local IP>@<instance_name>[@<unit_name>][@STREAM]`.
            if self.message_model == MessageModel.CLUSTERING:
                self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
            if self.client_id is None:
                self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                      self.enable_stream_request_type)
            def _create_instance() -> MQClientInstance:
                return MQClientInstance(
                    self.client_id, self.name_server_addrs,
                    tls_enable=self.tls_enable,
                    tls_options=self.tls_options,
                    socks_proxy_config=self.socks_proxy_config,
                    enable_stream_request_type=self.enable_stream_request_type,
                    unit_name=self.unit_name,
                    namespace_v2=self.namespace_v2,
                    poll_name_server_interval=self.poll_name_server_interval,
                    vip_channel_enabled=self.vip_channel_enabled,
                    decode_read_body=self.decode_read_body,
                    mq_client_api_timeout=self.mq_client_api_timeout,
                    enable_concurrent_heartbeat=self.enable_concurrent_heartbeat,
                    concurrent_heartbeat_thread_pool_size=self.concurrent_heartbeat_thread_pool_size)

            self._mq_client, created = MQClientManager.get_instance() \
                .get_or_create_mq_client_instance(self.client_id, _create_instance)
            self._mq_client.decode_read_body = self.decode_read_body
            self._mq_client.mq_client_api_timeout = self.mq_client_api_timeout
            self._mq_client.enable_concurrent_heartbeat = self.enable_concurrent_heartbeat
            self._mq_client.concurrent_heartbeat_thread_pool_size = \
                self.concurrent_heartbeat_thread_pool_size
            if created and self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            manager = MQClientManager.get_instance()
            manager.register_client(self.client_id, self._mq_client)
            try:
                self._mq_client.start()
            except Exception:
                # Startup failure must not leave a dangling reference count (the instance also removes itself from the cache).
                manager.unregister_client(self.client_id, self._mq_client)
                raise
            # Dynamic name server: the instance may have already obtained addresses from the address server at startup; backfill them into this consumer
            # so the running-info report and other places can see them.
            if not self.name_server_addrs and self._mq_client.name_server_addrs:
                self.name_server_addrs = list(self._mq_client.name_server_addrs)
            # Consumption stats (shared at the instance level)
            self._stats_manager = self._mq_client.consumer_stats_manager
            self._mq_client.register_consumer(self.consumer_group, self)
            self._started = True
            self._start_time = time.time()
            self._stop.clear()
            # In cluster mode, automatically subscribe to the retry topic (retry_topic = MixAll.get_retry_topic(consumer_group)),
            # where broker-redelivered messages are written
            if self.message_model != MessageModel.BROADCASTING:
                retry_topic = MixAll.get_retry_topic(self.consumer_group)
                if retry_topic not in self.subscription_data:
                # The retry topic subscription is built with SUB_ALL:
                # under SUB_ALL tagsSet / codeSet are both **empty** (not {"*"}), see subscription_data.FilterAPI
                    sub = FilterAPI.build_subscription_data(retry_topic, "*")
                    self.subscription_data[retry_topic] = sub
            # Broker notification 40 (consumer up/down -> immediate recompute) is **not** registered with a handler here:
            # it is registered at the instance level (MQClientInstance), where one code has only one handler,
            # and each consumer registering its own would overwrite the others. On receipt it is fanned out to
            # each registered consumer (see ``rebalance_immediately``).
        # The POP mode consume thread pool must be built before rebalance: rebalance immediately starts each queue's POP
        # loop, and the loop delivers fetched messages here.
        if self.pop_mode:
            # core=consumeThreadMin, max=consumeThreadMax, unbounded queue
            # (hence **real concurrency == core**). To get the two-tier core/max semantics, the self-implemented
            # ConsumeExecutor is used here (the standard library's ThreadPoolExecutor has only a max cap).
            self._pop_executor = ConsumeExecutor(
                core_pool_size=max(1, self._core_pool_size),
                maximum_pool_size=max(1, self.consume_thread_max),
                thread_name_prefix="rmq-popconsume-%s" % self.consumer_group)
        # The order of start:
        # refresh routes once -> send a heartbeat (so the broker knows this consumer) -> rebalance immediately -> start consume threads.
        # The heartbeat must come before rebalance: rebalance queries the broker for the consumer list.
        self._refresh_routes()
        # After routes arrive and before the heartbeat, send non-TAG subscriptions to the broker for validation
        # (CHECK_CLIENT_CONFIG 46). When SQL92 is wrong, the broker's filter layer
        # cannot get the compiled data and **silently passes all messages**; only this step can turn that into a startup error;
        # if this step fails, shutdown() is called and the exception is re-raised to the caller.
        try:
            self._mq_client.check_client_in_broker()
        except Exception:
            self.shutdown()
            raise
        try:
            self._send_heartbeat_to_all_broker()
        except Exception as e:  # noqa: BLE001
            logger.debug("initial heartbeat failed: %s", e)
        # The first allocation round must complete synchronously: otherwise pull threads spin on an empty allocation set until the first rebalance round takes effect
        try:
            self._do_rebalance()
        except Exception as e:  # noqa: BLE001
            logger.warning("initial rebalance failed: %s", e)
        self._start_heartbeat_loop()
        self._start_pull_loop()
        self._start_dispatch_loop()
        # The suspended listener's cleanup scheduling exists only on the **classic concurrent** path:
        # ordered consumption's cleanup entry simply returns, and the POP path has no corresponding scheduling.
        if not self._is_orderly() and not self.pop_mode:
            self._start_clean_expire_loop()
        self._start_offset_persist_loop()
        self._start_lock_loop()
        t = threading.Thread(target=self._rebalance_loop, daemon=True,
                             name="rmq-rebalance-%s" % self.consumer_group)
        t.start()
        self._rebalance_thread = t
        # Message trace dispatcher:
        # started after all consume loops are up, to avoid the trace producer's initialization slowing the first rebalance.
        self._start_trace_dispatcher()

    def _start_trace_dispatcher(self) -> None:
        """When enable_trace=true, build an AsyncTraceDispatcher (Type=CONSUME) and register a consume hook."""
        if self.enable_trace:
            try:
                dispatcher = AsyncTraceDispatcher(
                    self.consumer_group, TraceDispatcherType.CONSUME,
                    self.trace_msg_batch_num, self.trace_topic, self.rpc_hook)
                dispatcher.set_host_consumer(self)
                # The trace producer also needs the 5.x namespace.
                dispatcher.set_namespace_v2(self.namespace_v2)
                self.trace_dispatcher = dispatcher
                self.register_consume_message_hook(ConsumeMessageTraceHook(dispatcher))
            except Exception as e:  # noqa: BLE001
                logger.error("system mqtrace hook init failed ,maybe can't send msg trace data: %s", e)
        if self.trace_dispatcher is not None:
            try:
                self.trace_dispatcher.start(";".join(self.name_server_addrs), AccessChannel.LOCAL)
            except Exception as e:  # noqa: BLE001
                logger.warning("trace dispatcher start failed: %s", e)

    def shutdown(self) -> None:
        pop_drain_executor: Optional[ConsumeExecutor] = None
        with self._lock:
            if not self._started:
                return
            self._stop.set()
            # POP: mark all queues as dropped, in-flight batches are no longer acked (leave them to the broker to revive and redeliver)
            if self.pop_mode:
                for pq in self._pop_queues.values():
                    pq.set_dropped(True)
                self._pop_queues.clear()
                if self._pop_executor is not None:
                    pop_drain_executor = self._pop_executor
                    pop_drain_executor.shutdown(wait=False)
                    self._pop_executor = None
        # awaitTerminationMillisWhenShutdown > 0: give the POP consume executor a **bounded**
        # drain of its in-flight batches before moving on (never under the lock: the workers
        # themselves take it). <= 0 keeps the previous fire-and-forget behavior: no extra ceiling.
        await_millis = self.await_termination_millis_when_shutdown
        if pop_drain_executor is not None and await_millis > 0:
            pop_drain_executor.shutdown(wait=True, timeout=await_millis / 1000.0)
        # Persist consumed offsets once before exiting (a full flush of the currently held queues).
        # Must be called before _started=False (_require_client).
        try:
            self._persist_offsets_once()
        except Exception as e:  # noqa: BLE001
            logger.debug("persist offsets on shutdown failed: %s", e)
        # Ordered consumption unlocks every held queue on shutdown.
        # The POP ordered service goes through the same entry point, and it only reads the local process-queue table -- an empty table in POP mode,
        # so no message is sent.
        if self._is_orderly() and not self.pop_mode \
                and self.message_model != MessageModel.BROADCASTING:
            try:
                mqs = self._assigned_queues()
                if mqs:
                    self._require_client().unlock_batch_mq(self.consumer_group, self.client_id or "", mqs)
            except Exception as e:  # noqa: BLE001
                logger.debug("unlock on shutdown failed: %s", e)
        # Graceful unregister: immediately remove this group from each broker's consumer table,
        # without waiting for heartbeat timeout (default ~120s) -- otherwise during that window
        # consumer change notifications / transaction checks may still be sent to this already-exited instance.
        try:
            self._require_client().unregister_client_all_brokers(
                self.client_id or "", "", self.consumer_group)
        except Exception as e:  # noqa: BLE001
            logger.debug("unregister on shutdown failed: %s", e)
        with self._lock:
            self._started = False
        # awaitTerminationMillisWhenShutdown > 0 also bounds the dispatch-thread join:
        # concurrent/orderly batches run to completion on that thread, so this is where the
        # "let in-flight consumption finish" ceiling applies for the non-POP consume path.
        # <= 0 keeps the previous fixed 2s joins.
        dispatch_join_timeout = (await_millis / 1000.0 if await_millis > 0 else 2)
        for t in (self._persist_thread, self._lock_thread,
                  self._clean_expire_thread, self._rebalance_thread, self._heartbeat_thread):
            if t is not None and t.is_alive():
                t.join(timeout=2)
        if self._dispatch_thread is not None and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=dispatch_join_timeout)
        for t in list(self._queue_threads.values()):
            if t.is_alive():
                t.join(timeout=2)
        self._queue_threads.clear()
        with self._lock:
            self._last_pull_table.clear()
        if self._mq_client is not None:
            unregister_consumer = getattr(self._mq_client, "unregister_consumer", None)
            if unregister_consumer is not None:
                unregister_consumer(self.consumer_group)
            try:
                MQClientManager.get_instance().unregister_client(
                    self.client_id or "", self._mq_client)
            except Exception:  # noqa: BLE001
                pass
        for t in self._consume_threads:
            if t.is_alive():
                t.join(timeout=2)
        self._consume_threads.clear()
        # The trace dispatcher is closed last (flushing the remaining traces)
        if self.trace_dispatcher is not None:
            try:
                self.trace_dispatcher.shutdown()
            except Exception as e:  # noqa: BLE001
                logger.warning("trace dispatcher shutdown failed: %s", e)

    def suspend(self) -> None:
        """Suspend this consumer.

        Only flips one boolean: the pull/pop loop backs off at the suspend gate each round
        ``PULL_TIME_DELAY_WHEN_SUSPEND`` (1000ms) and retries, **without exiting**,
        and still stamps lastPullTimestamp, so suspending is not stalling and the allocation is not taken away by rebalance.
        Messages already fetched locally are consumed as usual (the gate only governs initiating the next pull).
        """
        self._paused = True
        logger.info("suspend this consumer, %s", self.consumer_group)

    def resume(self) -> None:
        """Resume from suspension: set back to false and trigger one rebalance immediately."""
        self._paused = False
        # Queue changes during suspension may have gone unprocessed: resume actively triggers one rebalance.
        self.rebalance_immediately()
        logger.info("resume this consumer, %s", self.consumer_group)

    def is_paused(self) -> bool:
        """Whether the consumer is currently suspended."""
        return self._paused

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        return self._mq_client

    # ---------------- Heartbeat (consumer registration) ----------------
    def _refresh_routes(self) -> None:
        """Pull the routes of subscribed topics once, and fill in the broker address table along the way (the heartbeat relies on it).

        Also register the subscribed topics as "in use", handing them to MQClientInstance's background task for periodic route refresh
        (``subscriptions()`` -> refresh the routes topic by topic).
        This way changes like a new topic being created by the broker or queue scaling are discovered without waiting for the next rebalance.
        """
        client = self._require_client()
        with self._lock:
            topics = list(self.subscription_data.keys())
        for topic in topics:
            client.register_topic_in_use(topic)
            try:
                client.get_topic_publish_info(topic)
            except MQClientException as e:  # noqa: BLE001
                logger.debug("refresh route for %s failed: %s", topic, e)

    def _build_heartbeat(self) -> HeartbeatData:
        hb = HeartbeatData(self.client_id or "")
        cd = ConsumerData(self.consumer_group, ConsumeType.CONSUME_PASSIVELY,
                          self.message_model, self.consume_from_where)
        # unitMode must be written into ConsumerData:
        # the broker uses it to stamp the UNIT system flag on the auto-created %RETRY% topic
        # (the broker judges it when registering the consumer).
        cd.unit_mode = self.unit_mode
        with self._lock:
            subs = list(self.subscription_data.values())
        for sub in subs:
            cd.subscription_data_set.add(sub)
        hb.consumer_data_set.add(cd)
        return hb

    def _send_heartbeat_to_all_broker(self) -> int:
        """Send a heartbeat to all known brokers.

        The consumer **must** register with the broker: the broker's consumer table only learns
        which clientIds are in the consume group after receiving a heartbeat, otherwise rebalance's GET_CONSUMER_LIST_BY_GROUP has no return.
        This implementation previously never sent consumer heartbeats (subscriptions rode along in the pull request's subscription property),
        because at the time queue allocation was "all to self" so it was not exposed; once real rebalance is done,
        an empty consumer list means no queues can be allocated. Returns the number of successful sends, for live verification assertions.

        Recipients take **every** broker in the route (master + slave, `get_all_broker_addrs`):
        iterate each brokerId in the route, skipping only when `id != MASTER_ID` and the group is empty there;
        this heartbeat always carries ConsumerData (never an empty consumer set), so slaves are not skipped. Skipping slaves is not "one redundant send fewer":
        the broker's consumer registry keeps **per-broker** state, and when a slave receives no heartbeat,
        it returns `SUBSCRIPTION_NOT_EXIST` to pulls directed at it
        (that broker's own pull handling only accepts the request in its else branch when it itself knows this consume group;
        pulls with the default `post_subscription_when_pull=false` carry no subscription flag and take exactly this path).
        The producer side is the opposite: `producer.py::_send_heartbeat_to_all_broker` only carries ProducerData,
        i.e. its consumer set is empty, in which case only the master needs the heartbeat.
        """
        client = self._require_client()
        hb = self._build_heartbeat()
        if not self.use_heartbeat_v2 and self.enable_concurrent_heartbeat:
            ok = client.send_heartbeat_concurrently(hb, self.mq_client_api_timeout)
            if ok > 0:
                self._heartbeat_count += 1
            return ok
        ok = 0
        for addr in client.get_all_broker_addrs():
            try:
                # With use_heartbeat_v2 on the body carries a non-zero heartbeatFingerprint and the
                # round goes through the V2 state machine (client/heartbeat_v2.py): once a broker
                # answers IS_SUPPORT_HEART_BEAT_V2 and still holds our fingerprint, later rounds send
                # the cheap withoutSub body. A broker that never answers those extFields keeps getting
                # the full subscription set, so the plain HEART_BEAT(34) path stays intact.
                if self.use_heartbeat_v2:
                    client.send_heartbeat_to_broker_v2(
                        addr, hb, self.consumer_group, self.mq_client_api_timeout)
                else:
                    client.send_heartbeat(addr, hb, self.mq_client_api_timeout)
                ok += 1
            except Exception as e:  # noqa: BLE001
                logger.debug("heartbeat to %s failed: %s", addr, e)
        if ok > 0:
            self._heartbeat_count += 1
        return ok

    def heartbeat_count(self) -> int:
        """Number of successful heartbeat rounds (for live verification)."""
        return self._heartbeat_count

    def assigned_queue_count(self) -> int:
        """Number of queues currently assigned to this instance (for live verification of multi-instance allocation)."""
        return len(self._assigned_queues())

    def assigned_queue_keys(self) -> List[str]:
        """Key list of the currently assigned queues (for live verification that "two instances in the same group are disjoint and complete")."""
        return sorted(self._mq_key(mq) for mq in self._assigned_queues())

    def _start_heartbeat_loop(self) -> None:
        t = threading.Thread(target=self._heartbeat_loop, daemon=True,
                             name="rmq-heartbeat-%s" % self.consumer_group)
        t.start()
        self._heartbeat_thread = t

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            if self._stop.wait(self.heartbeat_interval_millis / 1000.0):
                break
            if not self.heartbeat_enabled or not self._started:
                continue
            try:
                self._send_heartbeat_to_all_broker()
            except Exception as e:  # noqa: BLE001
                logger.debug("heartbeat loop error: %s", e)

    # ---------------- Consume loops ----------------
    def _start_pull_loop(self) -> None:
        self._pulling = True
        # Concurrent long-polling semantics: the broker suspends a
        # long-poll request per queue and returns immediately once a message arrives -- so each queue must have its own pull thread,
        # otherwise one empty queue's long poll (~15s suspend) would block the delivery of the other queues.
        self._rebalance_pull_threads()

    def _rebalance_pull_threads(self) -> None:
        """Sync the pull thread set per the current queue allocation, and clean up state for revoked/stalled queues.

        When a queue is taken away, one must
        (1) persist that queue's **consumed** offset, (2) discard the queue's local buffer (in-flight messages are no longer consumed, left to the new owner to redeliver),
        (3) for ordered consumption in cluster mode also UNLOCK_BATCH_MQ(42). Missing any step means in-flight messages in a revoked queue are
        **still consumed by the old instance**, duplicating the new owner (the root cause of the extra duplicate messages in live test S6).

        The same pass also performs **self-healing**: queues still owned by this instance but whose pull has stalled beyond ``PULL_MAX_IDLE_TIME``
        are treated as revoked, immediately followed by rebuilding the pull with a fresh buffer, thereby rescuing dead/stuck consume loops.

        The **revoke first, then build** order is deliberate: the revoke's cleanup (persist offsets / UNLOCK) must happen
        **before** building threads; the reverse would have new loops start pulling at old offsets and write even smaller offsets back, adding duplicate deliveries for nothing.
        """
        current = {self._mq_key(mq): mq for mq in self._assigned_queues()}
        retired: List[Tuple[MessageQueue, Optional[int]]] = []
        pop = self.pop_mode
        with self._lock:
            # (1) Revoke: taken away + pull stalled
            for key in list(self._queue_threads.keys()):
                if key not in current:
                    self._retire_queue_locked(key, None, retired)
                    continue
                if self._started and self._pull_stalled_locked(key):
                    # The wording is the keyword for diagnosing "consumption stall self-healed" on site; log search matches on it, changing it breaks compatibility
                    logger.error("[BUG]doRebalance, %s, try remove unnecessary mq, %s, "
                                 "because pull is pause, so try to fixed it",
                                 self.consumer_group, key)
                    self._retire_queue_locked(key, current[key], retired)
        # The cleanup is done outside the lock (network RPC), and must come before (2) build
        if retired:
            self._on_queues_revoked(retired)
        with self._lock:
            # (2) Build: start loops for missing queues
            for key, mq in current.items():
                if key in self._queue_threads:
                    continue
                if pop and key not in self._pop_queues:
                    self._pop_queues[key] = PopProcessQueue()
                # A fresh buffer in place => unfreeze:
                # the rebuilt queue resumes advancing at the corrected offset
                self._frozen_offsets.discard(key)
                # The thread was just created and the loop has not reached the stamping point yet; use the current time as a placeholder to avoid a false stall verdict on the next pass
                self._last_pull_table[key] = time.time()
                # Once a queue is allocated it enters _mq_map (which is the authoritative view of "assigned"),
                # without waiting for the first message: offset persistence, 307 runtime info, and 220 reset all rely on this mapping.
                self._mq_map[key] = mq
                # POP mode: one POP loop per queue (no offset pull, no pull buffer built)
                target = self._queue_pop_loop if pop else self._queue_pull_loop
                t = threading.Thread(target=target, args=(mq,), daemon=True,
                                     name="rmq-%s-%s-%s" % ("pop" if pop else "pull",
                                                            self.consumer_group, key))
                self._queue_threads[key] = t
                t.start()

    def _retire_queue_locked(self, key: str, fallback_mq: Optional[MessageQueue],
                             retired: List[Tuple[MessageQueue, Optional[int]]]) -> None:
        """Discard all local state of a queue (the caller must hold the lock); the offset is persisted by the caller outside the lock.

        Once the entry in the thread table is deleted, the original loop thread's next ``_owns_queue`` fails and it exits by itself;
        ``_last_pull_table`` is cleared as well, to avoid inheriting the old timestamp when the same-named queue reuses a thread.
        ``fallback_mq`` is used by the self-healing branch: a stalled queue may have never pulled a message, so _mq_map may have no entry,
        but its consumed offset is real; missing the persist would make the new owner redeliver from the beginning.
        """
        self._queue_threads.pop(key, None)  # The loop detects this and exits
        self._last_pull_table.pop(key, None)
        mq = self._mq_map.pop(key, fallback_mq)
        self._pending.pop(key, None)
        # The in-flight registration is voided as well: the queue has been removed from the local queue table, the cleanup task (which only iterates the local queue table)
        # will not touch it again, and old batch references must not be left in the registration here either.
        self._inflight_msgs.pop(key, None)
        self._lock_ok.discard(key)
        # Epoch +1 => all in-flight batches' acks are voided; the frozen flag is **kept** until
        # the queue is rebuilt (_rebalance_pull_threads unfreezes), to prevent the corrected offset from being overwritten by an old ack.
        self._queue_epoch[key] = self._queue_epoch.get(key, 0) + 1
        off = self._consume_offsets.pop(key, None)
        self._offset_table.pop(key, None)
        # POP: mark as dropped, in-flight batches are neither consumed nor acked (leave them to the broker to revive)
        pq = self._pop_queues.pop(key, None)
        if pq is not None:
            pq.set_dropped(True)
        if mq is not None:
            retired.append((mq, off))

    def _pull_stalled_locked(self, key: str) -> bool:
        """Whether this queue's pull/pop loop has stalled. Must be called while holding the lock.

        Both criteria count as dead: the thread has exited (an exception broke through), or the thread is alive but has not initiated any pull for longer than ``PULL_MAX_IDLE_TIME``
        (stuck somewhere other than the lock/flow control/network). A new loop that has never stamped is not counted.
        """
        t = self._queue_threads.get(key)
        if t is not None and not t.is_alive():
            return True
        began = self._last_pull_table.get(key)
        if began is None:
            return False
        return time.time() - began > PULL_MAX_IDLE_TIME

    def _on_queues_revoked(self, revoked: List[Tuple[MessageQueue, Optional[int]]]) -> None:
        """Cleanup for revoked queues: persist offsets + unlock for ordered consumption."""
        broadcast = self.message_model == MessageModel.BROADCASTING
        if broadcast:
            # Broadcast mode offsets are stored locally only. Revoked offsets must be written back to the file together with the queue info **first**,
            # and only then removed from the in-memory entries (the values stay in the file);
            # without this step, the queue rebuild cannot read the old offset and would rescan from the start/end per consume_from_where.
            self._save_local_offsets(extra=revoked)
            return
        client = self._mq_client
        if client is None:
            return
        for mq, off in revoked:
            if off is not None:
                try:
                    client.update_consumer_offset(self.consumer_group, mq, off)
                except Exception as e:  # noqa: BLE001
                    logger.debug("persist offset on revoke failed for %s: %s", mq, e)
            # The per-queue unlock on revoke belongs only to classic ordered: it reads the
            # the queue's local buffer from the queue table -- in POP mode that table is empty, so this path never runs.
            if self._is_orderly() and not self.pop_mode:
                # Ordered consumption: release the broker queue lock so the new owner can take over immediately
                try:
                    client.unlock_batch_mq(self.consumer_group, self.client_id or "", [mq])
                except Exception as e:  # noqa: BLE001
                    logger.debug("unlock on revoke failed for %s: %s", mq, e)
        logger.info("queues revoked, group=%s count=%d", self.consumer_group, len(revoked))

    def _owns_queue(self, key: str) -> bool:
        """Whether this thread still owns the queue (invalid once rebalance takes it away or the pull thread changes)."""
        with self._lock:
            return self._queue_threads.get(key) is threading.current_thread()

    @staticmethod
    def _mq_key(mq: MessageQueue) -> str:
        return "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id)

    def _all_queues_of_topic(self, topic: str) -> List[MessageQueue]:
        """All queues of a topic (the subscription info view).

        Taken from the **subscription info** (``topicRouteData2TopicSubscribeInfo``: read perm + readQueueNums,
        no master filtering), not the publish info -- the two differ on the routes "read-only topic with perm=4" and "master down, only slaves left",
        and the consume side must use the wider of the two (publish info only serves producer queue selection).
        """
        client = self._require_client()
        queues = client.get_topic_subscribe_info(topic)
        if not queues:
            logger.debug("rebalance: no subscribe info for topic %s", topic)
        return queues

    def _assigned_queues(self) -> List[MessageQueue]:
        """The set of queues currently assigned to this instance (computed by _do_rebalance)."""
        with self._lock:
            return list(self._assigned)

    def _do_rebalance(self) -> None:
        """Compute this instance's queue allocation, then sync the pull thread set.

        BROADCASTING: all queues belong to self (no broker coordination).
        CLUSTERING: query the broker's consumer list -> sort -> allocation strategy -> take this instance's share.
        When the consumer list cannot be found, **keep the existing allocation** and only warn; never fall back to "owning all queues",
        otherwise multiple instances in the same group would consume each other's messages.
        """
        client = self._require_client()
        was = {self._mq_key(mq) for mq in self._assigned_queues()}
        assigned: List[MessageQueue] = []
        with self._lock:
            topics = list(self.subscription_data.keys())
        if self.message_model == MessageModel.BROADCASTING:
            for topic in topics:
                assigned.extend(self._all_queues_of_topic(topic))
        else:
            for topic in topics:
                mq_all = sorted(self._all_queues_of_topic(topic), key=_mq_sort_key)
                if not mq_all:
                    continue
                cid_all = client.get_consumer_id_list_by_group(topic, self.consumer_group)
                if not cid_all:
                    logger.debug("rebalance: no consumer id list for %s/%s, keep current",
                                 self.consumer_group, topic)
                    assigned.extend([mq for mq in self._assigned_queues() if mq.topic == topic])
                    continue
                try:
                    got = self.allocate_strategy.allocate(
                        self.consumer_group, self.client_id or "", mq_all, sorted(cid_all))
                except Exception as e:  # noqa: BLE001
                    logger.error("allocate message queue exception, strategy=%s: %s",
                                 _strategy_name(self.allocate_strategy), e)
                    return
                assigned.extend(got)
        with self._lock:
            self._assigned = assigned
        now = {self._mq_key(mq) for mq in assigned}
        if now != was:
            logger.info("rebalance result changed, group=%s clientId=%s assigned=%d",
                        self.consumer_group, self.client_id, len(assigned))
            # An assignment change on the push side drops the fingerprints remembered per
            # broker (the heartbeat V2 state), i.e. it forces
            # the next heartbeat round to re-deliver the subscriptions to every broker instead of
            # the cheap withoutSub body. Only the push rebalance does this -- the
            # lite-pull consumer's assignment change leaves the fingerprints alone.
            if self.use_heartbeat_v2:
                client.reset_heartbeat_v2_fingerprint_table(self.consumer_group)
        # Newly assigned queues resolve their initial offset **immediately** (clear the dirty offset -> compute the start per consume_from_where -> write into the offset table).
        # It cannot be left to lazy resolution at first pull: CONSUME_FROM_LAST_OFFSET's semantics are "the latest offset at the allocation moment",
        # and lazy resolution would skip all messages produced between "after allocation" and "before first pull" (on live systems this presents as the consumer never receiving anything).
        for mq in assigned:
            key = self._mq_key(mq)
            if key in was:
                continue
            with self._lock:
                if key in self._offset_table:
                    continue
                sub = self.subscription_data.get(mq.topic)
            if sub is None:
                continue
            try:
                off = self._resolve_initial_offset(client, mq)
            except Exception as e:  # noqa: BLE001
                logger.debug("resolve initial offset for %s failed: %s", mq, e)
                continue
            with self._lock:
                self._offset_table.setdefault(key, off)
        self._rebalance_pull_threads()

    def rebalance_immediately(self) -> None:
        """Wake this consumer's rebalance loop after the instance receives the broker's 40 notification.

        Only sets an event to wake the background rebalance loop; does not compute the allocation directly in the notification thread.
        """
        self._rebalance_now.set()

    def _rebalance_loop(self) -> None:
        """Recompute the allocation periodically (default 20s), or immediately when notified.

        During startup and when **there is currently no allocation at all**, shorten to a 2s retry: the consumer may start before the topic is created
        (under ``autoCreateTopicEnable`` the broker creates the topic on the producer's first send), in which case the real route cannot yet be
        pulled -- the consumer does no default-topic fallback (see MQClientInstance.update_topic_route_info_
        from_name_server), and waiting 20s would mean not consuming for a long time. The fast retry only takes effect within 60s of startup,
        to avoid clients that long-term subscribe to a nonexistent topic hammering the NameServer at high frequency.
        """
        while not self._stop.is_set():
            starting_up = (time.time() - self._start_time) < 60.0
            interval = 2.0 if (starting_up and not self._assigned) else 20.0
            self._rebalance_now.wait(interval)
            self._rebalance_now.clear()
            if self._stop.is_set() or not self._started:
                break
            try:
                self._do_rebalance()
            except Exception as e:  # noqa: BLE001
                logger.debug("rebalance error: %s", e)

    def _queue_pull_loop(self, mq: MessageQueue) -> None:
        """Single-queue pull loop: long-poll pull -> push into the pending buffer."""
        client = self._require_client()
        orderly = self._is_orderly()
        key = self._mq_key(mq)
        while not self._stop.is_set() and self._started:
            if not self._owns_queue(key):
                return
            # Stamp the time on every pull **initiation**,
            # before the flow-control/lock checks: the criterion is "this loop is still running", not "this round really hit the network".
            with self._lock:
                self._last_pull_table[key] = time.time()
            # Suspend gate.
            # WARNING: order matters: the gate must come **after stamping**. Placed before stamping, a consumer that is merely
            # suspended by suspend() would, having not initiated a pull for longer than PULL_MAX_IDLE_TIME, be treated as a dead loop
            # by the stall criterion (_pull_stalled_locked),
            # and rebalance would take the allocation away and rebuild it directly -- so the pause check must come after stamping.
            if self._paused:
                time.sleep(PULL_TIME_DELAY_WHEN_SUSPEND)
                continue
            with self._lock:
                sub = self.subscription_data.get(mq.topic)
            if sub is None:
                return
            # Ordered consumption: queues not confirmed locked by the broker (LOCK_BATCH_MQ) are not pulled
            if orderly and key not in self._lock_ok:
                time.sleep(0.2)
                continue
            # Flow control: count / bytes / topic-level accumulation / concurrency span
            # pause this queue's pull when any limit is exceeded
            if self._flow_control_hit(mq, key):
                time.sleep(0.1)
                continue
            offset = self._offset_table.get(key)
            if offset is None:
                try:
                    offset = self._resolve_initial_offset(client, mq)
                except Exception as e:  # noqa: BLE001
                    logger.debug("resolve initial offset failed for %s: %s", mq, e)
                    time.sleep(1.0)
                    continue
                self._offset_table[key] = offset
            try:
                # Only when post_subscription_when_pull is on and not class-filter mode is the subscription expression
                # sent to the broker; not sent by default, with filtering falling back to the client's tag filtering.
                sub_expression = sub.sub_string if (
                    self.post_subscription_when_pull and not sub.class_filter_mode) else None
                sys_flag = PullSysFlag.build_sys_flag(commit_offset=False,
                                                      suspend=True,
                                                      subscription=sub_expression is not None,
                                                      class_filter=False)
                # Choose the master/slave node to pull from per the previous round's suggestWhichBrokerId
                broker_id = self._pull_from_which_node.get(mq, MixAll.MASTER_ID)
                pull_began = time.time()
                result = client.pull_message(self.consumer_group, mq, offset,
                                             self.pull_batch_size, sys_flag, 0,
                                             sub_expression, sub.sub_version,
                                             sub.expression_type,
                                             timeout_millis=self.pull_timeout_millis,
                                             max_msg_bytes=self.pull_batch_size_in_bytes,
                                             suspend_timeout_millis=self.pull_suspend_timeout_millis,
                                             broker_id=broker_id)
                # Update ``_pull_from_which_node`` on every response:
                # when the response has no suggestWhichBrokerId, default to master=0
                self._pull_from_which_node[mq] = result.suggest_which_broker_id \
                    if result.suggest_which_broker_id is not None else MixAll.MASTER_ID
                # Consumption stats: pull RT is recorded every time, TPS only when there are messages
                if self._stats_manager is not None:
                    self._stats_manager.inc_pull_rt(self.consumer_group, mq.topic,
                                                    int((time.time() - pull_began) * 1000))
                    if result.status == PullStatus.FOUND and result.msg_found_list:
                        self._stats_manager.inc_pull_tps(self.consumer_group, mq.topic,
                                                         len(result.msg_found_list))
            except RemotingTimeoutException as e:
                # A long poll timing out with no new message during suspend is normal behavior: the broker
                # clamps the suspend time to its own configured maximum (default ~15s),
                # ignoring the client's suspend_timeout_millis, so idle queues time out periodically.
                # This is not an error, debug level only, to avoid polluting the client's runtime logs (see logging.py).
                logger.debug("pull long-poll timeout for %s (benign, will retry): %s", mq, e)
                continue
            except MQBrokerException as e:
                # Broker-side non-SUCCESS codes (FLOW_CONTROL / TOPIC_NOT_EXIST / PULL_NOT_FOUND, etc.):
                # mostly expected paths like a topic not yet being created; debug + brief backoff to avoid a hot loop.
                # The broker's FLOW_CONTROL(215) gets its own short retry: it is transient producer-side
                # throttling, distinct from this client's own buffer thresholds (_flow_control_hit),
                # and neither belongs in the generic error backoff.
                logger.debug("pull error for %s: %s: %s", mq, type(e).__name__, e)
                if e.response_code == ResponseCode.FLOW_CONTROL:
                    time.sleep(PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL)
                else:
                    time.sleep(PULL_TIME_DELAY_WHEN_EXCEPTION)
                continue
            except Exception as e:  # noqa: BLE001
                logger.debug("pull error for %s: %s: %s", mq, type(e).__name__, e)
                time.sleep(PULL_TIME_DELAY_WHEN_EXCEPTION)
                continue

            # Client-side filtering before delivery:
            # first second-pass tag filtering, then run FilterMessageHook. **Must be done before taking _lock** --
            # hooks are user code and may block, so they must not be pressed into the enqueue critical section.
            # Messages dropped on the pull path are not acked: the offset advances as usual = silently skipped.
            if result.status == PullStatus.FOUND and result.msg_found_list:
                result.msg_found_list = self._filter_messages_for_delivery(
                    mq, sub, result.msg_found_list)

            # The enqueue and the "does this thread still own the queue" check must be atomic: queues taken away by rebalance during the long poll
            # have this batch **discarded directly** -- not consumed, offset not advanced,
            # left to the new owner to redeliver from our last persisted offset, otherwise the two instances would consume the same message.
            illegal = False
            with self._lock:
                if self._queue_threads.get(key) is not threading.current_thread():
                    logger.debug("queue %s revoked during pull, discard %d fetched messages",
                                 mq, len(result.msg_found_list or ()))
                    return
                if key not in self._pending:
                    self._pending[key] = deque()
                    self._mq_map[key] = mq
                if result.status == PullStatus.FOUND and result.msg_found_list:
                    self._pending[key].extend(result.msg_found_list)
                    self._update_msg_acc_cnt(key, result.msg_found_list)
                # The pull cursor advances to nextBeginOffset; the "consumed offset" is tracked and persisted separately by _consume_offsets
                if result.next_begin_offset is not None:
                    self._offset_table[key] = result.next_begin_offset
                    if result.status == PullStatus.OFFSET_ILLEGAL:
                        # When the offset is corrected by the broker
                        # correction and rebuild must be done together, see _offset_illegal_recover.
                        # Freezing happens first inside the lock (equivalent to the update-and-freeze done synchronously in the callback),
                        # with the queue-revoke and disk-persist RPCs moved outside the lock.
                        self._frozen_offsets.add(key)
                        self._consume_offsets[key] = int(result.next_begin_offset)
                        illegal = True
                    else:
                        # On an empty response (NO_NEW_MSG /
                        # NO_MATCHED_MSG), correct the "consumed offset", see _correct_tags_offset_locked
                        self._correct_tags_offset_locked(key, result.status,
                                                         result.next_begin_offset)
            if illegal:
                self._offset_illegal_recover(key)

    def _update_msg_acc_cnt(self, key: str, msgs: List[MessageExt]) -> None:
        """The computation of ``msg_acc_cnt`` (backlog count).

            acc_total = MAX_OFFSET property - this message's queueOffset
            if (acc_total > 0) msg_acc_cnt = acc_total

        Takes the **last message of this batch**; feeds the threshold comparison of ``adjust_thread_pool``.
        """
        if not msgs:
            return
        last = msgs[-1]
        prop = last.get_property(MessageConst.PROPERTY_MAX_OFFSET)
        if prop is None:
            return
        try:
            acc_total = int(prop) - int(last.queue_offset)
        except (TypeError, ValueError):
            return
        if acc_total > 0:
            with self._lock:
                self._msg_acc_cnt_table[key] = acc_total

    def msg_acc_cnt(self, key: Optional[str] = None) -> int:
        """Read ``msg_acc_cnt``: reads a single queue when a key is given, sums otherwise."""
        with self._lock:
            if key is None:
                return sum(self._msg_acc_cnt_table.values())
            return int(self._msg_acc_cnt_table.get(key, 0))

    # ---------------------------------------------------------------- POP consume loop

    def _queue_pop_loop(self, mq: MessageQueue) -> None:
        """Single-queue POP loop: pop -> deliver -> ack confirmation.

        Key differences from the pull loop:
          - **Does not query or commit consume offsets**: progress is tracked by the broker-side checkpoint, confirmation is by ack only;
          - pops are delivered to consume threads immediately, and the loop continues this round (without waiting for the consume result);
          - ``POLLING_NOT_FOUND`` (the queue has no messages for now) is a **normal state**, go to the next round directly, not an error.
        """
        client = self._require_client()
        key = self._mq_key(mq)
        invisible = self.pop_invisible_time
        if invisible < MIN_POP_INVISIBLE_TIME or invisible > MAX_POP_INVISIBLE_TIME:
            # Clamp: anything outside [5s, 300s] falls back to 60s
            invisible = 60000
        # The broker's default POP init mode is MAX (start at the tail); here it maps per consume_from_where,
        # so the "consume from the beginning" semantics hold in POP mode too.
        init_mode = (CONSUME_INIT_MODE_MIN
                     if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
                     else CONSUME_INIT_MODE_MAX)
        while not self._stop.is_set() and self._started:
            if not self._owns_queue(key):
                return
            pq = self._pop_queues.get(key)
            if pq is None or pq.is_dropped():
                return
            # The stall criterion reads last_pop_timestamp, which is stamped at pop **initiation**
            # (the loop entry): flow control / long-poll suspension must not
            # make a still-running loop be judged as stalled.
            with self._lock:
                now = time.time()
                self._last_pull_table[key] = now
            pq.last_pop_timestamp = now
            # POP mode is subject to the same suspend gate,
            # and likewise falls after stamping (see the comment in _queue_pull_loop).
            if self._paused:
                time.sleep(PULL_TIME_DELAY_WHEN_SUSPEND)
                continue
            with self._lock:
                sub = self.subscription_data.get(mq.topic)
            if sub is None:
                return
            # Flow control: too many popped-but-unacked, back off first (pop_threshold_for_queue)
            if pq.wait_ack_count() > self.pop_threshold_for_queue:
                time.sleep(0.05)
                continue
            began = time.time()
            try:
                result = client.pop_message(
                    self.consumer_group, mq.topic, mq.queue_id,
                    max_msg_nums=self.pop_batch_nums,
                    invisible_time=invisible,
                    poll_time=self.pop_poll_time_millis,
                    init_mode=init_mode,
                    exp=sub.sub_string or "*",
                    exp_type=sub.expression_type,
                    timeout_millis=self.pop_timeout_millis,
                    broker_name=mq.broker_name,
                )
            except RemotingTimeoutException:
                # No message during long-poll suspension -> the client times out first; normal behavior, go to the next round directly
                logger.debug("pop long-poll timeout for %s (benign, will retry)", mq)
                continue
            except MQBrokerException as e:  # noqa: BLE001
                # Same split as the pull loop: the broker's FLOW_CONTROL(215) is transient throttling
                # and retries quickly; every other broker error keeps the generic backoff.
                logger.debug("pop error for %s: %s: %s", mq, type(e).__name__, e)
                if e.response_code == ResponseCode.FLOW_CONTROL:
                    time.sleep(PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL)
                else:
                    time.sleep(PULL_TIME_DELAY_WHEN_EXCEPTION)
                continue
            except Exception as e:  # noqa: BLE001
                logger.debug("pop error for %s: %s: %s", mq, type(e).__name__, e)
                time.sleep(PULL_TIME_DELAY_WHEN_EXCEPTION)
                continue

            # The queue is revoked by rebalance after popping: this batch is **neither consumed nor acked**
            # (the queue is already dropped), leaving it to the broker to
            # automatically revive and redeliver to the new owner once invisibleTime expires.
            if not self._owns_queue(key) or pq.is_dropped():
                logger.debug("queue %s revoked during pop, discard %d messages un-acked",
                             mq, len(result.msg_found_list or ()))
                return
            found = result.status == PopStatus.FOUND
            if found:
                # Consumption stats: POP path
                # like the pull path, RT/TPS must be recorded into the stats table, otherwise the POP consumer's
                # the running-info report would always show 0 on the pull side -- on ops dashboards "this consumer is not
                # pulling" and "this consumer did not start at all" would be indistinguishable. RT is recorded at the FOUND branch entry
                # (before the null check), TPS only by the number actually popped.
                if self._stats_manager is not None:
                    self._stats_manager.inc_pull_rt(self.consumer_group, mq.topic,
                                                    int((time.time() - began) * 1000))
                    if result.msg_found_list:
                        self._stats_manager.inc_pull_tps(self.consumer_group, mq.topic,
                                                         len(result.msg_found_list))
            if found and result.msg_found_list:
                pq.inc_found_msg(len(result.msg_found_list))
                # Pre-delivery filtering: the POP path **must ack the dropped ones**,
                # otherwise the broker would revive and redeliver them once invisibleTime expires -- presenting as "filtering not in effect".
                kept = self._filter_messages_for_delivery(mq, sub, result.msg_found_list)
                if len(kept) != len(result.msg_found_list):
                    kept_ids = {id(m) for m in kept}
                    for msg in result.msg_found_list:
                        if id(msg) not in kept_ids:
                            self._ack_pop_msg(msg)
                            pq.ack()
                    logger.info("pop filter dropped %d of %d messages (acked)",
                                len(result.msg_found_list) - len(kept),
                                len(result.msg_found_list))
                if kept:
                    self._submit_pop_consume_request(kept, pq, mq)
            else:
                # Empty result: if the broker did not suspend per poll_time (returns immediately) this becomes a hot loop;
                # back off here per "this round was too short" to avoid hammering the broker.
                if (time.time() - began) * 1000.0 < 200:
                    time.sleep(0.2)
            # NO_NEW_MSG / POLLING_NOT_FOUND / POLLING_FULL all go directly to the next round

    def _submit_pop_consume_request(self, msgs: List[MessageExt], pq: PopProcessQueue,
                                    mq: MessageQueue) -> None:
        """Select the service by listener type: orderly listener + POP goes through the POP orderly path.

        This path is an **incomplete skeleton**: after the request is deduplicated and enqueued, run() returns as soon as it gets the queue lock -- the messages
        are **neither consumed nor acked**, and invisibleTime expiry leaves them to the broker to revive and redeliver, presenting macroscopically as
        "orderly + POP receives no messages and the backlog never clears". Keep this behavior, do not "fix" it: the orderly semantics of the POP path
        (queue lock, COMMIT/ROLLBACK) are not yet settled. msgs are ignored on this path (the request carries only pq + mq).

        Concurrent listeners go through ``_consume_pop_batch`` (see below).
        """
        if self._is_orderly():
            self._submit_pop_orderly_request(pq, mq)
            return
        size = max(1, self.consume_message_batch_max_size)
        batches = [msgs[i:i + size] for i in range(0, len(msgs), size)] or [msgs]
        for batch in batches:
            if not batch:
                continue
            if self._pop_executor is not None:
                self._pop_executor.submit(self._consume_pop_batch, batch, pq, mq)
            else:
                # Executor not started (unit test or not started): execute synchronously
                self._consume_pop_batch(batch, pq, mq)

    def _submit_pop_orderly_request(self, pq: PopProcessQueue, mq: MessageQueue,
                                    force: bool = False) -> None:
        """Submit a POP orderly request: deduplicate under the lock, only new requests go to the executor.

        Each key's lock is folded into self._lock: the critical section is only a single set operation.
        """
        with self._lock:
            is_new = (pq, mq) not in self._pop_orderly_requests
            if is_new:
                self._pop_orderly_requests.add((pq, mq))
            if not force and not is_new:
                return
        if self._pop_executor is not None:
            try:
                self._pop_executor.submit(self._run_pop_orderly_request, pq, mq)
            except Exception as e:  # noqa: BLE001
                # Submission failure only logs; the request stays in the set (not auto-redelivered).
                logger.error("error submit pop orderly request: %s, mq: %s", e, mq)
        else:
            # No executor started (unit test or not started): execute synchronously
            self._run_pop_orderly_request(pq, mq)

    def _run_pop_orderly_request(self, pq: PopProcessQueue, mq: MessageQueue) -> None:
        """The execution body of a POP orderly request (the skeleton ends here):
        remove the request and log a warning only when pq is revoked;
        otherwise get the queue lock and **do nothing**. The dedup set is the vehicle for "the same request is not processed twice".
        """
        if pq.is_dropped():
            logger.warning("run, message queue not be able to consume, because it's dropped. %s",
                           mq)
            with self._lock:
                self._pop_orderly_requests.discard((pq, mq))
            return

    def pop_orderly_request_count(self) -> int:
        """Number of entries in the request set (the dedup behavior must be locked down offline)."""
        with self._lock:
            return len(self._pop_orderly_requests)

    def _consume_pop_batch(self, msgs: List[MessageExt], pq: PopProcessQueue,
                           mq: MessageQueue) -> None:
        """Consume one POP batch and ack / extend invisibility per the result.

        This is the consume entry point of the POP concurrent path.
        """
        if pq.is_dropped() or not msgs:
            return
        pop_time = 0
        invisible = 0
        try:
            seg = split(msgs[0].get_property(MessageConst.PROPERTY_POP_CK))
            pop_time = extra_info_util.get_pop_time(seg)
            invisible = extra_info_util.get_invisible_time(seg)
        except Exception:  # noqa: BLE001
            logger.debug("parse pop ck failed for %s, treat as not timed out", mq)

        if self._is_pop_timeout(msgs, pop_time, invisible):
            # Already past invisibleTime: the ack would not be accepted either; give up this batch directly (wait for the broker to revive and redeliver)
            logger.debug("pop timeout, abort consume for %s: popTime=%s invisible=%s",
                         mq, pop_time, invisible)
            pq.dec_found_msg(-len(msgs))
            return

        self._reset_retry_topic_and_namespace(msgs)
        context = ConsumeConcurrentlyContext(mq)
        # ackIndex's default value is Integer.MAX_VALUE (this port's default
        # already is it; explicitly clamping to size-1 here only saves one clamp): CONSUME_SUCCESS acks everything by default.
        # If written as -1 here, nothing would be acked, and the messages would be revived and redelivered by the broker once invisibleTime expires --
        # under a short observation window it would masquerade as a pass.
        context.ack_index = len(msgs) - 1
        hook_ctx = None
        if self.consume_message_hook_list:
            hook_ctx = self._build_consume_hook_context(msgs, mq)
            self.execute_consume_hook_before(hook_ctx)
        begin_ms = time.time() * 1000
        has_exception = False
        try:
            # The POP concurrent path likewise stamps CONSUME_START_TIME on each message
            # before handing to the listener (POP has no expired-message sweep,
            # but this property is visible to the listener/trace; the policy must match classic concurrent)
            for m in msgs:
                MessageAccessor.set_consume_start_timestamp(m, int(time.time() * 1000))
            status = self.message_listener.consume_message(msgs, context)
        except Exception as e:  # noqa: BLE001
            # An exception from consumption is treated as RECONSUME_LATER
            logger.debug("pop listener error, treat as RECONSUME_LATER: %s", e)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
            has_exception = True
        # A listener returning null is likewise treated as RECONSUME_LATER (the same normalization as classic concurrent).
        # The status **before** normalization is left to returnType (returning null records RETURNNULL,
        # not FAILED); TPS and the hook context both use the **post**-normalization value
        # -- a listener returning null counts as **failed**, not successful
        raw_status = status
        if status is None:
            logger.warning("consumeMessage return null, Group: %s Msgs: %d MQ: %s",
                           self.consumer_group, len(msgs), mq)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
        self._record_consume_stats(mq.topic, len(msgs), begin_ms,
                                   failed=status == ConsumeConcurrentlyStatus.RECONSUME_LATER)
        self._finish_consume_hook(
            hook_ctx, raw_status, has_exception, begin_ms,
            failed=status == ConsumeConcurrentlyStatus.RECONSUME_LATER,
            succeeded=status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
            hook_status=status)

        if pq.is_dropped() or self._is_pop_timeout(msgs, pop_time, invisible):
            # The queue was revoked during consumption or has timed out: the result is no longer processed
            pq.dec_found_msg(-len(msgs))
            return
        self._process_pop_consume_result(status, context, msgs, pq, mq)

    @staticmethod
    def _is_pop_timeout(msgs: List[MessageExt], pop_time: int, invisible: int) -> bool:
        """Treated as timed out when popTime/invisibleTime cannot be parsed."""
        if not msgs or pop_time <= 0 or invisible <= 0:
            return True
        return int(time.time() * 1000) - pop_time >= invisible

    def _process_pop_consume_result(self, status, context: ConsumeConcurrentlyContext,
                                    msgs: List[MessageExt], pq: PopProcessQueue,
                                    mq: MessageQueue) -> None:
        """Decide by ackIndex which message to ack up to; the rest have their invisibility extended or are dropped."""
        ack_index = context.ack_index
        if status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
            if ack_index >= len(msgs):
                ack_index = len(msgs) - 1
        else:
            # RECONSUME_LATER: ack nothing
            ack_index = -1

        for i in range(0, ack_index + 1):
            self._ack_pop_msg(msgs[i])
            pq.ack()

        for i in range(ack_index + 1, len(msgs)):
            pq.ack()
            msg = msgs[i]
            # Exceeding the max retry count: go through _check_need_ack_or_delay (too old -> ack and drop directly,
            # otherwise pick a delay level per the message's lifetime)
            if self.max_reconsume_times >= 0 and msg.reconsume_times >= self.max_reconsume_times:
                self._check_need_ack_or_delay(msg)
                continue
            self._change_pop_invisible_time(msg, context.delay_level_when_next_consume)

    def _check_need_ack_or_delay(self, msg: MessageExt) -> None:
        """The fallback after retries are exhausted.

        If the message lifetime already exceeds twice the max delay level -> ack and drop directly (no infinite retries);
        otherwise pick a level per the lifetime to keep extending invisibility.
        """
        table = self.pop_delay_level
        msg_delay_time = int(time.time() * 1000) - msg.born_timestamp
        if msg_delay_time > table[-1] * 1000 * 2:
            logger.warning("pop consume too many times, ack and drop: key=%s", msg.get_keys())
            self._ack_pop_msg(msg)
            return
        level = len(table) - 1
        while level >= 0:
            if msg_delay_time >= table[level] * 1000:
                level += 1
                break
            level -= 1
        self._change_pop_invisible_time(msg, level)

    def _pop_ck_target(self, msg: MessageExt) -> Optional[Tuple[str, str, int, int, str]]:
        """Resolve (topic, brokerName, queueId, offset, ck) from POP_CK for ack/extending invisibility.

        WARNING: neither place can be taken for granted:
          1. topic must be restored by ``ExtraInfoUtil.getRealTopic`` per the CK's retryFlag -- the real topic of a revived message
             (retryFlag=1) is ``%RETRY%<group>_<topic>``, **not** the topic on the message;
          2. the address must be reverse-looked-up by the CK's brokerName, not by the topic's route -- the retry topic usually has no
             independent route entry, so looking up by topic would fail; it must go through the subscribe/publish address tables.
        """
        ck = msg.get_property(MessageConst.PROPERTY_POP_CK)
        if not ck:
            logger.debug("pop message without POP_CK, cannot ack: %s", msg.msg_id)
            return None
        try:
            seg = split(ck)
            broker_name = extra_info_util.get_broker_name(seg)
            queue_id = extra_info_util.get_queue_id(seg)
            offset = extra_info_util.get_queue_offset(seg)
            retry = extra_info_util.get_retry(seg)
        except Exception as e:  # noqa: BLE001
            logger.debug("bad POP_CK %r: %s", ck, e)
            return None
        topic = extra_info_util.get_real_topic(msg.topic, self.consumer_group, retry)
        return topic, broker_name, queue_id, offset, ck

    def _ack_pop_msg(self, msg: MessageExt) -> None:
        """Single ack: locate the broker by POP_CK, then send an ACK_MESSAGE(200051) request."""
        target = self._pop_ck_target(msg)
        if target is None:
            return
        topic, broker_name, queue_id, offset, ck = target
        try:
            client = self._require_client()
            # Address resolution is (brokerName, MASTER_ID, allow route refresh):
            # **master only**; if one route refresh still cannot get it, throw "The broker[X] not exist".
            # Static topic: the mock broker name is first rewritten by the route's broker-endpoint table to the real carrier
            # (using the checkpoint's own queueId -- each globalId may land on a different broker).
            client.ack_message(self.consumer_group, topic, queue_id, ck, offset,
                               broker_name=broker_name,
                               addr=client.pop_ack_addr(broker_name, topic, queue_id))
        except Exception as e:  # noqa: BLE001
            # A failed ack is not fatal: the message will be revived and redelivered by the broker once invisibleTime expires
            logger.debug("ack failed for %s: %s", msg.msg_id, e)

    def _change_pop_invisible_time(self, msg: MessageExt, delay_level: int) -> None:
        """Extend the invisibility time.

        When ``delay_level == 0``, use the message's retry count as the level; the level table is in **seconds**, the interface takes milliseconds.
        """
        target = self._pop_ck_target(msg)
        if target is None:
            return
        topic, broker_name, queue_id, offset, ck = target
        if delay_level == 0:
            delay_level = msg.reconsume_times
        table = self.pop_delay_level
        delay_second = table[-1] if delay_level >= len(table) else table[max(0, delay_level)]
        try:
            client = self._require_client()
            # Same "master only" + one route refresh
            # mock name rewriting is the same as the ack path.
            client.change_invisible_time(self.consumer_group, topic, queue_id, ck, offset,
                                         delay_second * 1000,
                                         broker_name=broker_name,
                                         addr=client.pop_ack_addr(broker_name, topic, queue_id))
        except Exception as e:  # noqa: BLE001
            logger.debug("change invisible time failed for %s: %s", msg.msg_id, e)

    def _flow_control_hit(self, mq: MessageQueue, key: str) -> bool:
        """Whether flow control is triggered (five threshold checks; any hit pauses this queue's pull).

        - ``pull_threshold_for_queue``: this queue's pulled-but-unconsumed **count** (default 1000)
        - ``pull_threshold_size_for_queue``: this queue's pulled-but-unconsumed **bytes in MB** (default 100)
        - ``pull_threshold_for_topic`` / ``pull_threshold_size_for_topic``: accumulation across all queues of the same topic (-1 disables)
        - ``consume_concurrently_max_span``: the **span** of queueOffsets of pulled-but-unconsumed messages (default 2000,
          prevents "one message keeps failing while the rest pile up" from making the offset span run away)
        """
        with self._lock:
            dq = list(self._pending.get(key, ()))
        size_mb = 0.0
        span = 0
        for m in dq:
            size_mb += getattr(m, "store_size", 0)
        size_mb /= (1024.0 * 1024.0)
        if dq:
            offsets = [m.queue_offset for m in dq]
            span = max(offsets) - min(offsets)
        reason = ""
        if len(dq) >= max(1, self.pull_threshold_for_queue):
            reason = "count=%d" % len(dq)
        elif self.pull_threshold_size_for_queue > 0 and size_mb >= self.pull_threshold_size_for_queue:
            reason = "size=%.1fMB" % size_mb
        elif self.consume_concurrently_max_span > 0 and span > self.consume_concurrently_max_span:
            reason = "span=%d" % span
        elif self.pull_threshold_for_topic > 0 or self.pull_threshold_size_for_topic > 0:
            with self._lock:
                topic_pending = [m for k, q in self._pending.items()
                                 if k in self._mq_map and self._mq_map[k].topic == mq.topic
                                 for m in q]
            if (self.pull_threshold_for_topic > 0
                    and len(topic_pending) >= self.pull_threshold_for_topic):
                reason = "topicCount=%d" % len(topic_pending)
            elif self.pull_threshold_size_for_topic > 0:
                topic_mb = sum(getattr(m, "store_size", 0) for m in topic_pending) / (1024.0 * 1024.0)
                if topic_mb >= self.pull_threshold_size_for_topic:
                    reason = "topicSize=%.1fMB" % topic_mb
        if not reason:
            return False
        self._flow_control_triggered += 1
        logger.debug("flow control: queue %s %s, pause pull", mq, reason)
        return True

    def _resolve_initial_offset(self, client: MQClientInstance,
                                mq: MessageQueue) -> int:
        # Two invariants hold this method together:
        # 1. the start offset NEVER depends on the subscription type. An earlier draft
        #    short-circuited SQL92 subscriptions to the max offset, which silently
        #    skipped every message produced *before* the consumer started (send-first +
        #    CONSUME_FROM_FIRST_OFFSET received nothing; consumer-first only passed
        #    because the queues were empty and the max offset was 0 anyway). The broker
        #    applies the SQL92 filter to whatever range the pull covers, so the policy
        #    alone decides the start.
        # 2. broadcasting and clustering differ ONLY in where the stored offset is read
        #    from (local file vs broker). Both fall through to the same policy switch
        #    when nothing is stored, so a CONSUME_FROM_TIMESTAMP or %RETRY% start can
        #    never become mode-dependent.
        stored = -1
        if self.message_model == MessageModel.BROADCASTING:
            # Broadcast mode: offsets are stored in the local file only
            local = self._load_local_offsets().get(
                "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id))
            if local is not None and local >= 0:
                stored = local
        else:
            # Cluster mode: first query the committed offset on the broker
            try:
                remote = client.query_consumer_offset(self.consumer_group, mq,
                                                      set_zero_if_not_found=False)
                if remote is not None and remote >= 0:
                    stored = remote
            except Exception as e:  # noqa: BLE001
                logger.debug("query consumer offset for %s not found: %s", mq, e)

        if stored >= 0:
            return stored

        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
            # FIRST_OFFSET with nothing stored gives 0 directly -- for both modes (the
            # local file and broker paths share this branch), and the comment policy is
            # "the offset will be fixed by the OFFSET_ILLEGAL process":
            # **do not** send a minOffset query. The cost of this extra shot is not just slowness:
            # minOffset is a master-only broker query, and while the master is down it is exactly "a newly started consumer
            # cannot pull anything" -- while slaves could already start pulling at 0, with the broker using
            # PULL_OFFSET_MOVED to correct the offset when out of bounds (_offset_illegal_recover).
            return 0

        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
            if NamespaceUtil.is_retry_topic(mq.topic):
                # A %RETRY% topic has no meaningful "time of the oldest retry" and every
                # queued retry must be delivered, so it starts at the queue tail rather
                # than searching backwards from a timestamp.
                return client.get_max_offset(mq)
            return client.search_offset_by_timestamp(mq, self._consume_timestamp_millis())

        # Default CONSUME_FROM_LAST_OFFSET: a %RETRY% topic starts at 0 (retry messages
        # must be fully retried) instead of skipping from the max offset.
        if NamespaceUtil.is_retry_topic(mq.topic):
            return 0
        return client.get_max_offset(mq)

    # ---------------- Dispatch and consume ----------------
    def _start_dispatch_loop(self) -> None:
        t = threading.Thread(target=self._dispatch_loop, daemon=True,
                             name="rmq-dispatch-%s" % self.consumer_group)
        t.start()
        self._dispatch_thread = t

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            progressed = False
            with self._lock:
                keys = list(self._pending.keys())
            for key in keys:
                if self._stop.is_set() or not self._started:
                    return
                # Taking the value and "does this thread still own the queue" must complete within the same lock: key is a snapshot from the start of this round,
                # and the queue may already have been taken away by rebalance (in which case the buffer has been cleaned and it must not be consumed)
                with self._lock:
                    mq = self._mq_map.get(key)
                    dq = self._pending.get(key)
                    if mq is None or dq is None or not dq:
                        continue
                    n = min(len(dq), max(1, self.consume_message_batch_max_size))
                    batch = [dq.popleft() for _ in range(n)]
                    # Register in-flight in the same critical section as "taking the batch": _correct_tags_offset_locked's gate
                    # relies on it to distinguish "the message is still in the buffer" and "the message is in the listener's hands" -- neither counts as empty.
                    self._msg_queue_inflight[key] = self._msg_queue_inflight.get(key, 0) + 1
                    # Take this batch's queue-buffer epoch along with it: compare before consuming/acking,
                    # and void the whole batch on mismatch
                    epoch = self._queue_epoch.get(key, 0)
                if not batch:
                    continue
                try:
                    done = self._consume_batch(key, mq, batch, epoch)
                    progressed = progressed or done
                except Exception as e:  # noqa: BLE001
                    # Unexpected exception on the dispatch path: push the batch back to the head of the queue and retry shortly (do not let it kill the dispatch thread)
                    logger.error("dispatch batch error (will retry): %s: %s", type(e).__name__, e)
                    with self._lock:
                        dq2 = self._pending.get(key)
                        if dq2 is not None:
                            for m in reversed(batch):
                                dq2.appendleft(m)
                    time.sleep(0.1)
                finally:
                    with self._lock:
                        left = self._msg_queue_inflight.get(key, 0) - 1
                        if left > 0:
                            self._msg_queue_inflight[key] = left
                        else:
                            self._msg_queue_inflight.pop(key, None)
            if not progressed:
                time.sleep(0.05)

    def _reset_retry_topic_and_namespace(self, msgs: List[MessageExt]) -> None:
        """Restore the retry topic and namespace (called before dispatch).

        Redelivered messages actually live under ``%RETRY%consumerGroup``, and the broker writes the original topic into
        the ``RETRY_TOPIC`` property. It must be used to restore the topic **before** handing to the listener,
        so the listener sees the business's original topic; without it, user code branching on topic goes wrong.
        """
        group_topic = MixAll.get_retry_topic(self.consumer_group)
        for msg in msgs:
            retry_topic = msg.get_property(MessageConst.PROPERTY_RETRY_TOPIC)
            if retry_topic and msg.topic == group_topic:
                msg.topic = retry_topic
            if self.namespace:
                msg.topic = NamespaceUtil.without_namespace(msg.topic, self.namespace)

    # ---------------- Broker-initiated requests: 220 / 221 / 307 / 309 ----------------
    # These four requests are sent by the broker (or mqadmin via the broker) back to the client.
    # The handling entries are registered on MQClientInstance (see
    # mq_client.py), because it must find the **corresponding** consumer instance by consumerGroup, rather than
    # each consumer registering once (in which case, with multiple consume groups in the same process, only the last one takes effect).

    def reset_offset(self, topic: str, offset_table: Dict[MessageQueue, int]) -> None:
        """The handling logic of RESET_CONSUMER_CLIENT_OFFSET(220): land the broker-issued new offset on this instance.

        Four steps needed: (1) void this queue (in-flight batches' acks and buffer voided together)
        (2) wait for non-orderly in-flight batches to finish (3) write the new offset and persist immediately, orderly unlock
        (4) remove the queue from the local queue table, and rebalance rebuilds at the new offset.

        This port's implementation: the new offset lands in the table first, then goes through the unified revoke path (``_retire_queue_locked`` epoch +1
        voids, clears the buffer, removes pull threads), the revoke's cleanup persists the **new offset**, and rebalance rebuilds last.
        The wait takes 0.2 seconds: the epoch has already voided all in-flight acks; it does not rely on "waiting" to avoid races.
        """
        if topic is None or not offset_table:
            return
        retired: List[Tuple[MessageQueue, Optional[int]]] = []
        with self._lock:
            for key, mq in list(self._mq_map.items()):
                if mq.topic != topic:
                    continue
                off = offset_table.get(mq)
                if off is None:
                    continue
                # The new offset goes into the table first: revoke hands **it** to _on_queues_revoked for persistence
                # (a persist must immediately follow writing the offset)
                self._consume_offsets[key] = int(off)
                self._retire_queue_locked(key, mq, retired)
        if not retired:
            return
        time.sleep(0.2)
        self._on_queues_revoked(retired)
        try:
            self._do_rebalance()
        except Exception as e:  # noqa: BLE001
            logger.debug("rebalance after reset offset failed: %s", e)
        logger.info("reset offset applied, group=%s topic=%s queues=%d",
                    self.consumer_group, topic, len(retired))

    def get_consumer_status(self, topic: str) -> Dict[MessageQueue, int]:
        """The response data source of GET_CONSUMER_STATUS_FROM_CLIENT(221).

        The response carries a snapshot of the **consumed offsets** table
        (not the pull cursor). A blank topic is not a filter: the caller asked
        for everything this consumer holds, so an empty string must not read as
        "match a topic literally named empty" and report nothing.
        """
        out: Dict[MessageQueue, int] = {}
        with self._lock:
            for key, mq in list(self._mq_map.items()):
                if topic and mq.topic != topic:
                    continue
                off = self._consume_offsets.get(key)
                if off is not None:
                    out[mq] = int(off)
        return out

    def consumer_running_info(self) -> ConsumerRunningInfo:
        """The response of GET_CONSUMER_RUNNING_INFO(307): assemble the runtime info."""
        info = ConsumerRunningInfo()
        info.properties = {
            ConsumerRunningInfo.PROP_NAMESERVER_ADDR: ";".join(self.name_server_addrs) + ";",
            ConsumerRunningInfo.PROP_CONSUME_TYPE: "CONSUME_PASSIVELY",
            ConsumerRunningInfo.PROP_CONSUME_ORDERLY: str(bool(self._is_orderly())).lower(),
            ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE: str(self.get_core_pool_size()),
            ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP: str(int(self._start_time * 1000)),
            ConsumerRunningInfo.PROP_CLIENT_VERSION: "V5_5_1",
        }
        with self._lock:
            subs = list(self.subscription_data.values())
            # In POP mode, popped messages go to pop_queues; the classic local queue table is empty
            # -- the two tables are mutually exclusive, and so are they in the 307 response: the same queue
            # appearing in both mqTable and mqPopTable would make the console count one consume stream as two.
            # _mq_map is the "assigned" registry (stall self-healing, offset persistence, and 220 reset all rely on it), written in both modes,
            # so it is filtered by mode here rather than not written.
            pop_keys = set(self._pop_queues or ()) if self.pop_mode else ()
            for key, mq in self._mq_map.items():
                if key in pop_keys:
                    continue
                pqi = ProcessQueueInfo()
                pqi.commit_offset = int(self._consume_offsets.get(key, 0))
                pqi.cached_msg_count = len(self._pending.get(key) or ())
                pqi.droped = False
                # This is the moment ops looks at;
                # hardcoding 0 would make "whether this stream has stalled" completely invisible in the 307 response.
                pqi.last_pull_timestamp = int(self._last_pull_table.get(key, 0) * 1000)
                info.mq_table[mq] = pqi.to_dict()
            if self.pop_mode:
                for key, pq in (self._pop_queues or {}).items():
                    mq = self._mq_map.get(key)
                    if mq is None:
                        continue
                    pqi = ProcessQueueInfo()
                    pqi.cached_msg_count = pq.wait_ack_count()
                    pqi.droped = pq.is_dropped()
                    # POP queues use last_pop_timestamp in place of last_pull_timestamp
                    # for stall detection; the same timestamp is filled here to stay comparable.
                    pqi.last_pull_timestamp = int(pq.last_pop_timestamp * 1000)
                    info.mq_pop_table[mq] = pqi.to_dict()
        info.subscription_set = [s.to_dict() if hasattr(s, "to_dict") else dict(s.__dict__)
                                 for s in subs]
        # statusTable: the minute snapshot of consumeStatus(group, topic)
        for s in subs:
            if self._stats_manager is not None:
                info.status_table[s.topic] = self._stats_manager.consume_status(
                    self.consumer_group, s.topic).to_dict()
            else:
                info.status_table[s.topic] = ConsumeStatus().to_dict()
        return info

    def consume_message_directly(self, msg: MessageExt,
                                 broker_name: Optional[str]) -> ConsumeMessageDirectlyResult:
        """CONSUME_MESSAGE_DIRECTLY(309): hand a message directly to this instance's listener.

        Triggered from the admin side; the ``order`` flag in the response distinguishes consume modes (the broker picks the DLQ policy by it).
        The orderly side's mapping has two more members than the concurrent side: COMMIT -> ``CR_COMMIT``, ROLLBACK ->
        ``CR_ROLLBACK``; returning them on the concurrent side only falls into ``default:``.
        """
        result = ConsumeMessageDirectlyResult()
        orderly = self._is_orderly()
        result.order = orderly
        msgs = [msg]
        mq = MessageQueue(topic=msg.topic, broker_name=broker_name or "",
                          queue_id=msg.queue_id)
        self._reset_retry_topic_and_namespace(msgs)
        context = ConsumeOrderlyContext(mq) if orderly else ConsumeConcurrentlyContext(mq)
        begin = int(time.time() * 1000)
        try:
            status = self.message_listener.consume_message(msgs, context) \
                if self.message_listener is not None else None
            if orderly:
                if status == ConsumeOrderlyStatus.SUCCESS:
                    result.consume_result = CMResult.CR_SUCCESS
                elif status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    result.consume_result = CMResult.CR_LATER
                elif status == ConsumeOrderlyStatus.COMMIT:
                    result.consume_result = CMResult.CR_COMMIT
                elif status == ConsumeOrderlyStatus.ROLLBACK:
                    result.consume_result = CMResult.CR_ROLLBACK
                elif status is None:
                    result.consume_result = CMResult.CR_RETURN_NULL
            elif status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
                result.consume_result = CMResult.CR_SUCCESS
            elif status == ConsumeConcurrentlyStatus.RECONSUME_LATER:
                result.consume_result = CMResult.CR_LATER
            elif status is None:
                result.consume_result = CMResult.CR_RETURN_NULL
        except Exception as e:  # noqa: BLE001
            result.consume_result = CMResult.CR_THROW_EXCEPTION
            result.remark = "%s: %s" % (type(e).__name__, e)
        # Ordered consumption's autoCommit reads the context value **after the listener finishes** (placed
        # outside try/catch, and also read on the exception path). The binlog usage of setting false inside the listener relies on this
        # feedback to let the broker know "this direct consume did not commit"; reading the initial value early would swallow it,
        # presenting on live systems only as mqadmin's returned autoCommit always being true.
        result.auto_commit = context.auto_commit if orderly else True
        result.spent_time_mills = int(time.time() * 1000) - begin
        return result

    def _requeue_pending(self, key: str, batch: List[MessageExt]) -> None:
        """Push this batch back to the head of the local queue (the equivalent of in-place retry / rollback).

        The pending buffer is the ``_pending`` deque, and the batch is popped from the head **before** being dispatched to the listener;
        while the pull cursor (``_offset_table``) has already advanced to ``nextBeginOffset`` at the moment of pulling,
        so "the offset not advancing" does not make the broker send these messages again -- to retry them in place they must be explicitly
        put back.
        """
        with self._lock:
            dq = self._pending.get(key)
            if dq is not None:
                for m in reversed(batch):
                    dq.appendleft(m)

    def _deregister_inflight_locked(self, key: str, batch: List[MessageExt]) -> None:
        """Remove this batch from the "in the listener's hands" registration (by object identity, idempotent). Must be called while holding the lock."""
        bucket = self._inflight_msgs.get(key)
        if not bucket:
            return
        gone = {id(m) for m in batch}
        bucket[:] = [m for m in bucket if id(m) not in gone]
        if not bucket:
            self._inflight_msgs.pop(key, None)

    def _process_queue_entries_locked(self, key: str) -> List[MessageExt]:
        """This queue's full "held messages" view: in-flight (dispatched but not settled) union pulled-but-not-dispatched.

        Pairing operations (register/remove, push back to buffer) all complete in pairs within ``self._lock``, and a message appears
        in only one side at any instant; the extra dedup by object identity here only guards against future new call sites writing overlaps.
        """
        out: List[MessageExt] = []
        seen: Set[int] = set()
        for m in list(self._inflight_msgs.get(key) or ()) + list(self._pending.get(key) or ()):
            if id(m) not in seen:
                seen.add(id(m))
                out.append(m)
        return out

    def _process_queue_first_entry_locked(self, key: str) -> Optional[MessageExt]:
        """The head of the queue = the entry with the smallest ``queueOffset``."""
        entries = self._process_queue_entries_locked(key)
        if not entries:
            return None
        return min(entries, key=lambda m: m.queue_offset or 0)

    def _process_queue_contains_locked(self, key: str, msg: MessageExt) -> bool:
        """Is this message still attached to the queue (in the in-flight registration or the pending buffer).

        Messages swept away by _clean_expired_queue do not count (they are already on their way back to the broker).
        """
        for m in self._inflight_msgs.get(key) or ():
            if m is msg:
                return True
        for m in self._pending.get(key) or ():
            if m is msg:
                return True
        return False

    def _remove_process_queue_entry_locked(self, key: str, msg: MessageExt) -> None:
        """Remove the just-swept entry (must hold the lock; the caller has confirmed it is the head).

        Remove from both sides: normally it is in only one; between the concurrent path's "failed redelivery pushed back to the buffer" and "method cleanup removes the registration"
        there is an instant when it is in both, and removing from both places ensures the next sweep round does not see it again.
        """
        self._deregister_inflight_locked(key, [msg])
        dq = self._pending.get(key)
        if dq and dq[0] is msg:
            dq.popleft()

    # ---------------- expired-message sweep: the escape hatch for suspended listeners ----------------
    # This is the only recovery
    # mechanism for "the listener hangs and never returns": messages held longer than consumeTimeout minutes are sent back to the broker (delayLevel=3), going through %RETRY%
    # for redelivery. Without it, one stuck message would stop that queue's offset forever, with the broker side only able to wait for the next
    # client restart -- on live systems this presents as "the message was clearly sent but is never received a second time", with no exception to look at.

    def _start_clean_expire_loop(self) -> None:
        t = threading.Thread(target=self._clean_expire_loop, daemon=True,
                             name="rmq-cleanexpire-%s" % self.consumer_group)
        t.start()
        self._clean_expire_thread = t

    def _clean_expire_loop(self) -> None:
        # initialDelay equals period (both are consumeTimeout minutes), so the first sweep round also waits a full period --
        # sweeping immediately would look through all just-started, not-yet-timed-out messages (harmless, but entirely unnecessary).
        period = max(1, self.consume_timeout) * 60.0
        if self._stop.wait(period):                # initialDelay = consumeTimeout minutes
            return
        while True:
            if not self._started:
                return
            try:
                self._clean_expired_msg_once()
            except Exception as e:  # noqa: BLE001
                # The scheduler shell catches all exceptions: an error in a single sweep round must not kill the schedule
                logger.error("scheduleAtFixedRate cleanExpireMsg exception: %s", e)
            if self._stop.wait(period):            # period = consumeTimeout minutes
                return

    def _clean_expired_msg_once(self) -> None:
        """Sweep the currently held queues one by one."""
        with self._lock:
            keys = list(self._mq_map.keys())
        for key in keys:
            self._clean_expired_queue(key)

    def _clean_expired_queue(self, key: str) -> None:
        """Sweep timed-out messages in a single queue.

        The semantic trio: only look at the head (smallest offset), per-message judgment strictly greater than consumeTimeout, at most 16 messages per round.
        """
        if self._is_orderly():
            # Ordered consumption has no such path (messages are to be retried in place anyway; redelivery would reorder)
            return
        with self._lock:
            loop = min(len(self._process_queue_entries_locked(key)), 16)  # At most 16 messages per round
        for _ in range(loop):
            msg = None
            with self._lock:
                first = self._process_queue_first_entry_locked(key)
                if first is not None:
                    stamp = MessageAccessor.get_consume_start_timestamp(first)
                    # Never stamped (buffered messages that have not yet entered the listener) are treated as not expired directly;
                    # the expiry criterion is **strictly** greater (equal to the threshold does nothing)
                    if stamp and (time.time() * 1000 - float(stamp)) > self.consume_timeout * 60 * 1000:
                        msg = first
            if msg is None:
                break                          # The head is not expired; the ones behind it cannot be either
            try:
                self.send_message_back(msg, 3)  # delayLevel is fixed at 3 (delay ~10s)
            except Exception as e:  # noqa: BLE001
                # Redelivery failure only logs: the message stays put and is
                # retried on the next round (or the next pass of this round); never removed (removed means truly lost)
                logger.error("send expired msg exception: %s", e)
                continue
            logger.info("send expire msg back. topic=%s, msgId=%s, storeHost=%s, "
                        "queueId=%s, queueOffset=%s",
                        msg.topic, msg.msg_id, msg.get_store_host_string(),
                        msg.queue_id, msg.queue_offset)
            with self._lock:
                # Only remove when it is **still** the head: if the listener happens to finish normally
                # during this time (or a smaller offset appears ahead), yield to the normal path, do not grab
                if self._process_queue_first_entry_locked(key) is msg:
                    self._remove_process_queue_entry_locked(key, msg)

    def _consume_batch(self, key: str, mq: MessageQueue, batch: List[MessageExt],
                       epoch: Optional[int] = None) -> bool:
        """Consume one batch and handle redelivery/suspension. Returns whether the consume offset advanced.

        ``epoch``: the epoch of this batch's queue buffer (see ``_queue_epoch``). When the queue is dropped while this batch
        was queuing (rebalance revoke / OFFSET_ILLEGAL correction / stall self-healing), these messages
        **must not be consumed nor acked** -- the dropped-queue short circuit, with the concurrent and orderly paths sharing the same policy.
        """
        if epoch is not None and epoch != self._queue_epoch.get(key, 0):
            dropped = self._queue_epoch.get(key, 0)
            logger.warning("the message queue not be able to consume, because it's dropped. "
                           "group=%s mq=%s msgs=%d epoch=%s->%s",
                           self.consumer_group, mq, len(batch), epoch, dropped)
            return False
        listener = self.message_listener
        broadcast = self.message_model == MessageModel.BROADCASTING
        self._reset_retry_topic_and_namespace(batch)
        # ---- Ordered consumption ----
        if self._is_orderly():
            ocontext = ConsumeOrderlyContext(mq)
            ohook_ctx = None
            if self.consume_message_hook_list:
                ohook_ctx = self._build_consume_hook_context(batch, mq)
                self.execute_consume_hook_before(ohook_ctx)
            obegin_ms = time.time() * 1000
            ohas_exception = False
            try:
                status = listener.consume_message(batch, ocontext)
            except Exception as e:  # noqa: BLE001
                # The exception only sets has_exception, leaving status null, uniformly
                # turned into suspension below: ordered consumption never has the "exception means ack" path
                logger.debug("orderly listener error (retry in place): %s", e)
                status = None
                ohas_exception = True
            # null / ROLLBACK / suspend all leave a warn
            if (status is None or status == ConsumeOrderlyStatus.ROLLBACK
                    or status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT):
                logger.warning("consumeMessage Orderly return not OK, Group: %s Msgs: %d MQ: %s",
                               self.consumer_group, len(batch), mq)
            raw_status = status
            if status is None or status not in _ORDERLY_STATUSES:
                # null is treated as suspension before the hook. In the static type
                # status can only be an enum member; under dynamic types "returned something else" takes the same fallback:
                # otherwise it would fall all the way into the SUCCESS branch, silently acking messages that were not consumed successfully.
                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
            # The hook gets the **post**-normalization status, the success criterion is SUCCESS||COMMIT,
            # while returnType uses the **pre**-normalization one
            self._finish_consume_hook(
                ohook_ctx, raw_status, ohas_exception, obegin_ms,
                failed=status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT,
                succeeded=status in (ConsumeOrderlyStatus.SUCCESS, ConsumeOrderlyStatus.COMMIT),
                hook_status=status)
            if ocontext.auto_commit:
                # ---- Ordered consumption's autoCommit=True branch ----
                if status in (ConsumeOrderlyStatus.COMMIT, ConsumeOrderlyStatus.ROLLBACK):
                    # COMMIT/ROLLBACK with autoCommit=true is an illegal usage
                    # (only for binlog consumption); here it only warns and **does not return early**, falling through
                    # into the SUCCESS branch: the message is acked as usual, not treated as a rollback
                    logger.warning("the message queue consume result is illegal, "
                                   "we think you want to ack these message %s", mq)
                    status = ConsumeOrderlyStatus.SUCCESS
                if status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=True)
                    # Before suspending, first pass _check_reconsume_times: only "still within the retry
                    # count / redelivery failed" retries in place; those already handed to the broker (redelivery succeeded) must
                    # advance the offset, otherwise one poison message permanently occupies this queue
                    if self._check_orderly_reconsume_times(batch):
                        self._requeue_pending(key, batch)
                        time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
                        return False
                else:
                    self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=False)
                self._advance_consume_offset(key, batch, epoch=epoch)
                return True
            # ---- autoCommit=False (binlog consumption scenario) ----
            if status == ConsumeOrderlyStatus.COMMIT:
                # Explicit commit: the offset advances, **no TPS recorded** (RT is still recorded outside the branch)
                self._record_consume_rt(mq.topic, obegin_ms)
                self._advance_consume_offset(key, batch, epoch=epoch)
                return True
            if status == ConsumeOrderlyStatus.ROLLBACK:
                # rollback: push messages back to the pending buffer and retry later
                self._record_consume_rt(mq.topic, obegin_ms)
                self._requeue_pending(key, batch)
                time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
                return False
            if status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=True)
                if self._check_orderly_reconsume_times(batch):
                    self._requeue_pending(key, batch)
                    time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
                # Difference from the auto-commit branch: a poison message handed to the broker is **not committed**,
                # and whether the offset advances is up to the binlog consumer itself
                return False
            # SUCCESS + autoCommit=False: only record OK TPS, do not commit.
            # This port does not expose the queue buffer to the listener (there is no commit opening),
            # so it does not maintain an explicit commit() subtable like "currently consuming";
            # doing nothing would let this batch be swallowed by the dispatch thread with the offset unmoved. Equivalently here it pushes back to
            # the head and waits one suspend period: the offset likewise does not advance, no messages are lost, and the consume thread is not turned into a busy wait
            # (without sleep, the next round would get the same batch immediately, which under the DEFAULT config is 100% CPU spinning).
            self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=False)
            self._requeue_pending(key, batch)
            time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
            return False
        # ---- Concurrent consumption ----
        # Register in-flight messages: on the concurrent path, messages **remain in** the
        # held messages table while held by a batch (removed only after the consume result is processed),
        # and the timeout sweep's "head" criterion and "is this still on the queue" criterion both look at it. The orderly path is the opposite
        # (once a batch is taken it is no longer in the pending buffer, while the sweep exits early for orderly),
        # so only concurrent batches are registered.
        with self._lock:
            self._inflight_msgs.setdefault(key, []).extend(batch)
        try:
            return self._consume_concurrent_batch(key, mq, batch, epoch, listener, broadcast)
        finally:
            # After normal completion (including failed-redelivery re-enqueue and sweep removal) this batch is no longer "in the listener's hands";
            # entries removed early by sweep/re-enqueue are idempotent here.
            with self._lock:
                self._deregister_inflight_locked(key, batch)

    def _consume_concurrent_batch(self, key: str, mq: MessageQueue, batch: List[MessageExt],
                                  epoch: Optional[int], listener, broadcast: bool) -> bool:
        """Consume one batch concurrently."""
        context = ConsumeConcurrentlyContext(mq)
        hook_ctx = None
        if self.consume_message_hook_list:
            # before hook **before** the listener (for generating SubBefore traces),
            # after after getting the status (generates SubAfter, with contextCode)
            hook_ctx = self._build_consume_hook_context(batch, mq)
            self.execute_consume_hook_before(hook_ctx)
        begin_ms = time.time() * 1000
        has_exception = False
        try:
            # Stamp CONSUME_START_TIME on each message **before** handing to the listener, restamping on every delivery
            # (including retries). _clean_expired_queue's suspend escape hatch relies on it to judge "how long this one has been held".
            for m in batch:
                MessageAccessor.set_consume_start_timestamp(m, int(time.time() * 1000))
            status = listener.consume_message(batch, context)
        except Exception as e:  # noqa: BLE001
            # An exception from consumption is treated as RECONSUME_LATER
            logger.debug("listener error, treat as RECONSUME_LATER: %s", e)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
            has_exception = True
        # A listener returning null is likewise treated as RECONSUME_LATER (the default ackIndex
        # only matters for CONSUME_SUCCESS; null falling through below means whole-batch redelivery). The pre-normalization status must
        # be left to returnType (returning null records RETURNNULL, not FAILED)
        raw_status = status
        if status is None:
            logger.warning("consumeMessage return null, Group: %s Msgs: %d MQ: %s",
                           self.consumer_group, len(batch), mq)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
        # CONSUME_SUCCESS uses the listener-set ackIndex
        # to divide "acknowledged prefix / pending-redelivery suffix" (default Integer.MAX_VALUE, clamped to size-1 means the whole batch is acknowledged);
        # RECONSUME_LATER forces ackIndex=-1, whole-batch redelivery.
        ack_index = context.ack_index
        if status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
            if ack_index >= len(batch):
                ack_index = len(batch) - 1
        else:
            ack_index = -1
        # Stats policy same as the ok/failed counts: on partial ack the tail counts as failed
        self._record_consume_stats(mq.topic, len(batch), begin_ms,
                                   failed=status == ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                   ack_count=ack_index + 1)
        self._finish_consume_hook(
            hook_ctx, raw_status, has_exception, begin_ms,
            failed=status == ConsumeConcurrentlyStatus.RECONSUME_LATER,
            succeeded=status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
            hook_status=status)
        if broadcast:
            # Broadcast mode does not redeliver: the unacknowledged tail is only logged as a warn before being dropped,
            # while the whole batch's offset still advances (the drop step clears the whole batch)
            dropped = len(batch) - ack_index - 1
            if dropped > 0:
                logger.warning("BROADCASTING, the message consume failed, drop it: %d msgs in %s",
                               dropped, mq)
            self._advance_consume_offset(key, batch, epoch=epoch)
            return True
        if ack_index + 1 >= len(batch):
            # Whole batch acknowledged (default path): nothing to redeliver, the offset advances directly
            self._advance_consume_offset(key, batch, epoch=epoch)
            return True
        # Cluster mode: redeliver the unacknowledged [ack_index+1, size) one by one to the %RETRY% topic (delay gradient
        # 3+reconsumeTimes; exceeding maxReconsumeTimes is auto-moved to %DLQ% by the broker)
        msg_back_failed = self._send_back_batch(key, batch[ack_index + 1:], context)
        # Those whose redelivery failed are removed from this batch and retried later;
        # here equivalently pushed back to the head of the queue for consumption shortly
        if msg_back_failed:
            with self._lock:
                dq = self._pending.get(key)
                if dq is not None:
                    for m in reversed(msg_back_failed):
                        dq.appendleft(m)
            time.sleep(0.2)
        failed_ids = {id(m) for m in msg_back_failed}
        # What is committed is "the largest queueOffset + 1 among this batch's processed entries", and must not pass over
        # those still left in the queue's buffer (the removal step returns their smallest offset at this point)
        self._advance_consume_offset(
            key, [m for m in batch if id(m) not in failed_ids],
            floor=min((m.queue_offset or 0) for m in msg_back_failed) if msg_back_failed else None,
            epoch=epoch)
        return not msg_back_failed

    def _send_back_batch(self, key: str, batch: List[MessageExt],
                         context: ConsumeConcurrentlyContext) -> List[MessageExt]:
        """Redeliver the unacknowledged entries to the broker one by one, returning those that failed.

        Failed entries get ``reconsume_times + 1`` in place -- the broker did not record that count,
        and without the client making it up they would never reach the DLQ.
        """
        failed: List[MessageExt] = []
        for msg in batch:
            with self._lock:
                present = self._process_queue_contains_locked(key, msg)
            if not present:
                # Skip entries already swept by timeout (or whose queue is revoked)
                # redelivery: they are already on their way back to the broker, and sending again is a duplicate message
                logger.info("Message is not found in its process queue; skip send-back-procedure, "
                            "topic=%s, brokerName=%s, queueId=%s, queueOffset=%s",
                            msg.topic, msg.broker_name, msg.queue_id, msg.queue_offset)
                continue
            try:
                # The retry count is field 13 of MessageExt's wire format,
                # which the broker increments on redelivery; it is not a properties key (PROPERTY_RECONSUME_TIME
                # 's actual value is "RECONSUME_TIME", written only by MessageAccessor.set_reconsume_time,
                # which the peer and callers match on; changing it breaks compatibility)
                delay_level = context.delay_level_when_next_consume
                if not delay_level:
                    # delay_level_when_next_consume == 0 -> 3 + reconsume_times
                    delay_level = 3 + msg.get_reconsume_times()
                self.send_message_back(msg, delay_level)
            except Exception as e:  # noqa: BLE001
                logger.debug("send message back failed for msg %s: %s", msg.msg_id, e)
                msg.set_reconsume_times(msg.get_reconsume_times() + 1)
                failed.append(msg)
        return failed

    # ---------------- Ordered consumption's retry count and redelivery ----------------
    # The orderly-side retry count and redelivery are **not the same link** as the concurrent set above; both differences must be held:
    #   1. `-1` on the orderly side means "unlimited" (Integer.MAX_VALUE), while 16 only applies on the concurrent side;
    #   2. the orderly side's redelivery is a **normal message send** (sent to %RETRY%<group>), not CONSUMER_SEND_MSG_BACK(3).

    def _orderly_suspend_millis(self, context: ConsumeOrderlyContext) -> int:
        """How long to suspend this queue before retrying.

        First resolve ``-1`` (the default of ``ConsumeOrderlyContext.suspend_current_queue_time_millis``,
        meaning "not specified") -> fall back to the consumer config ``suspend_current_queue_time_millis`` (default 1000),
        then **clamp the result to [10, 30000]**. This clamp is not decoration: a listener passing 0 still waits 10ms
        (otherwise a listener returning suspend would turn the consume thread into a busy wait), and passing 1 hour only waits 30s.
        """
        ms = context.suspend_current_queue_time_millis
        if ms == -1:
            ms = self.suspend_current_queue_time_millis
        if ms < 10:
            return 10
        if ms > 30000:
            return 30000
        return ms

    def _orderly_max_reconsume_times(self) -> int:
        """The max retry count for ordered consumption.

        Ordered consumption has no broker-side redelivery count (messages keep retrying in place in the local queue), so by default
        it should keep retrying until success -- `-1` reads as `Integer.MAX_VALUE` here. The concurrent consumption's
        `-1 -> 16` is a different set of semantics: there every round of redelivery goes through the broker, and 16 is the broker's default
        retryMaxTimes. Merging the two into one constant would either invent dead letters for ordered consumption out of thin air,
        or make concurrent messages retry infinitely.
        """
        if self.max_reconsume_times == -1:
            return _INT32_MAX
        return self.max_reconsume_times

    def _check_orderly_reconsume_times(self, msgs: Optional[List[MessageExt]]) -> bool:
        """Check whether the orderly batch's retries are exhausted.

        Returns "whether this batch should still suspend and retry in place". Two paths per message:
          - retries not exhausted: local `reconsumeTimes + 1` (the broker did not record it at all; without the client making it up
            the threshold would never be reached), keep suspending;
          - retries exhausted: hand to the broker for redelivery. **Successful redelivery means no more suspending** (commit the offset at this point,
            the poison message gives way and the queue moves on), and only failed redelivery increments and suspends.
        """
        suspend = False
        max_times = self._orderly_max_reconsume_times()
        for msg in msgs or []:
            if msg.get_reconsume_times() >= max_times:
                MessageAccessor.set_reconsume_time(msg, msg.get_reconsume_times())
                if not self._orderly_send_message_back(msg):
                    suspend = True
                    msg.set_reconsume_times(msg.get_reconsume_times() + 1)
            else:
                suspend = True
                msg.set_reconsume_times(msg.get_reconsume_times() + 1)
        return suspend

    def _orderly_send_message_back(self, msg: MessageExt) -> bool:
        """Ordered consumption's redelivery: use the internal producer to forward the message to the retry topic.

        Take the instance's built-in internal producer and send this message **as a normal message** to `%RETRY%<group>`:
        the broker's retry/DLQ handling on its send path, seeing that the group still holds an unexpired
        queue lock (a characteristic of ordered consumption groups), directly redirects it to `%DLQ%<group>`. Among the property settings,
        `RECONSUME_TIME` / `MAX_RECONSUME_TIMES` are lifted into the request header by the send side
        (see `mq_client._build_send_request`); the peer matches on these headers, and changing them breaks compatibility.

        Failure only returns false, never throws: the whole block catches exceptions; throwing to the consume thread would leave this one neither
        acked nor redelivered, only able to wait for the lock to time out.
        """
        try:
            client = self._require_client()
            new_msg = Message(MixAll.get_retry_topic(self.consumer_group), msg.body)
            new_msg.properties = dict(msg.properties)
            new_msg.flag = msg.flag
            origin_msg_id = MessageAccessor.get_origin_message_id(msg) or msg.msg_id
            if origin_msg_id:
                MessageAccessor.set_origin_message_id(new_msg, origin_msg_id)
            MessageAccessor.put_property(new_msg, MessageConst.PROPERTY_RETRY_TOPIC, msg.topic)
            MessageAccessor.set_reconsume_time(new_msg, msg.get_reconsume_times() + 1)
            MessageAccessor.set_max_reconsume_times(new_msg,
                                                    self._orderly_max_reconsume_times())
            # The half-message flag must be cleared, otherwise the broker would treat it as a transaction-check message again
            MessageAccessor.clear_property(new_msg,
                                           MessageConst.PROPERTY_TRANSACTION_PREPARED)
            new_msg.set_delay_time_level(3 + msg.get_reconsume_times())
            publish = client.get_topic_publish_info(new_msg.topic, is_default=True)
            mq = publish.select_one_message_queue() if publish is not None else None
            if mq is None:
                raise MQClientException("no writable queue for retry topic %s" % new_msg.topic)
            # unitMode follows the consumer: the internal producer reuses the consumer's client config,
            # and without it the redelivered messages would lose the unit flag.
            client.send_message(MixAll.CLIENT_INNER_PRODUCER_GROUP, new_msg, mq, 3000,
                                unit_mode=self.unit_mode)
            return True
        except Exception as e:  # noqa: BLE001
            logger.debug("orderly send message back failed, group=%s msg=%s: %s",
                         self.consumer_group, msg.msg_id, e)
            return False

    def _advance_consume_offset(self, key: str, batch: List[MessageExt],
                                floor: Optional[int] = None,
                                epoch: Optional[int] = None) -> None:
        """Advance the "consumed offset".

        ``epoch`` is the queue buffer's epoch when this batch was taken: a mismatch with the current epoch means the queue has been
        revoked/rebuilt (equivalent to the dropped-queue gate), voiding the **whole batch's acks**.
        Entries in ``_frozen_offsets`` are offsets frozen by OFFSET_ILLEGAL correction, which likewise must not be changed.

        The two criteria and the write must be in the **same critical section**: revoke/reset (220) increments the epoch inside the lock,
        so if the epoch were read outside the lock first and then the offset written inside, an in-flight batch's ack could squeeze in between and push the reset offset back.
        """
        if not batch:
            # When the whole batch's redelivery fails, no entry is acknowledged and the offset stays put
            return
        next_off = max((m.queue_offset or 0) for m in batch) + 1
        if floor is not None:
            next_off = min(next_off, floor)
        with self._lock:
            if epoch is not None and epoch != self._queue_epoch.get(key, 0):
                logger.debug("drop ack for %s: process queue was dropped (epoch %s -> %s)",
                             key, epoch, self._queue_epoch.get(key, 0))
                return
            if key in self._frozen_offsets:
                return
            cur = self._consume_offsets.get(key)
            self._consume_offsets[key] = max(cur or 0, next_off)

    def _correct_tags_offset_locked(self, key: str, status: PullStatus,
                                    next_off: Optional[int]) -> None:
        """On an empty response, advance the "consumed offset" along with the pull cursor (the tag-filter correction). Must be called while holding the lock.

        When the pull response is ``NO_NEW_MSG`` (the queue really has no messages) or ``NO_MATCHED_MSG`` (the broker went a full round per the subscription
        expression and matched nothing; the broker's returned
        ``PULL_RETRY_IMMEDIATELY`` is mapped into it when parsing the response), this queue's "consumed offset" must follow the pull cursor,
        otherwise it would **deadlock permanently**: messages nobody acks belong to nobody (those filtered on the broker side are not in the response,
        and those dropped by the client's second-pass tag filtering are explicitly not acked), and an unmoving offset stays put forever, with redelivery/restart
        sweeping this unwanted batch from the beginning again.

        The update is the consumed-offset write of ``_correct_tags_offset_locked``:
        (only moves forward, never back), and has one gate: the queue's held message count must be 0. It counts messages **still in the queue** --
        concurrent consumption's cleanup removal happens only after the listener returns, so in-flight batches
        count too; raising the offset without waiting for them to settle would have this batch silently skipped on process crash. This port combines
        ``_pending`` being empty with ``_msg_queue_inflight`` being 0 into the same criterion, with the latter's registration and
        "taking the batch" completed within the same lock in ``_dispatch_loop``.

        The status criteria are also folded into this function (not placed in the response callback's branches),
        so offline unit tests can lock down "which statuses to correct + when correction is allowed" in one go.
        """
        if status not in (PullStatus.NO_NEW_MSG, PullStatus.NO_MATCHED_MSG):
            return
        if next_off is None:
            return
        if key in self._frozen_offsets:
            # The offset is already frozen by OFFSET_ILLEGAL correction (updates
            # are refused unconditionally once frozen), and this path likewise must not change it
            return
        if self._pending.get(key) or self._msg_queue_inflight.get(key):
            return
        cur = self._consume_offsets.get(key)
        if cur is None or next_off > cur:
            self._consume_offsets[key] = next_off

    def _offset_illegal_recover(self, key: str) -> None:
        """The correction-rebuild path for an OFFSET_ILLEGAL response.

        The broker says "the offset you requested is illegal" (the queue was truncated, the commitlog expired, or it was reset server-side:
        the broker's own pull answer returns ``PULL_OFFSET_MOVED`` for OFFSET_OVERFLOW_BADLY / NO_MESSAGE_IN_QUEUE /
        OFFSET_RESET uniformly, which is mapped into this status when the client parses the response),
        and puts the corrected value in ``nextBeginOffset``. Correction is not just moving the cursor over:
        all messages on this queue **already fetched but not consumed/acked are voided** (they fall in the skipped range,
        and acking them would push the offset back to an illegal value), then the queue is rebuilt at the corrected offset.

        The four correction steps: set the new offset -> void this queue -> { freeze the offset; persist; remove the queue } -> rebalance immediately.
        This port's counterpart: set the offset + freeze (in the callback's critical section) -> revoke the queue
        (``_retire_queue_locked`` clears the buffer/thread table and **increments the epoch**) ->
        push the corrected offset to the broker outside the lock -> wake rebalance to rebuild.

        Must be called outside the lock: it does network RPCs inside.
        """
        with self._lock:
            retired: List[Tuple[MessageQueue, Optional[int]]] = []
            self._retire_queue_locked(key, None, retired)
        # persist: write the corrected offset back to the broker immediately (explicitly once, without waiting for the periodic persist)
        self._on_queues_revoked(retired)
        self.rebalance_immediately()
        logger.warning("the pull request offset illegal, fix it, queue=%s", key)

    # ---------------- Offset persistence ----------------
    def _start_offset_persist_loop(self) -> None:
        t = threading.Thread(target=self._offset_persist_loop, daemon=True,
                             name="rmq-offset-persist-%s" % self.consumer_group)
        t.start()
        self._persist_thread = t

    def _offset_persist_loop(self) -> None:
        # Offset persistence cadence: initialDelay = 1000 * 10,
        # the period takes persist_consumer_offset_interval (default 5s).
        # Both steps are read only once at start(); runtime changes do not alter the already-scheduled cadence.
        if self._stop.wait(10.0):    # initialDelay = 1000 * 10
            return
        # The period is taken once at the loop entry (a fixed period is scheduled; runtime field changes do not alter the already-scheduled task).
        # The first persist happens after initialDelay, not after initialDelay + one period.
        period = self.persist_consumer_offset_interval / 1000.0
        while True:
            try:
                self._persist_offsets_once()
            except Exception as e:  # noqa: BLE001
                logger.debug("persist offsets error: %s", e)
            if self._stop.wait(period):
                return

    def _persist_offsets_once(self) -> None:
        if self.message_model == MessageModel.BROADCASTING:
            self._save_local_offsets()
            return
        client = self._require_client()
        with self._lock:
            items = list(self._consume_offsets.items())
        for key, off in items:
            mq = self._mq_map.get(key)
            if mq is None:
                continue
            try:
                client.update_consumer_offset(self.consumer_group, mq, off)
            except Exception as e:  # noqa: BLE001
                logger.debug("update consumer offset failed for %s: %s", mq, e)

    def _local_offset_path(self) -> str:
        # Local offset file path: $HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
        base = os.path.join(os.path.expanduser("~"), ".rocketmq_offsets",
                            self.client_id or "DEFAULT", self.consumer_group)
        return os.path.join(base, "offsets.json")

    def _save_local_offsets(self,
                            extra: Optional[Iterable[Tuple[MessageQueue, Optional[int]]]] = None) -> None:
        """Write the local offset table to disk.

        ``extra`` is the offsets of queues just revoked and no longer in ``_consume_offsets``/``_mq_map``:
        the order must be write the file first, then remove the in-memory entries, with the values in the file
        kept so the rebuilt queue can resume from them (see the broadcast branch of ``_on_queues_revoked``).
        """
        with self._lock:
            items = dict(self._consume_offsets)
            mq_map = dict(self._mq_map)
        for mq, off in extra or ():
            if off is None:
                continue
            key = self._mq_key(mq)
            items[key] = int(off)
            mq_map.setdefault(key, mq)
        path = self._local_offset_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        text = _build_local_offsets_json(items, mq_map)
        # The old content rolls to .bak first, then the new file is written
        try:
            with open(path, "r", encoding="utf-8") as f:
                prev = f.read()
            if prev:
                with open(path + ".bak", "w", encoding="utf-8") as f:
                    f.write(prev)
        except OSError:
            pass
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)

    def _load_local_offsets(self) -> Dict[str, int]:
        # Main file missing/empty/parse failed -> fall back to .bak
        for path in (self._local_offset_path(), self._local_offset_path() + ".bak"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()
            except OSError:
                continue
            parsed = _parse_local_offsets_json(text)
            if parsed is not None:
                return parsed
        return {}

    # ---------------- Ordered consumption queue locks ----------------
    def _start_lock_loop(self) -> None:
        if not self._is_orderly() or self.message_model == MessageModel.BROADCASTING:
            return
        t = threading.Thread(target=self._lock_loop, daemon=True,
                             name="rmq-lock-%s" % self.consumer_group)
        t.start()
        self._lock_thread = t

    def _lock_loop(self) -> None:
        client = self._require_client()
        # Batch-lock the assigned queues every 20s;
        # try once immediately at startup to avoid the first 20s spinning idle
        while not self._stop.is_set():
            # POP orderly also runs this 20s
            # timer, but locking only iterates the classic pull table -- in POP mode
            # queues are registered in pop_queues, and that table is empty => the timer spins without a single network call.
            if not self.pop_mode:
                try:
                    mqs = self._assigned_queues()
                    if mqs:
                        ok = client.lock_batch_mq(self.consumer_group, self.client_id or "", mqs)
                        ok_keys = {"%s%s%d" % (m.topic, m.broker_name, m.queue_id) for m in ok}
                        with self._lock:
                            self._lock_ok = ok_keys
                        logger.debug("lock_batch_mq: %d/%d queues locked", len(ok_keys), len(mqs))
                except Exception as e:  # noqa: BLE001
                    logger.debug("lock mq error: %s", e)
            self._stop.wait(20.0)

    def _is_orderly(self) -> bool:
        return isinstance(self.message_listener, MessageListenerOrderly)

    # ---------------- Management capabilities ----------------
    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        # What is read is the subscription info table (read perm, no master filtering), not the publish info;
        # push and pull consumers share the same policy.
        return client.get_topic_subscribe_info(topic)

    def send_message_back(self, msg: MessageExt, delay_level: int,
                          broker_name: Optional[str] = None) -> None:
        """Message redelivery: send CONSUMER_SEND_MSG_BACK(36) to have the broker redirect it to the retry topic."""
        client = self._require_client()
        from remoting.protocol.codes import RequestCode
        from remoting.protocol.headers import ConsumerSendMsgBackRequestHeader
        from remoting.protocol.remoting_command import RemotingCommand
        if broker_name is None:
            broker_name = msg.broker_name
        # The address goes through find_broker_address_in_publish (master only),
        # and failure reports "Broker[X] master node does not exist" (the error string is matched by callers);
        # slaves do not accept CONSUMER_SEND_MSG_BACK.
        addr = client.find_broker_address_in_publish(broker_name)
        if addr is None:
            raise MQClientException("Broker[%s] master node does not exist" % broker_name)
        # When max_reconsume_times == -1, pass 16 to the broker (exceeding the limit is moved to %DLQ% by the broker)
        max_reconsume = 16 if self.max_reconsume_times == -1 else self.max_reconsume_times
        header = ConsumerSendMsgBackRequestHeader()
        header.offset = msg.commit_log_offset
        header.group = self.consumer_group
        header.delay_level = delay_level
        header.origin_msg_id = msg.msg_id
        header.origin_topic = msg.topic
        header.unit_mode = self.unit_mode
        header.max_reconsume_times = max_reconsume
        request = RemotingCommand.create_request_command(RequestCode.CONSUMER_SEND_MSG_BACK, header)
        # SEND_MESSAGE_BACK is one of the VIP-wrapped RPCs and always goes to the master address.
        response = client._invoke_sync(client.vip_addr(addr), request, 5000)
        client._check_response(response)


class PullCallback:
    pass

    def on_success(self, pull_result: PullResult) -> None:
        raise NotImplementedError

    def on_exception(self, e: BaseException) -> None:
        raise NotImplementedError


class DefaultMQPullConsumer:
    pass

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING,
                 socks_proxy_config: Optional[SocksProxyConfig] = None):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id: Optional[str] = None
        # ---- unit / stream config ----
        # unitName takes part in the clientId's `@<unitName>` suffix and the dynamic addressing URL; unitMode is
        # reported to the broker along with the ConsumerData heartbeat, message redelivery request header and filter hooks;
        # ``enable_stream_request_type`` both appends the `@STREAM` suffix to clientId (to
        # "prevent unexpected reuses of MQClientInstance") and adds
        # the `ReqT=0` extension field to every request. Pull-mode consumers default to on (every constructor sets true).
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = True
        # namespaceV2 (5.x server-side namespace): when non-empty, every request carries
        # nsd=true / ns=<value>, see the NamespaceRpcHook in remoting/rpchook.py.
        self.namespace_v2 = ""
        self.message_model = message_model
        self.broker_suspend_max_time_millis = 20000
        self.socks_proxy_config: Optional[SocksProxyConfig] = socks_proxy_config
        self.consumer_pull_timeout_millis = 10000
        self.consumer_timeout_millis_when_suspend = 30000
        # Route refresh interval, default 30000ms; only passed through once when start() builds the MQClientInstance.
        self.poll_name_server_interval = 30000
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self.register_topics: Set[str] = set()
        self.message_queue_lists: List[MessageQueue] = []
        self.message_queue_listener: Optional[MessageQueueListener] = None
        # Queue allocation strategy (default AllocateMessageQueueAveragely).
        # Pull mode manages queues itself and has no background rebalance,
        # so it exists only as config and is validated by start().
        self.allocate_message_queue_strategy: Optional[AllocateMessageQueueStrategy] = \
            AllocateMessageQueueAveragely()
        # Pre-delivery filter hooks: attached to the pull result unpacking path at start()
        self.filter_message_hook_list: List[FilterMessageHook] = []
        # Pull node suggestion table: MessageQueue -> the brokerId to pick next round
        self._pull_from_which_node: Dict[MessageQueue, int] = {}
        # ---- Consumer heartbeat (``heartbeat_interval_millis`` defaults to 30s) ----
        # Pull-mode consumers must also register with the broker's consumer table; this port follows the existing division of labor
        # (the heartbeat loop lives inside the consumer) and sends its own ConsumerData carrying CONSUME_ACTIVELY.
        self.heartbeat_enabled = True
        self.heartbeat_interval_millis = 30000
        # Heartbeat V2 and the VIP channel switches, both default false (see the push consumer's block
        # for what they do; the pull consumer's heartbeat body is CONSUME_ACTIVELY).
        self.use_heartbeat_v2 = False
        self.vip_channel_enabled = False
        # Received message bodies are materialised by default; switching this off decodes only
        # the headers and leaves every body unset.
        self.decode_read_body = True
        self.enable_concurrent_heartbeat = False
        self.concurrent_heartbeat_thread_pool_size = os.cpu_count() or 1
        self.mq_client_api_timeout = 3000
        self._heartbeat_count = 0
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self.async_pull_queue_capacity = 50000
        self.pull_callback_executor_threads = 0
        self._async_pull_executor: Optional[ConsumeExecutor] = None
        self._pull_callback_executor: Optional[ConsumeExecutor] = None
        self._async_pull_lock = threading.Lock()
        self._pull_executors_closed = False

    # ---------------- Pre-delivery filter hooks ----------------
    def register_filter_message_hook(self, hook: FilterMessageHook) -> None:
        """Register a pre-delivery filter hook."""
        if hook is not None:
            self.filter_message_hook_list.append(hook)

    def has_filter_message_hook(self) -> bool:
        return len(self.filter_message_hook_list) > 0

    def execute_filter_message_hook(self, context: FilterMessageContext) -> None:
        execute_filter_hooks(self.filter_message_hook_list, context)

    def _filter_messages_for_delivery(self, mq: MessageQueue, msgs: List[MessageExt]) -> List[MessageExt]:
        """Pull mode also goes through the filter hooks.

        Pull mode's tag filtering is left to the caller (when the pull interface passes no subscription data,
        tagsSet is empty -> no filtering); only hooks run here.
        """
        if not msgs or not self.filter_message_hook_list:
            return msgs
        context = FilterMessageContext(self.consumer_group, list(msgs), mq)
        context.unit_mode = self.unit_mode
        self.execute_filter_message_hook(context)
        return list(context.msg_list)

    # ---------------- Config ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        """Set the unit name: affects the clientId suffix and the dynamic addressing URL."""
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        """Set unit mode: passed through to the broker with the heartbeat/request headers."""
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        """Toggle the stream request type: affects the clientId suffix and every request's ReqT extension field."""
        self.enable_stream_request_type = bool(enable)

    def set_namespace_v2(self, namespace_v2: Optional[str]) -> None:
        """Set the 5.x server-side namespace: when non-empty, every request carries nsd=true / ns=<value>."""
        self.namespace_v2 = namespace_v2

    def get_namespace_v2(self) -> Optional[str]:
        return self.namespace_v2

    def set_vip_channel_enabled(self, enable: bool) -> None:
        """Turn the VIP channel on or off. With it on, the RPCs the broker answers on its fast
        channel -- offsets, queue locks, the group consumer list, message re-send-back,
        earliest-msg-store-time and every send, reply messages included -- target **the broker's
        listen port minus 2** instead of the plain one. Off by default.

        The rewrite itself lives on ``MQClientInstance``, so an
        already-started facade updates the shared instance too.
        """
        self.vip_channel_enabled = bool(enable)
        if self._mq_client is not None:
            self._mq_client.vip_channel_enabled = bool(enable)

    def is_vip_channel_enabled(self) -> bool:
        return self.vip_channel_enabled

    def set_use_heartbeat_v2(self, enable: bool) -> None:
        """Opt into the fingerprint-gated heartbeat V2 path; off by default here, so a client
        that never asks for it keeps the broker on its V1 registration path."""
        self.use_heartbeat_v2 = bool(enable)

    def set_decode_read_body(self, enable: bool) -> None:
        """Control whether received message bodies are materialised; on by default.

        The shared client instance is updated too, because decoding happens there.
        """
        self.decode_read_body = bool(enable)
        if self._mq_client is not None:
            self._mq_client.decode_read_body = bool(enable)

    def set_enable_concurrent_heartbeat(self, enable: bool) -> None:
        """Send one heartbeat per broker in parallel once the client instance starts."""
        self.enable_concurrent_heartbeat = bool(enable)
        if self._mq_client is not None:
            self._mq_client.enable_concurrent_heartbeat = bool(enable)

    def set_concurrent_heartbeat_thread_pool_size(self, size: int) -> None:
        """Worker count of the concurrent-heartbeat pool; applied when the instance starts."""
        self.concurrent_heartbeat_thread_pool_size = max(1, int(size))
        if self._mq_client is not None:
            self._mq_client.concurrent_heartbeat_thread_pool_size = max(1, int(size))

    def set_mq_client_api_timeout(self, timeout_millis: int) -> None:
        """Timeout of the client's internal API calls (heartbeat, route fetch, unregister,
        consumer-list lookup); per-call explicit timeouts still win."""
        self.mq_client_api_timeout = int(timeout_millis)
        if self._mq_client is not None:
            self._mq_client.mq_client_api_timeout = int(timeout_millis)

    def set_socks_proxy_config(self, socks_proxy_config: Optional[SocksProxyConfig]) -> None:
        pass
        self.socks_proxy_config = socks_proxy_config

    def get_socks_proxy_config(self) -> Optional[SocksProxyConfig]:
        return self.socks_proxy_config

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_message_queue_listener(self, listener: MessageQueueListener) -> None:
        self.message_queue_listener = listener

    def set_allocate_message_queue_strategy(
            self, strategy: Optional[AllocateMessageQueueStrategy]) -> None:
        """Set the queue allocation strategy: the setter does not validate; setting None is rejected by start()."""
        self.allocate_message_queue_strategy = strategy

    def get_register_topics(self) -> Set[str]:
        return self.register_topics

    # ---------------- Heartbeat (register the consume group with the broker) ----------------
    def _refresh_route_for_heartbeat(self) -> None:
        """Prepare broker addresses for the heartbeat: pull the routes of register_topics once and register them as "in use".

        This chain is indirect: what `subscriptions()` returns is the subscription set built from `register_topics`,
        which drives route refresh -> `broker_addr_table`
        having addresses -> the heartbeat can be sent. The heartbeat loop lives inside the consumer (the same division of labor as the push/lite consumers),
        so start explicitly refreshes once and hands the topic to
        MQClientInstance's background refresh task to keep it fresh.
        """
        client = self._require_client()
        for topic in sorted(self.register_topics):
            client.register_topic_in_use(topic)
            try:
                client.get_topic_publish_info(topic)
            except MQClientException as e:  # noqa: BLE001
                logger.debug("refresh route for %s failed: %s", topic, e)

    def _build_heartbeat(self) -> HeartbeatData:
        """The heartbeat data of a pull-mode consumer group.

        `consumeType` is always `CONSUME_ACTIVELY`,
        `consumeFromWhere` is always `CONSUME_FROM_LAST_OFFSET` -- the push consumer's
        PASSIVELY is a different policy, and neither broker-side field is for show:
        `consumerConnection` shows the consume type per consumeType;
        a broker with `rejectPullConsumerEnabled` **skips** ACTIVELY heartbeats
        and rejects unregistered pull-mode groups at pull time.

        The subscription set takes `subscriptions()`, building each with
        `build_subscription_data(topic, "*")`, and explicitly sets **subVersion to 0**
        -- pull mode has no "subscription version" semantics, and carrying the current timestamp
        would make the broker think the subscription changed on every heartbeat. WARNING: the subscription set comes only from `register_topics`.
        """
        hb = HeartbeatData(self.client_id or "")
        cd = ConsumerData(self.consumer_group, ConsumeType.CONSUME_ACTIVELY,
                          self.message_model, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
        cd.unit_mode = self.unit_mode
        for topic in sorted(self.register_topics):
            sub = FilterAPI.build_subscription_data(topic, FilterAPI.SUB_ALL)
            sub.sub_version = 0
            cd.subscription_data_set.add(sub)
        hb.consumer_data_set.add(cd)
        return hb

    def _send_heartbeat_to_all_broker(self) -> int:
        """Send one round of this group's heartbeat to all known brokers (including slaves), returning the number of successes.

        Recipients take `get_all_broker_addrs`, for the same reason as the push consumer's method of the same name
        (a slave is skipped only when the heartbeat carries no consumer data).
        """
        client = self._require_client()
        hb = self._build_heartbeat()
        if not self.use_heartbeat_v2 and self.enable_concurrent_heartbeat:
            ok = client.send_heartbeat_concurrently(hb, self.mq_client_api_timeout)
            if ok > 0:
                self._heartbeat_count += 1
            return ok
        ok = 0
        for addr in client.get_all_broker_addrs():
            try:
                # With use_heartbeat_v2 on the body carries a non-zero heartbeatFingerprint and the
                # round goes through the V2 state machine (client/heartbeat_v2.py): once a broker
                # answers IS_SUPPORT_HEART_BEAT_V2 and still holds our fingerprint, later rounds send
                # the cheap withoutSub body. A broker that never answers those extFields keeps getting
                # the full subscription set, so the plain HEART_BEAT(34) path stays intact.
                if self.use_heartbeat_v2:
                    client.send_heartbeat_to_broker_v2(
                        addr, hb, self.consumer_group, self.mq_client_api_timeout)
                else:
                    client.send_heartbeat(addr, hb, self.mq_client_api_timeout)
                ok += 1
            except Exception as e:  # noqa: BLE001
                logger.debug("heartbeat to %s failed: %s", addr, e)
        if ok > 0:
            self._heartbeat_count += 1
        return ok

    def heartbeat_count(self) -> int:
        """Number of successful heartbeat rounds (for live verification)."""
        return self._heartbeat_count

    def _start_heartbeat_loop(self) -> None:
        t = threading.Thread(target=self._heartbeat_loop, daemon=True,
                             name="rmq-pull-heartbeat-%s" % self.consumer_group)
        t.start()
        self._heartbeat_thread = t

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.is_set():
            if self._heartbeat_stop.wait(self.heartbeat_interval_millis / 1000.0):
                break
            if not self.heartbeat_enabled or not self._started:
                continue
            try:
                self._send_heartbeat_to_all_broker()
            except Exception as e:  # noqa: BLE001
                logger.debug("heartbeat loop error: %s", e)

    # ---------------- Lifecycle ----------------
    def start(self) -> None:
        if self._started:
            return
        # Purely local validation, placed before building the client instance; an illegal group name should not have to wait until a connection failure to be reported.
        validators.check_group(self.consumer_group)
        if self.consumer_group == MixAll.DEFAULT_CONSUMER_GROUP:
            raise MQClientException(
                "consumerGroup can not equal %s, please specify another one."
                % MixAll.DEFAULT_CONSUMER_GROUP)
        if not self.name_server_addrs:
            raise MQClientException("name server address is not set")
        # A None strategy rejects startup directly.
        if self.allocate_message_queue_strategy is None:
            raise MQClientException("allocateMessageQueueStrategy is null")
        # Only CLUSTERING rewrites instanceName; the clientId policy is
        # `<local IP>@<instanceName>[@<unitName>][@STREAM]`.
        if self.message_model == MessageModel.CLUSTERING:
            self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
        if self.client_id is None:
            self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                  self.enable_stream_request_type)
        def _create_instance() -> MQClientInstance:
            return MQClientInstance(self.client_id, self.name_server_addrs,
                                    socks_proxy_config=self.socks_proxy_config,
                                    enable_stream_request_type=self.enable_stream_request_type,
                                    unit_name=self.unit_name,
                                    namespace_v2=self.namespace_v2,
                                    poll_name_server_interval=self.poll_name_server_interval,
                                    vip_channel_enabled=self.vip_channel_enabled,
                                    decode_read_body=self.decode_read_body,
                                    mq_client_api_timeout=self.mq_client_api_timeout,
                                    enable_concurrent_heartbeat=self.enable_concurrent_heartbeat,
                                    concurrent_heartbeat_thread_pool_size=self.concurrent_heartbeat_thread_pool_size)
        self._mq_client, created = MQClientManager.get_instance() \
            .get_or_create_mq_client_instance(self.client_id, _create_instance)
        self._mq_client.decode_read_body = self.decode_read_body
        self._mq_client.mq_client_api_timeout = self.mq_client_api_timeout
        self._mq_client.enable_concurrent_heartbeat = self.enable_concurrent_heartbeat
        self._mq_client.concurrent_heartbeat_thread_pool_size = \
            self.concurrent_heartbeat_thread_pool_size
        if created and self.rpc_hook is not None:
            self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
        manager = MQClientManager.get_instance()
        manager.register_client(self.client_id, self._mq_client)
        try:
            self._mq_client.start()
        except Exception:
            manager.unregister_client(self.client_id, self._mq_client)
            raise
        self._started = True
        # Heartbeat: first refresh the routes of register_topics (no addresses means nobody to send to), then synchronously send one round so the broker
        # immediately knows this group, then hand off to the background loop. The order matches the lite pull consumer's start:
        # send immediately after registration, without waiting for the periodic task's first 1s.
        self._refresh_route_for_heartbeat()
        try:
            self._send_heartbeat_to_all_broker()
        except Exception as e:  # noqa: BLE001
            logger.debug("initial heartbeat failed: %s", e)
        self._start_heartbeat_loop()

    def shutdown(self) -> None:
        if not self._started:
            return
        self._started = False
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2.0)
        # Async pull dual pool: stop accepting new tasks and let in-flight tasks finish (the queue draining means threads exit naturally,
        # consistent with this repo's producer.shutdown approach of closing async pools).
        with self._async_pull_lock:
            self._pull_executors_closed = True
            if self._async_pull_executor is not None:
                self._async_pull_executor.shutdown()
                self._async_pull_executor = None
            if self._pull_callback_executor is not None:
                self._pull_callback_executor.shutdown()
                self._pull_callback_executor = None
        client = self._mq_client
        if client is not None:
            try:
                client.unregister_client_all_brokers(self.client_id or "", "",
                                                     self.consumer_group)
            except Exception as e:  # noqa: BLE001
                logger.debug("unregister on shutdown failed: %s", e)
            # Reference-count semantics: while another facade with the same clientId is still in use the instance stays alive,
            # and only shutdown when the count reaches zero.
            MQClientManager.get_instance().unregister_client(self.client_id or "", client)

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        return self._mq_client

    # ---------------- Pull ----------------
    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        # What is read is the subscription info table (read perm, no master filtering), not the publish info;
        # push and pull consumers share the same policy.
        return client.get_topic_subscribe_info(topic)

    def fetch_message_queues_in_balance(self, topic: str) -> List[MessageQueue]:
        """The queues this instance should be responsible for "after balancing": the caller's pull loop relies on it to decide which queues to pull.

        A common implementation reads the keys of the rebalance's process-queue table directly -- that table is the **product** of the background rebalance.
        This port's pull mode does not install a background rebalance (queues are managed by the caller, see the class comment),
        so it computes on the spot using the **same allocation formula** as the push consumer's topic rebalance:
        BROADCASTING gives all; CLUSTERING takes the subscription info (read perm policy, same source as
        `fetch_subscribe_message_queues`) as mq_all, queries `GET_CONSUMER_LIST_BY_GROUP`
        as cid_all, then hands them to this instance's configured allocation strategy. The formula shares the same
        policy as the push consumer's `_rebalance`, and if the two diverge the same queue would be claimed by two instances.

        When the route or consumer group list cannot be obtained (offline, broker not responding) **fall back to the key set of the local
        `pullFromWhichNodeTable`** (see `pull`) -- i.e. "keep the existing allocation":
        the queues this instance is actually pulling are its de facto allocation. It must never fall back to "owning all queues",
        otherwise multiple instances in the same group would consume each other's messages (the same iron law as the push consumer). The return value is given in the house sort
        order, ensuring stable ordering across calls.
        """
        client = self._require_client()  # Not started throws MQClientException directly
        if topic is None:
            # The wording is the keyword callers match on the exception message; changing it breaks compatibility.
            raise ValueError("topic is null")
        pulled = [mq for mq in self._pull_from_which_node if mq.topic == topic]
        if self.message_model == MessageModel.BROADCASTING:
            allocated = client.get_topic_subscribe_info(topic) or pulled
        else:
            allocated = None
            try:
                mq_all = sorted(client.get_topic_subscribe_info(topic), key=_mq_sort_key)
                cid_all = client.get_consumer_id_list_by_group(topic, self.consumer_group)
                if mq_all and cid_all:
                    allocated = self.allocate_message_queue_strategy.allocate(
                        self.consumer_group, self.client_id or "", mq_all, sorted(cid_all))
            except Exception as e:  # noqa: BLE001
                logger.debug("fetchMessageQueuesInBalance rebalance view unavailable: %s", e)
            if allocated is None:
                logger.debug("fetchMessageQueuesInBalance: no route/consumer list for %s/%s, "
                             "keep current assignment", self.consumer_group, topic)
                allocated = pulled
        # Restore the namespaced topic to the caller-side bare name:
        # this port's pull mode never rewrites resource names (see the namespace field comment); here it is equivalent to returning as-is.
        # Tighten by topic once more at the end -- compare topic by topic,
        # and do not let the allocation strategy's unexpected return value mix other topics into the caller's pull loop.
        return sorted([mq for mq in (allocated or []) if mq.topic == topic], key=_mq_sort_key)

    def pull(self, mq: MessageQueue, sub_expression: str = "*", offset: int = 0,
             max_nums: int = 32, timeout_millis: Optional[int] = None) -> PullResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.consumer_pull_timeout_millis
        sub = FilterAPI.build_subscription_data(mq.topic, sub_expression)
        # The four switch bits of sysFlag:
        #   sys_flag = PullSysFlag.build_sys_flag(false, block, true, false)
        # i.e. commit_offset=False, suspend=block. **pull()'s block=false** --
        # the offset is committed by the caller via update_consume_offset, and this is a **short poll** that does not suspend.
        # WARNING: this was once written as suspend=True: the broker would suspend to its own configured max at the end of the queue
        # (default 20s), while the client times out in 5s -> RemotingTimeoutException (always reproduced on live systems).
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                              subscription=True, class_filter=False)
        result = client.pull_message(self.consumer_group, mq, offset, max_nums,
                                     sys_flag, 0, sub.sub_string or "*",
                                     # subVersion passes 0 for TAG type (isTagType ? 0L : subVersion)
                                     0,
                                     ExpressionType.TAG, timeout_millis=timeout,
                                     max_msg_bytes=-1, suspend_timeout_millis=15000,
                                     broker_id=self._pull_from_which_node.get(
                                         mq, MixAll.MASTER_ID))
        self._pull_from_which_node[mq] = result.suggest_which_broker_id \
            if result.suggest_which_broker_id is not None else MixAll.MASTER_ID
        if result.status == PullStatus.FOUND and result.msg_found_list:
            result.msg_found_list = self._filter_messages_for_delivery(mq, result.msg_found_list)
        return result

    def pull_block_if_not_found(self, mq: MessageQueue, sub_expression: str, offset: int,
                                max_nums: int) -> PullResult:
        """Long-poll pull: block=true -> suspend waiting for a message."""
        client = self._require_client()
        sub = FilterAPI.build_subscription_data(mq.topic, sub_expression)
        # block=true: suspend=True lets the broker suspend until a message arrives; the timeout uses
        # consumer_timeout_millis_when_suspend (this longer timeout is used only when suspending to wait for a message).
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                              subscription=True, class_filter=False)
        result = client.pull_message(self.consumer_group, mq, offset, max_nums,
                                     sys_flag, 0, sub.sub_string or "*", 0,
                                     ExpressionType.TAG,
                                     timeout_millis=self.consumer_timeout_millis_when_suspend,
                                     max_msg_bytes=-1,
                                     suspend_timeout_millis=self.broker_suspend_max_time_millis,
                                     broker_id=self._pull_from_which_node.get(
                                         mq, MixAll.MASTER_ID))
        self._pull_from_which_node[mq] = result.suggest_which_broker_id \
            if result.suggest_which_broker_id is not None else MixAll.MASTER_ID
        if result.status == PullStatus.FOUND and result.msg_found_list:
            result.msg_found_list = self._filter_messages_for_delivery(mq, result.msg_found_list)
        return result

    # ---------------- Async pull ----------------
    def _ensure_async_pull_executors(self) -> Tuple[ConsumeExecutor, ConsumeExecutor]:
        """Lazily build the dual pool of pull_async (names/parameters aligned with the producer's ``_create_async_executors``)."""
        with self._async_pull_lock:
            if self._pull_executors_closed:
                raise MQClientException("consumer already shutdown")
            if self._async_pull_executor is None:
                cores = os.cpu_count() or 1
                self._async_pull_executor = ConsumeExecutor(
                    cores, cores, thread_name_prefix="AsyncPullExecutor",
                    max_queue_size=self.async_pull_queue_capacity,
                    thread_name_sep="_", thread_index_from=1)
            if self._pull_callback_executor is None:
                cores = os.cpu_count() or 1
                cb_threads = self.pull_callback_executor_threads
                if cb_threads <= 0:
                    cb_threads = cores
                self._pull_callback_executor = ConsumeExecutor(
                    cb_threads, cb_threads,
                    thread_name_prefix="PullCallbackExecutor",
                    thread_name_sep="_", thread_index_from=1)
            return self._async_pull_executor, self._pull_callback_executor

    def pull_async(self, mq: MessageQueue, sub_expression: str = "*", offset: int = 0,
                   max_nums: int = 32, callback: Optional[PullCallback] = None,
                   timeout_millis: Optional[int] = None) -> None:
        pass
        self._require_client()  # Not started throws synchronously, without waiting for the callback (debug-friendly, same as send_async)
        if callback is None:
            raise MQClientException("callback is required for pull_async")
        timeout = timeout_millis if timeout_millis is not None else self.consumer_pull_timeout_millis
        executor, callback_executor = self._ensure_async_pull_executors()
        begin = time.monotonic()

        def _deliver(result: Optional[PullResult], error: Optional[BaseException]) -> None:
            try:
                if error is None:
                    callback.on_success(result)
                else:
                    callback.on_exception(error)
            except BaseException:  # noqa: BLE001 -- a user callback exception must not kill the callback thread
                logger.exception("pull_async callback raised, mq=%s", mq)

        def _dispatch(result: Optional[PullResult], error: Optional[BaseException]) -> None:
            try:
                callback_executor.submit(_deliver, result, error)
            except RejectedExecutionError:
                # The pool is closed (the window where shutdown raced an in-flight task): deliver in place to guarantee exactly once.
                _deliver(result, error)

        def _run() -> None:
            cost = int((time.monotonic() - begin) * 1000)
            if timeout <= cost:
                _dispatch(None, RemotingTooMuchRequestException("ASYNC pull call timeout"))
                return
            try:
                result = self.pull(mq, sub_expression, offset, max_nums, timeout - cost)
            except BaseException as e:  # noqa: BLE001 -- RPC failure can only go into the callback
                _dispatch(None, e)
                return
            _dispatch(result, None)

        try:
            executor.submit(_run)
        except RejectedExecutionError as e:
            raise MQClientException("executor rejected", None, e) from e

    # ---------------- Offset management ----------------
    def fetch_consume_offset(self, mq: MessageQueue) -> Optional[int]:
        return self._require_client().query_consumer_offset(self.consumer_group, mq)

    def update_consume_offset(self, mq: MessageQueue, offset: int) -> None:
        self._require_client().update_consumer_offset(self.consumer_group, mq, offset)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        return self._require_client().search_offset_by_timestamp(mq, timestamp)

    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def earliest_msg_store_time(self, mq: MessageQueue) -> int:
        client = self._require_client()
        from remoting.protocol.codes import RequestCode
        from remoting.protocol.headers import (GetEarliestMsgStoretimeRequestHeader,
                                                 GetEarliestMsgStoretimeResponseHeader)
        from remoting.protocol.remoting_command import RemotingCommand
        # Same shape as max/min/search -- master only, one route refresh to re-query, and "The broker[X] not exist" is thrown if still unavailable.
        # "The broker[X] not exist" is the error wording callers match on; changing it breaks compatibility.
        addr = client._publish_addr_in_admin(mq)
        header = GetEarliestMsgStoretimeRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_EARLIEST_MSG_STORETIME, header)
        response = client._invoke_sync(client.vip_addr(addr), request, 5000)
        client._check_response(response)
        resp_header = GetEarliestMsgStoretimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.timestamp or 0

    def send_message_back(self, msg: MessageExt, delay_level: int) -> None:
        """Message redelivery (CONSUMER_SEND_MSG_BACK 36).

        Two points to note (stepped on live systems):
        1. The address is reverse-looked-up via `find_broker_address_in_publish(msg.broker_name)` in the **publish address table**
           (master only), so the caller must have accessed that topic with this consumer first.
        2. On failure, **no silent fallback**: some implementations swallow the exception on redelivery failure and use the internal default producer to send the message
           directly to `%RETRY%group`. This port does not fall back to that path -- on redelivery failure it throws, letting the caller see it,
           rather than silently resending via a different path.
        """
        client = self._require_client()
        from remoting.protocol.codes import RequestCode
        from remoting.protocol.headers import ConsumerSendMsgBackRequestHeader
        from remoting.protocol.remoting_command import RemotingCommand
        # Slaves do not accept CONSUMER_SEND_MSG_BACK (master only), so only brokerId=0 is recognized here
        addr = client.find_broker_address_in_publish(msg.broker_name or "")
        if addr is None:
            raise MQClientException("Broker[%s] master node does not exist" % msg.broker_name)
        header = ConsumerSendMsgBackRequestHeader()
        header.offset = msg.commit_log_offset
        header.group = self.consumer_group
        header.delay_level = delay_level
        header.origin_msg_id = msg.msg_id
        header.origin_topic = msg.topic
        header.unit_mode = self.unit_mode
        # WARNING: the configured max_reconsume_times (default -1) **cannot** be passed directly here:
        # after client version >= V3_4_9 the broker adopts that field unconditionally, and -1 would make
        # reconsumeTimes(0) >= -1 hold, putting the message directly into %DLQ%. Leave None to the subscription group's
        # retryMaxTimes judgment.
        header.max_reconsume_times = None
        request = RemotingCommand.create_request_command(RequestCode.CONSUMER_SEND_MSG_BACK, header)
        # SEND_MESSAGE_BACK is one of the VIP-wrapped RPCs and always goes to the master address.
        response = client._invoke_sync(client.vip_addr(addr), request, 5000)
        client._check_response(response)

    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        client.create_topic_in_route(new_topic, queue_num, queue_num, 6)


class DefaultLitePullConsumer:
    """Lite pull consumer.

    Essential differences from DefaultMQPullConsumer:
    - DefaultMQPullConsumer: the caller pulls queue by queue itself with ``pull(mq, offset)`` and manages offsets itself; no local buffer,
      no background pull threads, no rebalance.
    - DefaultLitePullConsumer: supports two modes --
      * **subscribe mode**: after registering subscriptions it automatically rebalances and allocates queues (same as push), with background pull threads feeding
        messages into a **local buffer**; ``poll()`` only takes messages from the local buffer, with no need for the caller to manage offsets;
      * **assign mode**: the caller explicitly specifies queues with ``assign([mq...])``, no rebalance, likewise feeding a local buffer in the background.
    Both modes take batches with ``poll(timeout)``; offsets default to autoCommit (committed to the broker once pulled).

    Design trade-offs:
    - A background **single** pull service thread sequentially iterates all assigned queues doing short polls (suspend=False), stuffing messages into
      a thread-safe local buffer ``_local_buffer``; ``poll()`` waits with a Condition and drains that buffer.
      Independent threads are not started per queue (semantically equivalent: local buffer + poll).
    - The subscribe mode's rebalance reuses the existing ``get_consumer_id_list_by_group`` + ``AllocateMessageQueueAveragely``,
      the same allocation algorithm as push consumers; when the consumer group list cannot be queried it "keeps the current allocation", never falling back to exclusive ownership.
    - No POP / push mode; no broker-initiated request (309/313) handling (that is the push consumer's responsibility).
    """

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING,
                 socks_proxy_config: Optional[SocksProxyConfig] = None):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id: Optional[str] = None
        # ---- unit / stream config ----
        # unitName takes part in the clientId's `@<unitName>` suffix and the dynamic addressing URL; unitMode is
        # reported to the broker along with the ConsumerData heartbeat, message redelivery request header and filter hooks;
        # ``enable_stream_request_type`` both appends the `@STREAM` suffix to clientId
        # (to prevent unexpected reuses of MQClientInstance) and adds
        # the `ReqT=0` extension field to every request. All three constructors default to true.
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = True
        # namespaceV2 (5.x server-side namespace): when non-empty, every request carries
        # nsd=true / ns=<value>, see the NamespaceRpcHook in remoting/rpchook.py.
        self.namespace_v2 = ""
        self.message_model = message_model
        self.socks_proxy_config: Optional[SocksProxyConfig] = socks_proxy_config
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook

        # The subscribe mode's subscription table (topic -> sub_expression)
        self.subscription: Dict[str, str] = {}
        # The SubscriptionData for subscriptions (with tagsSet), used for the heartbeat sent to the broker for tag filter registration
        self.subscription_data: Dict[str, SubscriptionData] = {}
        # Expression type per subscribed topic (TAG / SQL92 / ...): the pull request must carry the
        # same type as the subscription data registered by the heartbeat, otherwise a SQL92
        # expression would be evaluated by the broker as a tag string and silently match nothing.
        self.subscription_expr_type: Dict[str, str] = {}
        # assign mode: the tag filter expression when the caller explicitly specifies queues (passed through to pull)
        self._assign_sub_expr: Dict[str, str] = {}
        self._assign_mode = False
        self._assigned: Set[MessageQueue] = set()

        # The rebalance algorithm (subscribe mode)
        self.allocate_message_queue_strategy = AllocateMessageQueueAveragely()

        # Pull / poll config
        self.consume_from_where = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        # The initial value of the consume_timestamp field: now - 30 minutes.
        # Leaving it empty would degrade CONSUME_FROM_TIMESTAMP into "consume from the current moment".
        self.consume_timestamp = time.strftime(
            "%Y%m%d%H%M%S", time.localtime(time.time() - 30 * 60))
        self.pull_batch_size = 32
        self.poll_timeout_millis = 5000
        # Route refresh interval, default 30000ms; only passed through once when start() builds the MQClientInstance.
        self.poll_name_server_interval = 30000
        self.auto_commit = True
        self.auto_commit_interval_millis = 5000
        self.consumer_timeout_millis_when_suspend = 30000
        self.broker_suspend_max_time_millis = 20000
        self.pull_interval_millis = 50  # Backoff on empty polls at the queue tail, to avoid idle spinning hammering the broker
        self.pull_thread_nums = 1
        # The period of the background comparison of the queue sets of topics registered with register_topic_message_queue_change_listener, default 30s.
        self.topic_metadata_check_interval_millis = 30 * 1000
        # ---- Heartbeat V2 / VIP channel switches (both default false) ----
        # ``use_heartbeat_v2`` puts this consumer's HEART_BEAT(34) rounds on the fingerprint-gated
        # path (client/heartbeat_v2.py); ``vip_channel_enabled`` sends the send/reply RPCs and the
        # offset / lock RPCs to the broker's VIP port (listen port - 2).
        self.use_heartbeat_v2 = False
        self.vip_channel_enabled = False
        # Received message bodies are materialised by default; switching this off decodes only
        # the headers and leaves every body unset.
        self.decode_read_body = True
        self.enable_concurrent_heartbeat = False
        self.concurrent_heartbeat_thread_pool_size = os.cpu_count() or 1
        self.mq_client_api_timeout = 3000

        # Runtime state
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._running = False
        self._pull_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

        # Local buffer + offset cursor
        self._local_buffer: Deque[MessageExt] = deque()
        self._buffer_lock = threading.Lock()
        self._buffer_cond = threading.Condition(self._buffer_lock)
        # Pull cursor: only means "after the last one already fetched into the local buffer", advanced by _pull_one.
        self._next_offset: Dict[MessageQueue, int] = {}
        # Consumed cursor: advances only when poll() really hands messages to the caller
        # (the consumed cursor and the handed-off messages move together at that moment).
        # Only by splitting it from the pull cursor into two tables can commit(Map, persist) make sense: when the caller specifies "where to commit to"
        # it must never casually change "where to pull from next".
        self._consume_offset: Dict[MessageQueue, int] = {}
        # Commit landing: the in-memory offset table.
        # All three commit* entry points write this table first, sending it to the broker only when persist=true; committed() also reads it first,
        # i.e. the memory-first, store-second read order.
        self._offset_table: Dict[MessageQueue, int] = {}
        self._seek_offset: Dict[MessageQueue, int] = {}
        # Topic queue set change listener (the listener map plus the set of watched topics):
        # the key is the **namespaced** topic (namespace already wrapped in), and the callback also receives this key.
        self._topic_change_listeners: Dict[str, TopicMessageQueueChangeListener] = {}
        self._message_queues_for_topic: Dict[str, Set[MessageQueue]] = {}
        self._topic_listener_lock = threading.Lock()
        self._metadata_thread: Optional[threading.Thread] = None
        # nextAutoCommitDeadline: initial value -1 => commit once on the first poll.
        self._next_auto_commit_deadline = -1
        self._paused: Set[MessageQueue] = set()
        self._message_queue_listener = None
        self._last_rebalance_ts = 0

    # ---------------- Namespace ----------------
    def _with_namespace(self, topic: str) -> str:
        if self.namespace and not topic.startswith(self.namespace + "%"):
            return self.namespace + "%" + topic
        return topic

    # ---------------- Config ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        """Set the unit name: affects the clientId suffix and the dynamic addressing URL."""
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        """Set unit mode: passed through to the broker with the heartbeat/request headers."""
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        """Toggle the stream request type: affects the clientId suffix and every request's ReqT extension field."""
        self.enable_stream_request_type = bool(enable)

    def set_namespace_v2(self, namespace_v2: Optional[str]) -> None:
        """Set the 5.x server-side namespace: when non-empty, every request carries nsd=true / ns=<value>."""
        self.namespace_v2 = namespace_v2

    def get_namespace_v2(self) -> Optional[str]:
        return self.namespace_v2

    def set_vip_channel_enabled(self, enable: bool) -> None:
        """Turn the VIP channel on or off. With it on, the RPCs the broker answers on its fast
        channel -- offsets, queue locks, the group consumer list, message re-send-back,
        earliest-msg-store-time and every send, reply messages included -- target **the broker's
        listen port minus 2** instead of the plain one. Off by default.

        The rewrite itself lives on ``MQClientInstance``, so an
        already-started facade updates the shared instance too.
        """
        self.vip_channel_enabled = bool(enable)
        if self._mq_client is not None:
            self._mq_client.vip_channel_enabled = bool(enable)

    def is_vip_channel_enabled(self) -> bool:
        return self.vip_channel_enabled

    def set_use_heartbeat_v2(self, enable: bool) -> None:
        """Opt into the fingerprint-gated heartbeat V2 path; off by default here, so a client
        that never asks for it keeps the broker on its V1 registration path."""
        self.use_heartbeat_v2 = bool(enable)

    def set_decode_read_body(self, enable: bool) -> None:
        """Control whether received message bodies are materialised; on by default.

        The shared client instance is updated too, because decoding happens there.
        """
        self.decode_read_body = bool(enable)
        if self._mq_client is not None:
            self._mq_client.decode_read_body = bool(enable)

    def set_enable_concurrent_heartbeat(self, enable: bool) -> None:
        """Send one heartbeat per broker in parallel once the client instance starts."""
        self.enable_concurrent_heartbeat = bool(enable)
        if self._mq_client is not None:
            self._mq_client.enable_concurrent_heartbeat = bool(enable)

    def set_concurrent_heartbeat_thread_pool_size(self, size: int) -> None:
        """Worker count of the concurrent-heartbeat pool; applied when the instance starts."""
        self.concurrent_heartbeat_thread_pool_size = max(1, int(size))
        if self._mq_client is not None:
            self._mq_client.concurrent_heartbeat_thread_pool_size = max(1, int(size))

    def set_mq_client_api_timeout(self, timeout_millis: int) -> None:
        """Timeout of the client's internal API calls (heartbeat, route fetch, unregister,
        consumer-list lookup); per-call explicit timeouts still win."""
        self.mq_client_api_timeout = int(timeout_millis)
        if self._mq_client is not None:
            self._mq_client.mq_client_api_timeout = int(timeout_millis)

    def set_socks_proxy_config(self, socks_proxy_config: Optional[SocksProxyConfig]) -> None:
        pass
        self.socks_proxy_config = socks_proxy_config

    def get_socks_proxy_config(self) -> Optional[SocksProxyConfig]:
        return self.socks_proxy_config

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_namespace(self, ns: str) -> None:
        self.namespace = ns

    def set_rpc_hook(self, hook: RPCHook) -> None:
        self.rpc_hook = hook

    def set_consume_from_where(self, where: str) -> None:
        self.consume_from_where = where

    def set_consume_timestamp(self, ts: str) -> None:
        self.consume_timestamp = ts

    def set_pull_batch_size(self, n: int) -> None:
        self.pull_batch_size = max(1, int(n))

    def set_poll_timeout_millis(self, ms: int) -> None:
        self.poll_timeout_millis = max(0, int(ms))

    def set_auto_commit(self, auto: bool) -> None:
        self.auto_commit = bool(auto)

    def set_auto_commit_interval_millis(self, ms: int) -> None:
        self.auto_commit_interval_millis = max(0, int(ms))

    def set_consumer_timeout_millis_when_suspend(self, ms: int) -> None:
        self.consumer_timeout_millis_when_suspend = int(ms)

    def set_broker_suspend_max_time_millis(self, ms: int) -> None:
        self.broker_suspend_max_time_millis = int(ms)

    def set_pull_interval_millis(self, ms: int) -> None:
        self.pull_interval_millis = max(0, int(ms))

    def set_topic_metadata_check_interval_millis(self, ms: int) -> None:
        # Lower bound 1s: the config itself has no lower bound, but 0/negative would make the fixed-rate scheduling throw directly;
        # here it degrades to the fastest one trip per 1s.
        self.topic_metadata_check_interval_millis = max(1000, int(ms))

    def get_topic_metadata_check_interval_millis(self) -> int:
        return self.topic_metadata_check_interval_millis

    def set_allocate_message_queue_strategy(self, strategy) -> None:
        self.allocate_message_queue_strategy = strategy

    def set_message_queue_listener(self, listener) -> None:
        self._message_queue_listener = listener

    # ---------------- Subscription / allocation ----------------
    def subscribe(self, topic: str, sub_expression: str = "*") -> None:
        self._assign_mode = False
        ns = self._with_namespace(topic)
        self.subscription[ns] = sub_expression
        self.subscription_expr_type[ns] = ExpressionType.TAG
        # Build the SubscriptionData synchronously, so the heartbeat registers the tag subscription with the broker (otherwise the broker does not recognize tag filtering)
        try:
            self.subscription_data[ns] = FilterAPI.build_subscription_data(ns, sub_expression)
        except Exception:
            self.subscription_data.pop(ns, None)

    def subscribe_with_selector(self, topic: str, selector) -> None:
        # Carry the selector's expression type end-to-end: TAG keeps the tag semantics
        # (heartbeat tagsSet/codeSet + TAG on the wire); other types (SQL92 / CLASS_FILTER)
        # register selector-typed subscription data and the pull request repeats that type,
        # so the broker evaluates the expression instead of reading it as a tag string.
        selector_type = getattr(selector, "type", ExpressionType.TAG) or ExpressionType.TAG
        if selector_type == ExpressionType.TAG:
            self.subscribe(topic, getattr(selector, "expression", "*") or "*")
            return
        expression = getattr(selector, "expression", None)
        if not expression:
            raise ValueError("Expression can't be null! %s" % selector_type)
        self._assign_mode = False
        ns = self._with_namespace(topic)
        self.subscription[ns] = expression
        self.subscription_expr_type[ns] = selector_type
        # Same build policy as the push side: non-tag types only set topic/subString/expressionType,
        # leaving tagsSet/codeSet empty (the expression is not a tag list).
        sub = SubscriptionData(topic=ns, sub_string=expression)
        sub.expression_type = selector_type
        self.subscription_data[ns] = sub

    def unsubscribe(self, topic: str) -> None:
        ns = self._with_namespace(topic)
        self.subscription.pop(ns, None)
        self.subscription_data.pop(ns, None)
        self.subscription_expr_type.pop(ns, None)

    def set_sub_expression_for_assign(self, topic: str, sub_expression: str) -> None:
        """Specify a tag filter expression for a topic's queues in assign mode (setSubExpressionForAssign).

        Also builds the SubscriptionData registered with the heartbeat, so the broker filters by that tag (assign mode also needs the broker to know the subscription).
        """
        ns = self._with_namespace(topic)
        self._assign_sub_expr[ns] = sub_expression
        try:
            self.subscription_data[ns] = FilterAPI.build_subscription_data(ns, sub_expression)
        except Exception:
            self.subscription_data.pop(ns, None)

    def assign(self, message_queues) -> None:
        self._assign_mode = True
        queues = set(message_queues)
        # Reassigning drops the revoked queues along with their whole per-queue state
        # (the pull cursor and consumed cursor disappear together). It does **not** touch the in-memory offset table
        # -- that cleanup hangs off the rebalance step that releases a no-longer-needed queue,
        # and assign mode does not go through rebalance, so persist does not happen here either.
        for mq in self._assigned - queues:
            self._next_offset.pop(mq, None)
            self._consume_offset.pop(mq, None)
        self._assigned = queues
        for mq in self._assigned:
            if mq not in self._next_offset:
                try:
                    self._next_offset[mq] = self._resolve_initial_offset(mq)
                except Exception:
                    logger.debug("assign: resolve initial offset failed for %s", mq)

    # ---------------- Lifecycle ----------------
    def _create_client(self) -> MQClientInstance:
        return MQClientInstance(self.client_id, self.name_server_addrs,
                                socks_proxy_config=self.socks_proxy_config,
                                enable_stream_request_type=self.enable_stream_request_type,
                                unit_name=self.unit_name,
                                namespace_v2=self.namespace_v2,
                                poll_name_server_interval=self.poll_name_server_interval,
                                vip_channel_enabled=self.vip_channel_enabled,
                                decode_read_body=self.decode_read_body,
                                mq_client_api_timeout=self.mq_client_api_timeout,
                                enable_concurrent_heartbeat=self.enable_concurrent_heartbeat,
                                concurrent_heartbeat_thread_pool_size=self.concurrent_heartbeat_thread_pool_size)

    def start(self) -> None:
        if self._started:
            return
        # Purely local validation, done first
        validators.check_group(self.consumer_group)
        if self.consumer_group == MixAll.DEFAULT_CONSUMER_GROUP:
            raise MQClientException(
                "consumerGroup can not equal %s, please specify another one."
                % MixAll.DEFAULT_CONSUMER_GROUP)
        if not self.name_server_addrs:
            raise MQClientException("name server address is not set")
        if not self.subscription and not self._assign_mode:
            raise MQClientException("subscription is not set, call subscribe() or assign() first")
        # Unconditionally validated at startup:
        # the loop below resolving initial offsets swallows exceptions; throwing late would be a silent degradation to consuming from max offset.
        self._parse_consume_timestamp(self.consume_timestamp)
        # A None strategy rejects startup directly,
        # rather than waiting for None.allocate in rebalance to throw AttributeError and be silently swallowed.
        if self.allocate_message_queue_strategy is None:
            raise MQClientException("allocateMessageQueueStrategy is null")
        # Only CLUSTERING rewrites instanceName; the clientId policy is
        # `<local IP>@<instanceName>[@<unitName>][@STREAM]`.
        if self.message_model == MessageModel.CLUSTERING:
            self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
        if self.client_id is None:
            self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                  self.enable_stream_request_type)
        self._mq_client, created = MQClientManager.get_instance() \
            .get_or_create_mq_client_instance(self.client_id, self._create_client)
        self._mq_client.decode_read_body = self.decode_read_body
        self._mq_client.mq_client_api_timeout = self.mq_client_api_timeout
        self._mq_client.enable_concurrent_heartbeat = self.enable_concurrent_heartbeat
        self._mq_client.concurrent_heartbeat_thread_pool_size = \
            self.concurrent_heartbeat_thread_pool_size
        if created and self.rpc_hook is not None:
            self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
        manager = MQClientManager.get_instance()
        manager.register_client(self.client_id, self._mq_client)
        try:
            self._mq_client.start()
        except Exception:
            manager.unregister_client(self.client_id, self._mq_client)
            raise
        # assign mode resolves initial offsets at start
        if self._assign_mode:
            for mq in list(self._assigned):
                if mq not in self._next_offset:
                    try:
                        self._next_offset[mq] = self._resolve_initial_offset(mq)
                    except Exception:
                        logger.debug("start: resolve initial offset failed for %s", mq)
        # First register the tag subscriptions with the broker (heartbeat), then start the background pull, so the first pull round does not lose messages because the broker does not recognize the subscription.
        # The heartbeat relies on the "known broker list" to be sent, and that list comes only from the topic route table: at start, first pull the routes of subscribed/assigned
        # topics once and register them as "in use" (updateTopicRouteInfoFromNameServer necessarily happens before sending the heartbeat).
        # Without this step, the first heartbeat is silently not sent for lack of brokers =>
        # the broker side cannot see this instance => multi-instance rebalance each exclusively owns all queues (consuming each other's messages).
        self._refresh_route_for_heartbeat()
        self._running = True
        self._send_heartbeat_to_all_broker()
        self._start_heartbeat_loop()
        self._started = True
        self._pull_thread = threading.Thread(
            target=self._pull_service_loop, daemon=True,
            name="rmq-lite-pull-%s" % self.consumer_group)
        self._pull_thread.start()
        # startScheduleTask(): the periodic comparison thread for queue set changes,
        # regardless of whether a listener is registered (it is started anyway).
        self._metadata_thread = threading.Thread(
            target=self._metadata_loop, daemon=True,
            name="rmq-lite-metadata-%s" % self.consumer_group)
        self._metadata_thread.start()

    def _refresh_route_for_heartbeat(self) -> None:
        """Prepare broker addresses for the heartbeat: pull and register the routes of the topics this instance cares about."""
        topics = list(self.subscription.keys())
        topics += [mq.topic for mq in self._assigned if mq.topic not in topics]
        for topic in topics:
            self._mq_client.register_topic_in_use(topic)
            try:
                self._mq_client.get_topic_publish_info(topic)
            except Exception as e:  # noqa: BLE001
                logger.debug("lite start: refresh route for %s failed: %s", topic, e)

    def shutdown(self) -> None:
        if not self._started:
            return
        self._started = False
        self._running = False
        # shutdown goes through the offset persist step (_persist_offset_table): flush the in-memory offset table once per the currently held queues,
        # regardless of auto_commit (values accumulated with persist=False in manual mode must also be persisted).
        # Auto-commit mode takes one more step, commit(): this port does not have that 5s timer,
        # so offsets "handed out by poll but not yet at the deadline" must be made up here, otherwise they would be redelivered from the previous slot after restart.
        try:
            if self.auto_commit:
                self.commit()
            else:
                self._persist_offset_table(self._assigned)
        except Exception:  # noqa: BLE001
            logger.debug("shutdown commit failed")
        # Wake any waiting poll() so it returns as soon as possible after shutdown
        with self._buffer_cond:
            self._buffer_cond.notify_all()
        if self._pull_thread is not None:
            self._pull_thread.join(timeout=2.0)
        if self._metadata_thread is not None:
            self._metadata_thread.join(timeout=2.0)
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2.0)
        if self._mq_client is not None:
            # Reference-count semantics: while another facade with the same clientId is still in use the instance stays alive,
            # and only shutdown when the count reaches zero.
            MQClientManager.get_instance().unregister_client(
                self.client_id or "", self._mq_client)

    def is_running(self) -> bool:
        return self._running

    # ---------------- Heartbeat (register the tag subscriptions with the broker) ----------------
    def _build_heartbeat(self) -> HeartbeatData:
        hb = HeartbeatData(self.client_id or "")
        cd = ConsumerData(self.consumer_group, ConsumeType.CONSUME_ACTIVELY,
                          self.message_model, self.consume_from_where)
        # consumeType is CONSUME_ACTIVELY and **not** the push consumer's PASSIVELY,
        # and the broker side splits by it: a broker with `rejectPullConsumerEnabled=true` skips ACTIVELY
        # heartbeats and rejects unregistered pull groups; cold-data flow control also takes two different branches per PASSIVELY/ACTIVELY.
        # Writing PASSIVELY would misalign both.
        # See the same-named comment on the push consumer's _build_heartbeat
        cd.unit_mode = self.unit_mode
        for sub in self.subscription_data.values():
            cd.subscription_data_set.add(sub)
        hb.consumer_data_set.add(cd)
        return hb

    def _send_heartbeat_to_all_broker(self) -> int:
        if self._mq_client is None:
            return 0
        hb = self._build_heartbeat()
        if not self.use_heartbeat_v2 and self.enable_concurrent_heartbeat:
            return self._mq_client.send_heartbeat_concurrently(
                hb, self.mq_client_api_timeout)
        ok = 0
        # Sent to every broker (master + slave): same rationale as the push consumer's `DefaultMQPushConsumer._send_heartbeat_to_all_broker`
        # -- a slave is skipped only when the heartbeat carries no consumer data; pull mode carries a subscription set, so slaves must be registered too.
        for addr in self._mq_client.get_all_broker_addrs():
            try:
                # Heartbeat V2 for the lite pull consumer: same state machine as the push/pull
                # consumers (see client/heartbeat_v2.py), keyed by this consumer group.
                if self.use_heartbeat_v2:
                    self._mq_client.send_heartbeat_to_broker_v2(
                        addr, hb, self.consumer_group, self.mq_client_api_timeout)
                else:
                    self._mq_client.send_heartbeat(addr, hb, self.mq_client_api_timeout)
                ok += 1
            except Exception as e:  # noqa: BLE001
                logger.debug("lite heartbeat to %s failed: %s", addr, e)
        return ok

    def _start_heartbeat_loop(self) -> None:
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name="rmq-lite-hb-%s" % self.consumer_group)
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                self._send_heartbeat_to_all_broker()
            except Exception:  # noqa: BLE001
                logger.debug("lite heartbeat loop error")
            # 5s heartbeat interval (same as the push consumer)
            for _ in range(50):
                if not self._running:
                    break
                time.sleep(0.1)

    # ---------------- Pull service ----------------
    def _pull_service_loop(self) -> None:
        while self._running:
            try:
                now = time.time() * 1000.0
                if not self._assign_mode and (self._last_rebalance_ts == 0
                                             or (now - self._last_rebalance_ts) > 1000):
                    self._rebalance()
                    self._last_rebalance_ts = now
                targets = list(self._assigned)
                got_any = False
                for mq in targets:
                    if not self._running:
                        break
                    if mq in self._paused:
                        continue
                    if self._pull_one(mq):
                        got_any = True
            except Exception as e:  # noqa: BLE001
                logger.debug("lite pull service error: %s", e)
            # Backoff: slow down on empty polls to avoid hammering the broker; when there are messages, refill the buffer as soon as possible
            time.sleep(self.pull_interval_millis / 1000.0 if not got_any else 0.005)

    def _subscription_for(self, topic: str) -> str:
        if topic in self.subscription:
            return self.subscription[topic]
        if topic in self._assign_sub_expr:
            return self._assign_sub_expr[topic]
        return "*"

    def _pull_one(self, mq: MessageQueue) -> bool:
        offset = self._next_offset.get(mq)
        if offset is None:
            offset = self._resolve_initial_offset(mq)
            self._next_offset[mq] = offset
        sub = self._subscription_for(mq.topic) or "*"
        # The expression type that goes on the wire must match the subscription data the heartbeat
        # registered; an unknown topic (assign mode without an explicit expression) keeps tag semantics.
        exp_type = self.subscription_expr_type.get(mq.topic, ExpressionType.TAG)
        # Short poll (suspend=False), with offsets committed separately by auto-commit, not carried along on the pull request.
        # lite_pull=True sets FLAG_LITE_PULL_MESSAGE (buildSysFlag(false, block, true, false, true)),
        # and that flag decides the request code switches to LITE_PULL_MESSAGE(361): the broker distinguishes pull-mode subscription semantics by this code,
        # and sending the wrong one would filter per the consume group's subscription, which in assign mode the broker side simply does not have.
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                              subscription=True, class_filter=False,
                                              lite_pull=True)
        try:
            result = self._mq_client.pull_message(
                self.consumer_group, mq, offset, self.pull_batch_size,
                sys_flag, 0, sub, 0, exp_type,
                timeout_millis=30000, max_msg_bytes=-1,
                suspend_timeout_millis=15000)
        except Exception as e:  # noqa: BLE001
            logger.debug("lite pull_one failed for %s@%d: %s", mq.topic, mq.queue_id, e)
            return False
        # After a pull round comes back:
        # regardless of FOUND / NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL, advance the pull cursor to
        # the broker's nextBeginOffset -- NO_MATCHED_MSG's nextBeginOffset has already passed the entire unmatched range swept this
        # round (not following it would re-sweep the same range every round), and OFFSET_ILLEGAL's
        # nextBeginOffset is the broker's corrected offset (out-of-bounds self-healing relies on this step too).
        # There is only one brake: **the result of an in-flight request must not overwrite the offsets just seeked / just revoked in this round**.
        # This port's seek() rewrites _next_offset directly, and revoking a queue pops the entry directly,
        # so "is the cursor still the value it was when I sent the request" is used to judge.
        # The same brake also governs the FOUND branch's enqueueing into the buffer.
        intact = self._next_offset.get(mq) == offset
        if intact:
            self._next_offset[mq] = result.next_begin_offset
        if intact and result.status == PullStatus.FOUND and result.msg_found_list:
            if exp_type == ExpressionType.TAG:
                msgs = self._filter_tags(mq.topic, result.msg_found_list, sub)
            else:
                # Non-tag expressions are evaluated by the broker; a client-side tag pass would
                # read the SQL/class expression as a tag list and drop everything it matched.
                msgs = result.msg_found_list
            if msgs:
                self._enqueue(msgs)
                return True
        return False

    def _filter_tags(self, topic, msgs, sub):
        if not sub or sub == "*":
            return msgs
        try:
            sub_data = FilterAPI.build_subscription_data(topic, sub)
        except Exception:
            return msgs
        if not sub_data.tags_set:
            return msgs
        kept = []
        for m in msgs:
            # Note: the really pulled-back MessageExt puts the tag in properties["TAGS"] (which get_tags() reads),
            # and the instance attribute .tags is None; unit tests explicitly set_tags, so both sources are supported here.
            tag = getattr(m, "tags", None) or m.get_tags()
            if tag in sub_data.tags_set:
                kept.append(m)
        return kept

    def _enqueue(self, msgs: List[MessageExt]) -> None:
        with self._buffer_cond:
            self._local_buffer.extend(msgs)
            self._buffer_cond.notify_all()

    def _maybe_auto_commit(self) -> None:
        """Commit **all** assigned queues when the time comes (maybeAutoCommit).

        The only call site is the start of ``poll()`` (plus ``shutdown()``'s fallback commit):
        an idle consumer, even with a 5s offset-persist timer, has that timer flushing the **in-memory offset table**,
        and that table is written only by the ``commit`` path, so stopping poll does not advance the broker offset.
        Checking the auto-commit inside the pull loop here would become "committing even when nobody polls", which is more aggressive than the semantics should be, so it is not placed there.

        What is committed is the "consumed cursor" rather than the pull cursor -- messages in the buffer not yet handed to the caller do not count as consumed,
        the same policy as the buffer-removal step, which advances the consumed cursor only in poll.
        """
        now = time.time() * 1000.0
        if now < self._next_auto_commit_deadline:
            return
        self._next_auto_commit_deadline = now + self.auto_commit_interval_millis
        self.commit()

    def _resolve_initial_offset(self, mq: MessageQueue) -> int:
        if mq in self._seek_offset:
            return self._seek_offset[mq]
        if self._mq_client is None:
            # assign/seek usually checkServiceState() first, rejecting when not started. This port allows assign
            # first then start, but **cannot** give 0 without having asked the broker -- assign stores the result into
            # _next_offset, and once stored it is never resolved again (the pull loop only backfills on missing values), pinning the start
            # permanently at 0. Throw to let the caller take note; the real resolution is left to the pull loop after start.
            raise MQClientException("The lite pull consumer is not running")
        # Use the committed offset if there is one (to guarantee resuming consumption after restart)
        try:
            off = self._mq_client.query_consumer_offset(self.consumer_group, mq)
            if off is not None:
                return off
        except Exception:
            pass
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
            # The FIRST_OFFSET branch is the same shape as push (take 0 directly),
            # and no minOffset query is sent -- minOffset is the admin-side policy (master only),
            # which while the master is down would leave a newly started lite-pull unable to pull anything; out-of-bounds is corrected by the broker's
            # PULL_OFFSET_MOVED (_offset_illegal_recover).
            return 0
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
            ts = self._parse_consume_timestamp(self.consume_timestamp)
            if NamespaceUtil.is_retry_topic(mq.topic):
                # Same rule as push: a %RETRY% queue cannot be searched backwards from a
                # timestamp, so it starts at the queue tail.
                return self._mq_client.get_max_offset(mq)
            return self._mq_client.search_offset_by_timestamp(mq, ts)
        if NamespaceUtil.is_retry_topic(mq.topic):
            # Default LAST_OFFSET for a %RETRY% queue: start at 0 so every queued retry
            # is delivered.
            return 0
        return self._mq_client.get_max_offset(mq)

    def _parse_consume_timestamp(self, ts: str) -> int:
        # The consumeTimestamp field is **only** a 14-digit local wall-clock date (yyyyMMddHHmmss).
        # The old implementation first went through an isdigit() branch treating pure digits as epoch seconds/millis, so "20230101000000"
        # was therefore interpreted as the year 2611 AD, while the strptime branch below was never reachable.
        try:
            if len(str(ts)) != 14:
                raise ValueError(ts)
            return int(time.mktime(time.strptime(str(ts), "%Y%m%d%H%M%S")) * 1000)
        except (ValueError, TypeError, OverflowError):
            raise MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received %s" % ts)

    # ---------------- rebalance (subscribe mode) ----------------
    def _rebalance(self) -> None:
        # It goes through the **exactly identical** topic-rebalance path as push, so mq_all / cid_all must both be sorted first.
        # Inconsistent ordering would make different instances in the same group compute conflicting allocations (the same queue consumed by two instances simultaneously).
        new_set: Set[MessageQueue] = set()
        for topic in list(self.subscription.keys()):
            try:
                # Same source as push's rebalance: the same queue-set computation,
                # with mq_all coming from the subscription info (read perm, no master filtering).
                mq_all = sorted(self._mq_client.get_topic_subscribe_info(topic),
                                key=_mq_sort_key)
            except Exception:
                mq_all = []
            cid_all = sorted(self._mq_client.get_consumer_id_list_by_group(topic, self.consumer_group) or [])
            if self.client_id not in cid_all:
                cid_all = sorted(cid_all + [self.client_id])
            try:
                allocated = self.allocate_message_queue_strategy.allocate(
                    self.consumer_group, self.client_id, mq_all, cid_all)
            except Exception as e:  # noqa: BLE001
                # Same policy as the topic rebalance's catch-all: only log --
                # skip this topic this round (keeping its current allocation), and never take the queue away because of one strategy exception.
                logger.error("allocate message queue exception, strategy=%s: %s",
                             _strategy_name(self.allocate_message_queue_strategy), e)
                new_set |= {mq for mq in self._assigned if mq.topic == topic}
                continue
            new_set |= set(allocated)
        if new_set != self._assigned:
            old = self._assigned
            self._assigned = new_set
            for mq in (new_set - old):
                if mq not in self._next_offset:
                    try:
                        self._next_offset[mq] = self._resolve_initial_offset(mq)
                    except Exception:
                        logger.debug("rebalance: resolve offset failed for %s", mq)
            for mq in (old - new_set):
                # Releasing a queue this instance no longer holds: persist(mq) first, then drop it from the offset table --
                # send out the last committed offset before letting go,
                # so an already consumed queue is not redelivered from the previous 5s window.
                self._persist_offset(mq)
                self._next_offset.pop(mq, None)
                # The per-queue assignment entries are dropped too: on a queue revoke this port
                # discards the **entire per-queue state** (pull / consume / seek offsets),
                # and keeping the seek offset would let this queue silently jump back to a position the user manually
                # pinned long ago should it return to this instance someday. That in-memory offset slot was already cleared by the persist above.
                self._consume_offset.pop(mq, None)
                self._offset_table.pop(mq, None)
                self._seek_offset.pop(mq, None)
            if self._message_queue_listener is not None:
                try:
                    self._message_queue_listener.message_queue_changed(list(new_set), list(old))
                except Exception:
                    pass

    # ---------------- poll / offsets ----------------
    def poll(self, timeout: Optional[int] = None) -> List[MessageExt]:
        if timeout is None:
            timeout = self.poll_timeout_millis
        if self.auto_commit:
            self._maybe_auto_commit()
        deadline = time.time() + (timeout / 1000.0)
        with self._buffer_cond:
            while not self._local_buffer:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return []
                self._buffer_cond.wait(remaining)
            out: List[MessageExt] = []
            while self._local_buffer and len(out) < 1024:
                out.append(self._local_buffer.popleft())
        held = {(q.topic, q.broker_name, q.queue_id): q for q in self._next_offset}
        advanced: Dict[MessageQueue, int] = {}
        for m in out:
            mq = held.get((m.topic, m.broker_name, m.queue_id))
            if mq is None:
                continue
            nxt = m.queue_offset + 1
            if nxt > advanced.get(mq, -1):
                advanced[mq] = nxt
        self._consume_offset.update(advanced)
        return out

    def seek(self, mq: MessageQueue, offset: int) -> None:
        self._seek_offset[mq] = offset
        self._next_offset[mq] = offset
        self._consume_offset[mq] = offset
        with self._buffer_cond:
            kept = [m for m in self._local_buffer
                    if not (m.topic == mq.topic and m.broker_name == mq.broker_name
                            and m.queue_id == mq.queue_id and m.queue_offset < offset)]
            self._local_buffer = deque(kept)

    def seek_to_begin(self, mq: MessageQueue) -> None:
        self.seek(mq, self._mq_client.get_min_offset(mq))

    def seek_to_end(self, mq: MessageQueue) -> None:
        self.seek(mq, self._mq_client.get_max_offset(mq))

    def pull_cursor_of(self, mq: MessageQueue) -> int:
        """The pull cursor (observation point: what a live comparison looks at is exactly the difference between "how much was pulled" and "how much was handed out")."""
        return self._next_offset.get(mq, -1)

    def consume_cursor_of(self, mq: MessageQueue) -> int:
        """The consumed cursor (the slot ``poll()`` hands out)."""
        return self._consume_offset.get(mq, -1)

    def pending_commit_of(self, mq: MessageQueue) -> int:
        """The slot in the in-memory offset table (where values committed with ``persist=False`` but not yet persisted sit)."""
        return self._offset_table.get(mq, -1)

    def committed(self, mq: MessageQueue) -> Optional[int]:
        """Read the committed offset memory-first, store-second:
        look at the in-memory offset table first (values just committed with ``persist=False`` but not yet sent to the broker also count),
        then ask the broker, and backfill the broker's value into the table."""
        cached = self._offset_table.get(mq)
        if cached is not None:
            return cached
        broker_offset = self._mq_client.query_consumer_offset(self.consumer_group, mq)
        if broker_offset is not None:
            self._offset_table[mq] = broker_offset
        return broker_offset

    def _persist_offset(self, mq: MessageQueue) -> None:
        """Persist one queue: only sends this queue's in-memory offset to the broker, with no cleanup."""
        offset = self._offset_table.get(mq)
        if offset is None:
            return
        try:
            self._mq_client.update_consumer_offset(self.consumer_group, mq, offset)
        except Exception as e:  # noqa: BLE001
            logger.debug("lite persist failed for %s: %s", mq, e)

    def _persist_offset_table(self, mqs) -> None:
        """Persist a set of queues: the part of the in-memory offset table falling on ``mqs``
        is written to the broker, and entries **not** in it are deleted from the table on the way (the sweep of queues this instance no longer holds).

        The second half is this table's real semantics: it only serves the currently held queues, and revoked queues left in the table would never be
        committed again, so this persist sweeps them away as it goes. The cost is that ``commit(some queues, persist=True)`` drops the other
        queues' **not yet persisted** in-memory values along with them -- whoever is to be committed must be given in full, and do not expect the other queues from the previous round to still be in the table.
        """
        wanted = set(mqs)
        if not wanted:
            return
        for mq in list(self._offset_table.keys()):
            if mq not in wanted:
                del self._offset_table[mq]
                continue
            self._persist_offset(mq)

    def commit(self, offsets=None, persist: bool = True) -> None:
        """Commit consume offsets. The argument picks one of three commit scopes:

        - ``commit()``                       -> every assigned queue, at its "consumed cursor"
        - ``commit({MessageQueue: offset})`` -> only the queues named by the dict, at the given offsets
        - ``commit([MessageQueue, ...])``     -> only these queues, at their consumed cursors

        Specifying offsets **only changes the commit position, not the pull cursor**: messages already in the buffer are still handed to the caller, and the next round's
        ``commit()`` overwrites the table back per the cursor (the order at the same place is exactly this: an explicit
        offset updates only the in-memory table, while a full commit re-reads each assigned
        queue's consumed cursor).

        Two deliberate choices of this implementation, both on this chain:
        1. a full commit itself writes only the in-memory table; shipping it to the broker would otherwise
           rely on the client instance's periodic offset persist (every ``persist_consumer_offset_interval``,
           5s), which this port's lite consumer does not attach, so ``persist=True`` (the default) sends it in place.
        2. this persist step is commonly done oneway, with exceptions only logged; here it is sent
           synchronously with a response, so bad offsets can be seen in the logs right away.
        """
        if offsets is None:
            # No argument: only iterate the currently held queues
            targets = {mq: self._consume_offset.get(mq, -1) for mq in self._assigned}
            scope = self._assigned
        elif isinstance(offsets, dict):
            if not offsets:
                logger.warning("MessageQueues is empty, Ignore this commit ")
                return
            targets = dict(offsets)
            scope = targets.keys()
        else:
            queues = list(offsets)
            if not queues:
                return
            targets = {mq: self._consume_offset.get(mq, -1) for mq in queues}
            scope = queues

        for mq, offset in targets.items():
            if offset == -1:
                logger.error("consumerOffset is -1 in messageQueue [%s].", mq)
                continue
            if mq not in self._assigned:
                # The "queue is held and not dropped" guard: queues not held by this instance
                # are never committed on its behalf, and are silently skipped (not even a log here).
                continue
            self._offset_table[mq] = offset

        if persist:
            self._persist_offset_table(scope)

    def offset_for_timestamp(self, mq: MessageQueue, timestamp: int) -> int:
        return self._mq_client.search_offset_by_timestamp(mq, timestamp)

    def assignment(self) -> List[MessageQueue]:
        return list(self._assigned)

    def fetch_message_queues(self, topic: str) -> List[MessageQueue]:
        return self._fetch_message_queues_by_full_name(self._with_namespace(topic))

    def _fetch_message_queues_by_full_name(self, full_topic: str) -> List[MessageQueue]:
        pass
        if self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        try:
            self._mq_client.update_topic_route_info_from_name_server(full_topic)
        except Exception as e:  # noqa: BLE001
            logger.debug("fetch queues: route refresh for %s failed: %s", full_topic, e)
        queues = self._mq_client.get_topic_subscribe_info(full_topic)
        if not queues:
            raise MQClientException("Can not find Message Queue for this topic, "
                                    + full_topic + " Namesrv return empty")
        return queues

    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        return self.fetch_message_queues(topic)

    def pause(self, message_queues) -> None:
        self._paused |= set(message_queues)

    def resume(self, message_queues) -> None:
        self._paused -= set(message_queues)

    # ---------------- Topic queue set change listeners ----------------
    def register_topic_message_queue_change_listener(
            self, topic: str, listener: TopicMessageQueueChangeListener) -> None:
        pass
        if not topic or listener is None:
            raise MQClientException("Topic or listener is null")
        ns = self._with_namespace(topic)
        with self._topic_listener_lock:
            if ns in self._topic_change_listeners:
                logger.warning("Topic %s had been registered, new listener will "
                               "overwrite the old one", ns)
            self._topic_change_listeners[ns] = listener
            started = self._started
        if started:
            try:
                self._message_queues_for_topic[ns] = set(
                    self._fetch_message_queues_by_full_name(ns))
            except Exception as e:  # noqa: BLE001
                logger.debug("register listener: fetch queues for %s failed: %s", ns, e)

    def fetch_topic_message_queues_and_compare(self) -> int:
        pass
        with self._topic_listener_lock:
            entries = list(self._topic_change_listeners.items())
        fired = 0
        for ns, listener in entries:
            try:
                new_queues = set(self._fetch_message_queues_by_full_name(ns))
                old_queues = self._message_queues_for_topic.get(ns)
                if self._is_set_equal(new_queues, old_queues):
                    continue
                self._message_queues_for_topic[ns] = new_queues
                listener.on_changed(ns, new_queues)
                fired += 1
            except Exception as e:  # noqa: BLE001
                logger.error("ScheduledTask fetchMessageQueuesAndCompare exception: %s", e)
        return fired

    @staticmethod
    def _is_set_equal(set1: Optional[Set[MessageQueue]],
                      set2: Optional[Set[MessageQueue]]) -> bool:
        if set1 is None and set2 is None:
            return True
        if set1 is None or set2 is None or len(set1) != len(set2):
            return False
        return all(mq in set1 for mq in set2)

    def _metadata_loop(self, first_delay_millis: float = 10000) -> None:
        deadline = time.monotonic() + first_delay_millis / 1000.0
        while self._running:
            time.sleep(0.2)
            if not self._running:
                return
            now = time.monotonic()
            if now < deadline:
                continue
            deadline = now + self.topic_metadata_check_interval_millis / 1000.0
            self.fetch_topic_message_queues_and_compare()


class SimpleMessageListener(MessageListenerConcurrently):
    """Convenience listener wrapper: turns the consume logic into a pure function."""

    def __init__(self, fn: Callable[[List[MessageExt]], ConsumeConcurrentlyStatus]):
        self.fn = fn

    def consume_message(self, msgs: List[MessageExt],
                        context: ConsumeConcurrentlyContext) -> ConsumeConcurrentlyStatus:
        return self.fn(msgs)


__all__ = [
    "DefaultMQPushConsumer", "DefaultMQPullConsumer", "MessageSelector",
    "MessageQueueListener", "TopicMessageQueueChangeListener",
    "AllocateMessageQueueStrategy",
    "AllocateMessageQueueAveragely", "AllocateMessageQueueAveragelyByCircle",
    "AllocateMessageQueueByConfig", "AllocateMessageQueueConsistentHash",
    "AllocateMessageQueueByMachineRoom", "AllocateMachineRoomNearby",
    "MachineRoomResolver", "ConsistentHashRouter", "MD5Hash", "HashFunction",
    "SimpleMessageListener",
    "PullResult", "PullStatus", "MessageListener", "MessageListenerConcurrently",
    "MessageListenerOrderly", "ConsumeConcurrentlyStatus", "ConsumeOrderlyStatus",
]

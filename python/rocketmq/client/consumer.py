# -*- coding: utf-8 -*-
"""消费者（对应 org.apache.rocketmq.client.consumer.* 的核心能力）。

提供：DefaultMQPushConsumer（注册监听器 + 拉取循环消费）、DefaultMQPullConsumer
（手动拉取与 offset 管理）、MessageSelector、AllocateMessageQueueStrategy 分配策略、
MessageQueueListener、消费进度管理、消息重投（sendMessageBack）等。
"""
from __future__ import annotations

import bisect
import hashlib
import json
import os
import queue
import threading
import time
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Set, Tuple

from ..common.message import Message, MessageExt, MessageQueue
from ..common.message_accessor import MessageAccessor
from ..common.message_const import MessageConst
from ..common.mix_all import MixAll
from ..common.subscription_data import ExpressionType, FilterAPI, SubscriptionData
from ..common.sysflag import MessageSysFlag, PullSysFlag
from ..logging import get_logger
from ..remoting.exception import RemotingException, RemotingTimeoutException
from ..remoting.protocol.codes import RequestCode
from ..remoting.protocol.heartbeat import (ConsumeFromWhere, ConsumeType,
                                           ConsumerData, HeartbeatData, MessageModel)
from ..remoting.protocol.namespace_util import NamespaceUtil
from ..remoting.protocol.body import (CMResult, ConsumeMessageDirectlyResult,
                                       ConsumeStatus, ConsumerRunningInfo,
                                       ProcessQueueInfo)
from ..remoting.protocol import extra_info as extra_info_util
from ..remoting.protocol.extra_info import split
from ..remoting.rpchook import RPCHook
from . import validators
from .consume_executor import ConsumeExecutor
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
from .top_addressing import DefaultTopAddressing
from .trace import AccessChannel
from .trace_dispatcher import AsyncTraceDispatcher, TraceDispatcherType
from .trace_hook import ConsumeMessageTraceHook

logger = get_logger()

# POP 消费失败的延迟梯度（秒），逐项对应 Java
# DefaultMQPushConsumerImpl.popDelayLevel。
POP_DELAY_LEVEL = (10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600,
                   1200, 1800, 3600, 7200)

# Java DefaultMQPushConsumerImpl.MIN/MAX_POP_INVISIBLE_TIME：超出范围一律回落到 60000
MIN_POP_INVISIBLE_TIME = 5000
MAX_POP_INVISIBLE_TIME = 300000

# Java ConsumeInitMode
CONSUME_INIT_MODE_MIN = 0
CONSUME_INIT_MODE_MAX = 1

# Java `Integer.MAX_VALUE`：顺序消费用尽判据的默认值（-1 在这条链路上读成它，
# 见 DefaultMQPushConsumerImpl#checkReconsumeTimes 的注释「default reconsume times」）。
_JAVA_INT_MAX = 0x7FFFFFFF

# Java ConsumeOrderlyStatus 的四个成员（声明顺序见其枚举：SUCCESS/ROLLBACK/COMMIT/挂起）。
# 顺序消费的 listener 只允许返回这四个之一：null 由 Java 兜成挂起，未知值在动态类型下
# 也得走同一条兜底，否则会被当成 SUCCESS 静默 ack。
_ORDERLY_STATUSES = (ConsumeOrderlyStatus.SUCCESS, ConsumeOrderlyStatus.ROLLBACK,
                     ConsumeOrderlyStatus.COMMIT,
                     ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT)

# Java ProcessQueue.PULL_MAX_IDLE_TIME（`rocketmq.client.pull.pullMaxIdleTime`，默认 120000ms）：
# 一个仍归本实例的队列如果超过这么久没发起过任何拉取/弹出，说明它的循环死了（或卡住了）。
# Java RebalanceImpl.updateProcessQueueTableInRebalance:442 就按这个判据把它撤掉重建。
PULL_MAX_IDLE_TIME = 120.0


def _mq_sort_key(mq: MessageQueue):
    """队列排序键，语义对齐 Java MessageQueue.compareTo：topic → brokerName → queueId。

    Java rebalance 会先把 mqAll/cidAll 排序再分配；顺序不一致会让不同实例算出
    不同的分配结果（同一队列被两个实例同时消费）。
    """
    return (mq.topic, mq.broker_name, mq.queue_id)


def _safe_hook_name(hook) -> str:
    try:
        return str(hook.hook_name())
    except Exception:  # noqa: BLE001
        return type(hook).__name__


def client_side_tag_filter(sub: Optional[SubscriptionData],
                           msgs: List[MessageExt]) -> List[MessageExt]:
    """客户端二次 tag 过滤（对应 Java PullAPIWrapper.processPullResult:113-122）。

    broker 侧是按 tag 的**哈希（codeSet）**过滤的，存在哈希碰撞误放；Java 因此让客户端
    再按字符串核一遍。守卫 `!tagsSet.isEmpty() && !isClassFilterMode` 意味着：
    订阅 `"*"`（SUB_ALL）时不过滤 —— 所以 `FilterAPI.build_subscription_data`
    对 SUB_ALL 必须保持 tags_set 为空（那里有详细注释）。
    """
    if not msgs or sub is None or not sub.tags_set or sub.class_filter_mode:
        return msgs
    return [m for m in msgs if m.get_tags() is not None and m.get_tags() in sub.tags_set]


def execute_filter_hooks(hook_list: List["FilterMessageHook"], context: FilterMessageContext) -> None:
    """依次执行过滤钩子，**异常一律吞掉**（Java PullAPIWrapper.executeHook:171-178 记 error）。

    与 send/consume 钩子不同：过滤钩子失败不能影响消费，只是该次过滤不生效。
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
    """投递前过滤 = 客户端二次 tag 过滤 + FilterMessageHook（拉取/POP/pull 三处共用）。

    钩子拿到的是**可变的** ``msg_list``；被摘掉的消息由调用方决定处置方式：
    拉取路径 = 静默跳过（位点照常推进，不 ack，Java 亦然）；
    POP 路径 = 必须立刻 ack，否则 invisibleTime 到期后会复活重投。
    """
    out = client_side_tag_filter(sub, list(msgs))
    if out and hook_list:
        context = FilterMessageContext(consumer_group, out, mq)
        context.unit_mode = unit_mode    # Java DefaultMQPushConsumerImpl:640
        execute_filter_hooks(hook_list, context)
        out = list(context.msg_list)
    return out


class MessageSelector:
    """消息选择器（对应 Java MessageSelector）。"""

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
    """队列变更监听器（对应 Java MessageQueueListener）。"""

    def message_queue_changed(self, topic: str, mq_all: Set[MessageQueue],
                              mq_divided: Set[MessageQueue]) -> None:
        raise NotImplementedError


class AllocateMessageQueueStrategy:
    """队列分配策略接口（对应 Java AllocateMessageQueueStrategy）。

    与 Java 的有意差异：Java 的
    ``AbstractAllocateMessageQueueStrategy#check`` 在非法入参时抛
    ``IllegalArgumentException``，这里一律返回空列表——rebalance 是后台周期任务，
    一条脏入参不该把消费者打挂。
    """

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        raise NotImplementedError

    def get_name(self) -> str:
        """对应 Java ``AllocateMessageQueueStrategy#getName``：算法名（AVG 等）。"""
        raise NotImplementedError


def _strategy_name(strategy: AllocateMessageQueueStrategy) -> str:
    """取策略名用于日志。

    Java 侧策略是接口实现、必有 ``getName()``；Python 允许业务方鸭子类型地传一个只有
    ``allocate`` 的自定义对象，所以缺 ``get_name`` 时退化成类名而不是抛 AttributeError。
    """
    getter = getattr(strategy, "get_name", None)
    if callable(getter):
        try:
            return str(getter())
        except Exception:  # noqa: BLE001 - 日志取值不该影响 rebalance
            pass
    return strategy.__class__.__name__


class AllocateMessageQueueAveragely(AllocateMessageQueueStrategy):
    """平均分配（对应 Java AllocateMessageQueueAveragely）。"""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        # 守卫顺序与 Java AbstractAllocateMessageQueueStrategy#check 一致：currentCID →
        # mqAll → cidAll。唯一偏差：Java 这三种情况都抛 IllegalArgumentException，
        # 这里只返回空列表（rebalance 是后台周期任务，脏入参不该把消费者打挂）；
        # currentCID 不在 cidAll 时保留 Java 那条 [BUG] info 日志。
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
    """环形平均分配（对应 Java AllocateMessageQueueAveragelyByCircle）。"""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        # 守卫与 Averagely 同款（Java AbstractAllocateMessageQueueStrategy#check），同样只返回空列表。
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
    """按显式配置分配（对应 Java AllocateMessageQueueByConfig）。

    与 Java 的有意差异：Java 直接 ``return this.messageQueueList``，未配置时是 ``null``；
    这里 __init__ 规整成空列表、allocate 返回列表副本。两边的 ``allocate`` 都**不**做
    check，所以空 group / 空 cid_all 也照样返回配置值。
    """

    def __init__(self, message_queue_list: Optional[List[MessageQueue]] = None):
        self.message_queue_list = list(message_queue_list) if message_queue_list else []

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        return list(self.message_queue_list)

    def get_name(self) -> str:
        return "CONFIG"


# ------------------------------------------------------ 一致性哈希环（Java common 包）
#
# 对应 org.apache.rocketmq.common.consistenthash.{HashFunction, Node, VirtualNode,
# ConsistentHashRouter}。只有 AllocateMessageQueueConsistentHash 用它，但整套照搬而不做
# "等价改写"：环上的落点由 MD5 取字节的方式、虚拟节点命名、tailMap 含端点这三处细节
# 共同决定，任何一处不同都会让全体队列换主，与 Java 客户端混跑时表现为重复/漏消费。


class HashFunction:
    """对应 Java `HashFunction#hash(String)`。自定义哈希用它注入策略。"""

    def hash(self, key: str) -> int:
        raise NotImplementedError


class MD5Hash(HashFunction):
    """对应 Java `ConsistentHashRouter.MD5Hash`：MD5 摘要的**前 4 字节**按大端拼成整数。

    ⚠ 只取前 4 字节（不是完整 128 bit），换成其它取法就与 Java 不是同一个环。
    """

    def hash(self, key: str) -> int:
        digest = hashlib.md5(key.encode("utf-8")).digest()
        value = 0
        for i in range(4):
            value = (value << 8) | digest[i]
        return value


class Node:
    """对应 Java `Node#getKey`：环上可寻址的东西，物理或虚拟都行。"""

    def get_key(self) -> str:
        raise NotImplementedError


class ClientNode(Node):
    """对应 Java `AllocateMessageQueueConsistentHash.ClientNode`：key 就是 clientId。"""

    def __init__(self, client_id: str):
        self.client_id = client_id

    def get_key(self) -> str:
        return self.client_id


class VirtualNode(Node):
    """对应 Java `VirtualNode`：key = 物理节点 key + "-" + 副本序号。"""

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
    """对应 Java `ConsistentHashRouter`：把节点哈希成环，路由到顺时针最近的物理节点。

    Java 用 `TreeMap<Long, VirtualNode>`；这里用「升序 key 列表 + dict」等价替代。
    `route_node` 要的是 `tailMap(hashVal).firstKey()`，而 TreeMap 的 tailMap **含端点**，
    所以等价的查找是 `bisect_left`（相等的 hash 归自己），环空或越过末尾时回绕到首节点。
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
        # 对应 Java `#addNode`：已有副本要接着编号，否则同一个物理节点的 v 个虚拟节点
        # 会全落在同一个 hash 上（Java 分两次 addNode 时靠 i + existingReplicas 区分）。
        if v_node_count < 0:
            raise ValueError("illegal virtual node counts :%d" % v_node_count)
        existing_replicas = self.get_existing_replicas(p_node)
        for i in range(v_node_count):
            v_node = VirtualNode(p_node, i + existing_replicas)
            key = self.hash_function.hash(v_node.get_key())
            if key not in self._ring:
                bisect.insort(self._keys, key)
            # Java 是 TreeMap.put：同 hash 时后来者覆盖，位置不变
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
            index = 0  # 越过环的末尾 → 回绕到第一个（Java 的 ring.firstKey()）
        return self._ring[self._keys[index]].get_physical_node()

    def get_existing_replicas(self, p_node: Node) -> int:
        return sum(1 for v_node in self._ring.values() if v_node.is_virtual_node_of(p_node))


def _java_message_queue_string(mq: MessageQueue) -> str:
    """对应 Java `MessageQueue#toString`（一致性哈希要哈希**它**，不是 repr）。

    ⚠ 必须逐字符等于 Java 的 `"MessageQueue [topic=.., brokerName=.., queueId=..]"`：
    Python 自己的 `__repr__` 是另一种写法，拿它去哈希会得到完全不同的环。
    """
    return "MessageQueue [topic=%s, brokerName=%s, queueId=%d]" % (
        mq.topic, mq.broker_name, mq.queue_id)


def _java_split(text: str, sep: str) -> List[str]:
    """对应 Java `String#split(String)`（limit=0）：**丢掉末尾的空段**。

    Python 的 `str.split` 保留尾空段，两边对 broker 名的切分结果因此不同，而
    `AllocateMessageQueueByMachineRoom` 恰好按 `length == 2` 判合法，差异会直接改变
    一条队列参不参与分配：

    | 输入 | Java | Python 原生 |
    |---|---|---|
    | `"room1@"` | `["room1"]`（1 段，剔除） | `["room1", ""]`（2 段，误收） |
    | `"room1@b@"` | `["room1", "b"]`（2 段，收） | `["room1", "b", ""]`（3 段，误剔） |
    | `"@"` | `[]` | `["", ""]` |
    | `""` / `"broker-a"` | 无分隔符命中时**整串原样返回**，哪怕是空串 | 同 |

    最后一行是 Java `Pattern#split` 里 "If no match was found, return this" 那个早返回，
    所以不能无条件裁尾（否则 `""` 会变成 `[]`）。以上取值用 JDK 17 实测核对过。
    """
    parts = text.split(sep)
    if len(parts) == 1:
        return parts  # 没命中分隔符：Java 原样返回整串
    while parts and parts[-1] == "":
        parts.pop()
    return parts


class AllocateMessageQueueConsistentHash(AllocateMessageQueueStrategy):
    """一致性哈希分配（对应 Java `AllocateMessageQueueConsistentHash`，`getName()` 为
    `CONSISTENT_HASH`）。

    与 AVG / AVG_BY_CIRCLE 的差别不是"分得均不均"，而是**稳定性**：队列数或消费者数变化时，
    只有落在新增/移除节点之间弧段上的队列会换主（Java 单测
    `AllocateMessageQueueConsitentHashTest` 正是断言这一点），AVG 则会把所有人的分界整体挪掉。

    守卫口径同其它策略：Java 的 `check` 抛 IllegalArgumentException，这里返回空列表。
    唯一保留抛的是构造函数里的 `virtualNodeCnt < 0`（Java 也在构造时抛，且不属于 rebalance
    后台路径）。
    """

    def __init__(self, virtual_node_cnt: int = 10,
                 custom_hash_function: Optional[HashFunction] = None):
        # 对应 Java 三个构造函数的链：默认 10 个虚拟节点、默认 MD5Hash
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
            node = router.route_node(_java_message_queue_string(mq))
            if node is not None and node.get_key() == current_cid:
                result.append(mq)
        return result

    def get_name(self) -> str:
        return "CONSISTENT_HASH"


class AllocateMessageQueueByMachineRoom(AllocateMessageQueueStrategy):
    """按机房分配（对应 Java `AllocateMessageQueueByMachineRoom`，`getName()` 为
    `MACHINE_ROOM`，注释里的场景是"支付宝逻辑机房"）。

    约定 broker 名写成 `<机房>@<brokerName>`，只有前缀落在 `consumeridcs` 里的队列参与
    分配，然后在这些队列内部再做一次"平均分配"（分片算法与 AVG 逐行相同，但 rem 的归属
    判据是 `rem > currentIndex`，即余数队列发给前 rem 个消费者）。

    ⚠ broker 名的切分走 [`_java_split`]，不是 Python 原生 `split`：Java 会丢掉末尾空段，
    `"room1@"` 在 Java 是 1 段（不参与分配）、`"room1@b@"` 是 2 段（参与）。

    ⚠ Java 的 `consumeridcs` 字段没有默认值，没 set 就 `contains` → NPE；这里默认空集合，
    表现为"一条都不分"，与本端口一贯的"守卫返回空结果"口径一致。
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
            temp = _java_split(mq.broker_name, "@")
            if len(temp) == 2 and temp[0] in self.consumeridcs:
                premq_all.append(mq)
        mod = len(premq_all) // len(cid_all)  # Java 是 int 除法
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
    """对应 Java `AllocateMachineRoomNearby.MachineRoomResolver`：告诉策略"谁在哪个机房"。

    Java 注释明确写了两个方法**都不能返回 null**（否则该机房视为空，队列会被撤走）。
    """

    def broker_deploy_in(self, message_queue: MessageQueue) -> str:
        raise NotImplementedError

    def consumer_deploy_in(self, client_id: str) -> str:
        raise NotImplementedError


class AllocateMachineRoomNearby(AllocateMessageQueueStrategy):
    """机房就近分配（对应 Java `AllocateMachineRoomNearby`）。

    代理模式：先按机房把队列和消费者各自分组，
    1. 本消费者所在机房的队列只分给**同机房**的消费者（用内层策略）；
    2. 那些**机房里没有任何活消费者**的队列，交给所有消费者按内层策略瓜分——
       否则它们就没人消费了。

    `getName()` 是 `"MACHINE_ROOM_NEARBY" + "-" + 内层策略名`（Java 同），因为日志里
    必须能看出实际用的是哪个分配算法。

    两个构造参数缺失时 Java 抛 NullPointerException；resolver 给出空机房时 Java 抛
    IllegalArgumentException —— 这里都**照抛**：静默返回空列表等于把整个 topic 的队列
    撤走，而 rebalance 抓住异常时反而会保住现有分配，与 Java 行为一致。
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

        # 按机房分组。Java 用 TreeMap ⇒ 机房名**字典序**遍历，这里同样排序，
        # 否则同名机房的处理顺序会随插入顺序变（结果集是并集，顺序也会进日志/断言）。
        mr_2_mq: Dict[str, List[MessageQueue]] = {}
        for mq in mq_all:
            room = self.machine_room_resolver.broker_deploy_in(mq)
            if room:
                mr_2_mq.setdefault(room, []).append(mq)
            else:
                raise ValueError("Machine room is null for mq %s"
                                 % _java_message_queue_string(mq))
        mr_2_c: Dict[str, List[str]] = {}
        for cid in cid_all:
            room = self.machine_room_resolver.consumer_deploy_in(cid)
            if room:
                mr_2_c.setdefault(room, []).append(cid)
            else:
                raise ValueError("Machine room is null for consumer id %s" % cid)

        allocate_results: List[MessageQueue] = []
        # 1. 本消费者所在机房的队列：只在同机房消费者之间分
        current_machine_room = self.machine_room_resolver.consumer_deploy_in(current_cid)
        mq_in_this_machine_room = mr_2_mq.pop(current_machine_room, None)
        consumer_in_this_machine_room = mr_2_c.get(current_machine_room)
        if mq_in_this_machine_room:
            allocate_results += self.allocate_message_queue_strategy.allocate(
                consumer_group, current_cid, mq_in_this_machine_room,
                consumer_in_this_machine_room)
        # 2. 没有活消费者的机房：队列不能没人消费，交给全部消费者
        for room in sorted(mr_2_mq):
            if room not in mr_2_c:
                allocate_results += self.allocate_message_queue_strategy.allocate(
                    consumer_group, current_cid, mr_2_mq[room], cid_all)
        return allocate_results

    def get_name(self) -> str:
        return "MACHINE_ROOM_NEARBY-%s" % _strategy_name(
            self.allocate_message_queue_strategy)


class PopProcessQueue:
    """POP 模式的队列状态（对应 org.apache.rocketmq.client.impl.consumer.PopProcessQueue）。

    与 pull 模式的 ProcessQueue 不同，POP **没有"已拉未消费"缓冲**：消息一弹出就交给
    消费线程，确认靠 ack。这里只跟踪两件事：

    - ``wait_ack_counter``：已弹出但还没 ack / 还没延长不可见时间的条数，用于流控；
    - ``dropped``：队列是否已被 rebalance 撤走（撤走后本批消息不再消费、也不 ack，
      交给 invisibleTime 到期后 broker 自动复活重投）。
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
        """Java 传的是负数（decFoundMsg(-msgs.size())），这里按"减多少"理解。"""
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
    """推模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer）。"""

    # ---------------- POP 模式（5.x 轻量消费）配置 ----------------
    #
    # Java 的 push-consumer POP 走 **broker 侧分配**：QUERY_ASSIGNMENT(400) 拿
    # MessageQueueAssignment(mode=POP)，客户端不做 rebalance。本项目**刻意不实现这条路径**，
    # 而是复用已有的**客户端 rebalance**（见 _rebalance_pull_threads）：队列由本地按分配策略
    # 算出，然后每队列独立 POP。语义等价（都是"每队列一个 POP 循环 + ack 确认"），
    # 差别只在于"谁决定分哪些队列"——这是有意差异，改动前请先读本段注释。

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING, **kwargs):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        # TLS（Java 全局系统属性 tls.enable 的等价物；None = 交给 env ROCKETMQ_TLS_ENABLE）
        self.tls_enable: Optional[bool] = kwargs.pop("tls_enable", None)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id: Optional[str] = None
        # ---- unit / stream 配置（对应 Java ClientConfig 的同名字段）----
        # unitName 参与 clientId 的 `@<unitName>` 后缀与动态取址 URL；unitMode 随
        # ConsumerData 心跳、消息回投请求头和过滤钩子上报给 broker；
        # enableStreamRequestType 既给 clientId 加 `@STREAM` 后缀（Java 的注释写明是
        # 为了"prevent unexpected reuses of MQClientInstance"），也给每个请求加
        # `ReqT=0` 扩展字段。Java 的推送消费者与 producer 一样默认关闭。
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = False
        self.message_model = message_model
        self.consume_from_where = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        self.consume_timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time() - 30 * 60))
        # ---- 消费线程池（对齐 Java DefaultMQPushConsumer / ThreadPoolExecutor）----
        # Java 5.x 的默认值是 min=20（:162）**且** max=20（:169）—— 两侧同为 20
        # （4.x 时代才是 min=20/max=64，本端口早年照抄的是 4.x 那一组）。
        # 本实现的**拉取**路径是"每队列一个拉取线程"（不是共享线程池），只有 **POP**
        # 路径才真正建线程池（Java 的 ConsumeMessagePopConcurrentlyService 也是线程池）。
        # 两个值同时是"声明值"：consume_thread_max 既是 POP 线程池的 max，也是
        # update_core_pool_size 的上界（Java 守卫 n < getConsumeThreadMax()）。
        # 因为 Java 用**无界**队列（真实并发度 == core），默认配置下 core 只能往**下**
        # 调（n < 20）—— 这是 Java 的既有行为，不是本端口的额外限制。
        self.consume_thread_min = 20
        self.consume_thread_max = 20
        # 自动弹性阈值（Java DefaultMQPushConsumer.adjustThreadPoolNumsThreshold，默认 100000）。
        # ⚠ 自动 inc/dec 在 Java 5.5.1 是**空实现**（AbstractConsumeMessageService:70-75），
        # 见 adjust_thread_pool()：保留计算与配置是为了可观测，不要"顺手修好"。
        self.adjust_thread_pool_nums_threshold = 100000
        # 声明式 core pool size（Java setCorePoolSize 的等价物），默认 = consume_thread_min
        self._core_pool_size = self.consume_thread_min
        # 是否自建执行器（Java AbstractConsumeMessageService.ownsConsumeExecutor）。
        # 本实现不支持外部注入执行器，故恒为 True —— update_core_pool_size 的这道守卫恒满足。
        self._owns_consume_executor = True
        # ProcessQueue.msgAccCnt（按队列 key）：最近一次拉取算出的"积压条数"
        # = 消息上的 MAX_OFFSET 属性 - 该消息的 queueOffset（>0 才更新）
        self._msg_acc_cnt_table: Dict[str, int] = {}
        self.consume_concurrently_max_span = 2000
        self.pull_threshold_for_queue = 1000
        self.pull_threshold_size_for_queue = 100
        self.pull_threshold_for_topic = -1
        self.pull_threshold_size_for_topic = -1
        self.pull_interval = 0
        self.pull_timeout_millis = 30000
        # 长轮询的 suspend 时长（对应 Java brokerSuspendMaxTimeMillis 默认 20000）。
        # 注意：这个属性必须在这里初始化——pull 循环会读它，缺了就会每轮抛
        # AttributeError 并被 _consume_loop 的兜底 except 吞掉，表现为"消费端一条都收不到"。
        self.pull_suspend_timeout_millis = 20000
        self.consume_message_batch_max_size = 1
        self.pull_batch_size = 32
        self.pull_batch_size_in_bytes = 256 * 1024
        self.max_reconsume_times = -1
        self.suspend_current_queue_time_millis = 1000
        self.consume_timeout = 15
        self.client_rebalance = True
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self.allocate_strategy: AllocateMessageQueueStrategy = AllocateMessageQueueAveragely()
        # 订阅表 topic -> SubscriptionData
        self.subscription_data: Dict[str, SubscriptionData] = {}
        self.message_listener: Optional[MessageListener] = None
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._pulling = False
        self._lock = threading.RLock()
        self._consume_threads: List[threading.Thread] = []
        self._msg_queue_inflight: Dict[str, int] = {}
        self._offset_table: Dict[str, int] = {}
        self._stop = threading.Event()
        # ---- 对齐 Java 的消费进度 / 缓冲 / 锁状态 ----
        # _offset_table 是"拉取游标"（nextBeginOffset）；_consume_offsets 是
        # "已消费位点"（Java ProcessQueue.removeMessage 后的 commitOffset），
        # 周期持久化到 broker 的是后者。_pending 是已拉未消费缓冲（Java ProcessQueue）。
        self._pending: Dict[str, Deque[MessageExt]] = {}
        self._mq_map: Dict[str, MessageQueue] = {}
        self._consume_offsets: Dict[str, int] = {}
        # 顺序消费：broker LOCK_BATCH_MQ 确认锁定成功的队列 key 集（Java ConsumeMessageOrderlyService）
        self._lock_ok: Set[str] = set()
        self._flow_control_triggered = 0
        self._dispatch_thread: Optional[threading.Thread] = None
        self._persist_thread: Optional[threading.Thread] = None
        self._lock_thread: Optional[threading.Thread] = None
        self._rebalance_thread: Optional[threading.Thread] = None
        self._queue_threads: Dict[str, threading.Thread] = {}
        # 每队列最近一次「发起拉取/弹出」的时刻（Java ProcessQueue.lastPullTimestamp /
        # PopProcessQueue.lastPopTimestamp）。rebalance 用它判 pull 是否停摆（PULL_MAX_IDLE_TIME）。
        self._last_pull_table: Dict[str, float] = {}
        # ---- 真实 rebalance（对齐 Java RebalanceImpl）----
        # _assigned 是"当前分给本实例的队列集"（Java ProcessQueueTable 的键集），
        # 由 _do_rebalance() 按 LOCK/分配策略计算；_rebalance_now 用于
        # NOTIFY_CONSUMER_IDS_CHANGED(40) 触发的即时 rebalance（Java rebalanceImmediately）。
        self._assigned: List[MessageQueue] = []
        self._rebalance_now = threading.Event()
        # start() 时刻：rebalance 循环在启动阶段用更短的间隔重试（见 _rebalance_loop）
        self._start_time = time.time()
        # ---- 消费者心跳（对齐 Java heartbeatBrokerInterval 默认 30s）----
        # 必需：broker 的 ConsumerManager 只有收到心跳才记录消费组里的 clientId，
        # rebalance 的 GET_CONSUMER_LIST_BY_GROUP 才有返回。
        self.heartbeat_enabled = True
        self.heartbeat_interval_millis = 30000
        # 路由刷新周期（对应 Java ClientConfig.pollNameServerInterval 默认 30000ms）。
        # 只在 start() 建 MQClientInstance 时透传一次，之后修改不生效
        # （Java 的 scheduledExecutorService 按启动时的周期排定）。
        self.poll_name_server_interval = 30000
        # 已消费位点落盘周期（对应 Java ClientConfig.persistConsumerOffsetInterval
        # 默认 5000ms；MQClientInstance.startScheduledTask:417-423 的
        # ``scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10, <本值>)``）。
        # 同样只在 start() 时被 ``_offset_persist_loop`` 读一次。
        self.persist_consumer_offset_interval = 5000
        self._heartbeat_count = 0
        self._heartbeat_thread: Optional[threading.Thread] = None
        # ---- POP 模式（5.x 轻量消费）----
        # 关掉时完全走原来的 pull 长轮询路径，行为与改动前一致。
        self.pop_mode = False
        # 弹出后对其它实例不可见的时长（Java popInvisibleTime 默认 60000）
        self.pop_invisible_time = 60000
        # 单次 POP 的最大条数（Java popBatchNums 默认 32；broker 侧 >32 会拒）
        self.pop_batch_nums = 32
        # 本队列"已弹未 ack"计数器上限，超过就暂停 POP（Java popThresholdForQueue 默认 96）
        self.pop_threshold_for_queue = 96
        # 消费失败时延长不可见时间的梯度（秒）
        self.pop_delay_level = list(POP_DELAY_LEVEL)
        # POP 长轮询挂起时长。0 = 短轮询（broker 立即返回或 NO_NEW_MSG）。
        # ⚠ 非 0 时请求超时必须 > 它，否则客户端先超时。
        self.pop_poll_time_millis = 15000
        self.pop_timeout_millis = 25000
        # 队列 key -> PopProcessQueue（已弹未 ack 计数 + 是否已被 rebalance 撤销）
        self._pop_queues: Dict[str, PopProcessQueue] = {}
        self._pop_executor: Optional[ConsumeExecutor] = None
        # ---- 消息轨迹（对应 Java ClientConfig.enableTrace / traceTopic）----
        # 开启后 start() 注册 ConsumeMessageTraceHook，落 SubBefore/SubAfter 两段轨迹
        self.enable_trace = False
        self.trace_topic: Optional[str] = None      # None → 用 RMQ_SYS_TRACE_TOPIC
        # 消费统计（Java ConsumerStatsManager），start() 时绑定到实例的 manager
        self._stats_manager: Optional[ConsumerStatsManager] = None
        self.trace_msg_batch_num = 10
        self.consume_message_hook_list: List[ConsumeMessageHook] = []
        # 投递前过滤钩子（Java DefaultMQPushConsumerImpl.filterMessageHookList）
        self.filter_message_hook_list: List[FilterMessageHook] = []
        self.trace_dispatcher = None

    # ---------------- 配置 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        """对应 Java `ClientConfig#setUnitName`：影响 clientId 后缀与动态取址 URL。"""
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        """对应 Java `ClientConfig#setUnitMode`：随心跳/请求头透传给 broker。"""
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        """对应 Java `ClientConfig#setEnableStreamRequestType`。"""
        self.enable_stream_request_type = bool(enable)

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_consume_from_where(self, where: str) -> None:
        self.consume_from_where = where

    def set_consume_timestamp(self, timestamp: str) -> None:
        """设置 CONSUME_FROM_TIMESTAMP 的起点时间（对应 Java setConsumeTimestamp）。

        格式 ``yyyyMMddHHmmss``（Java UtilAll.YYYYMMDDHHMMSS），默认 30 分钟前。
        """
        self.consume_timestamp = timestamp

    def _consume_timestamp_millis(self) -> int:
        # 对应 Java UtilAll.parseDate(ts, YYYYMMDDHHMMSS)：解析不了必须硬失败——静默回落到
        # 「现在 - 30 分钟」会让起点错位无人察觉（Java 在 checkConfig :1058 就抛）。
        try:
            if len(str(self.consume_timestamp)) != 14:
                raise ValueError(self.consume_timestamp)
            return int(time.mktime(time.strptime(self.consume_timestamp, "%Y%m%d%H%M%S")) * 1000)
        except (ValueError, TypeError, OverflowError):
            raise MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received %s"
                % self.consume_timestamp)

    def set_consume_thread_nums(self, n: int) -> None:
        """便捷方法：min 与 max 一起设（Java 4.x ``setConsumeThreadNums`` 的语义）。

        Java 5.x 的 ``DefaultMQPushConsumer`` 已拆成 ``setConsumeThreadMin/Max``，
        本方法保留是为了兼容既有调用点。
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
        """Java ``DefaultMQPushConsumer.setAdjustThreadPoolNumsThreshold``。"""
        self.adjust_thread_pool_nums_threshold = int(value)

    def get_adjust_thread_pool_nums_threshold(self) -> int:
        return self.adjust_thread_pool_nums_threshold

    # ---------------- 线程弹性（对应 Java AbstractConsumeMessageService）----------------

    def update_core_pool_size(self, core_pool_size: int) -> bool:
        """运行时调整消费并发度（对应 Java `updateCorePoolSize` → `setCorePoolSize`）。

        Java 的守卫逐条照抄（``AbstractConsumeMessageService:63-71``）::

            ownsConsumeExecutor && corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE          # 32767
                && corePoolSize < consumeThreadMax

        任一条不满足就**静默忽略**（Java 也是静默 return，不抛异常）。返回值只用于
        单测断言"是否真的生效"，Java 侧无返回值。
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
        """Java ``getCorePoolSize``：非自建执行器时返回 -1。"""
        if not self._owns_consume_executor:
            return -1
        if self._pop_executor is not None:
            return self._pop_executor.get_core_pool_size()
        return self._core_pool_size

    def _apply_core_pool_size(self) -> None:
        """把声明的 core size 落到真实执行器上（没有执行器时只记声明值）。"""
        if self._pop_executor is not None and self._owns_consume_executor:
            self._pop_executor.set_core_pool_size(self._core_pool_size)

    def compute_accumulation_total(self) -> int:
        """Java ``DefaultMQPushConsumerImpl.computeAccumulationTotal``。

        = 所有 ProcessQueue 的 ``msgAccCnt`` 之和。本实现按"每队列"记录最近一次
        拉取算出的积压条数（见 ``_update_msg_acc_cnt``）。
        """
        with self._lock:
            return sum(self._msg_acc_cnt_table.values())

    def adjust_thread_pool(self) -> None:
        """Java ``DefaultMQPushConsumerImpl.adjustThreadPool``（每分钟调度一次）。

        ⚠ **在 Java 5.5.1 这是 no-op，我们照抄 no-op**：它调用的
        ``consumeMessageService.incCorePoolSize()/decCorePoolSize()`` 在
        ``AbstractConsumeMessageService:70-75`` 是**空方法体**。这里保留阈值比较
        与日志，仅为让 ``msgAccCnt`` / 阈值配置可观测、可断言；**不要"修好"它** ——
        真正生效的是显式的 ``update_core_pool_size()``。
        """
        acc_total = self.compute_accumulation_total()
        threshold = self.adjust_thread_pool_nums_threshold
        inc_threshold = threshold * 1.0
        dec_threshold = threshold * 0.8
        if acc_total >= inc_threshold:
            # Java: consumeMessageService.incCorePoolSize() —— 空实现
            logger.debug("adjustThreadPool: acc=%d >= incThreshold=%d (inc is a no-op upstream)",
                         acc_total, int(inc_threshold))
        if acc_total < dec_threshold:
            # Java: consumeMessageService.decCorePoolSize() —— 空实现
            logger.debug("adjustThreadPool: acc=%d < decThreshold=%d (dec is a no-op upstream)",
                         acc_total, int(dec_threshold))

    def set_pull_suspend_timeout_millis(self, millis: int) -> None:
        self.pull_suspend_timeout_millis = int(millis)

    def set_message_listener(self, listener) -> None:
        self.message_listener = listener

    def set_allocate_message_queue_strategy(self, strategy: AllocateMessageQueueStrategy) -> None:
        self.allocate_strategy = strategy

    # ---------------- 消息轨迹（对应 Java DefaultMQPushConsumer 的 enableMsgTrace）----------------
    def set_enable_msg_trace(self, enable: bool) -> None:
        """开关消费侧消息轨迹（Java 构造函数参数 ``enableMsgTrace`` → enableTrace）。"""
        self.enable_trace = enable

    def set_enable_trace(self, enable: bool) -> None:
        self.enable_trace = enable

    def is_enable_trace(self) -> bool:
        return self.enable_trace

    def set_customized_trace_topic(self, trace_topic: Optional[str]) -> None:
        """自定义轨迹 topic（Java ``customizedTraceTopic``）；空则用系统默认。"""
        self.trace_topic = trace_topic

    def set_trace_topic(self, trace_topic: Optional[str]) -> None:
        self.trace_topic = trace_topic

    def set_trace_msg_batch_num(self, n: int) -> None:
        self.trace_msg_batch_num = n

    def register_consume_message_hook(self, hook: ConsumeMessageHook) -> None:
        """注册消费钩子（对应 Java registerConsumeMessageHook）。"""
        if hook is not None:
            self.consume_message_hook_list.append(hook)

    def has_consume_message_hook(self) -> bool:
        return len(self.consume_message_hook_list) > 0

    # ---------------- 投递前过滤钩子（对应 Java FilterMessageHook）----------------
    def register_filter_message_hook(self, hook: FilterMessageHook) -> None:
        """注册投递前过滤钩子（Java DefaultMQPushConsumerImpl.registerFilterMessageHook:152）。"""
        if hook is not None:
            self.filter_message_hook_list.append(hook)

    def has_filter_message_hook(self) -> bool:
        return len(self.filter_message_hook_list) > 0

    def execute_filter_message_hook(self, context: FilterMessageContext) -> None:
        """钩子异常一律吞掉并记 error（Java PullAPIWrapper.executeHook:171-178）。"""
        execute_filter_hooks(self.filter_message_hook_list, context)

    def _filter_messages_for_delivery(self, mq: MessageQueue, sub: Optional[SubscriptionData],
                                      msgs: List[MessageExt]) -> List[MessageExt]:
        """投递前过滤（见模块级 filter_messages_for_delivery 的说明）。"""
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
        """构造消费钩子上下文。初始化值与 Java 一致：success=False、props 为空字典。"""
        context = ConsumeMessageContext(self.consumer_group, msgs, mq)
        context.success = False
        context.props = {}
        context.access_channel = AccessChannel.LOCAL
        return context

    def _consume_return_type(self, status, has_exception: bool, consume_rt_ms: float,
                             failed: bool, succeeded: bool) -> ConsumeReturnType:
        """对应 Java 的 returnType 判定（决定轨迹的 contextCode）。

        Java 判定顺序：status == null → EXCEPTION/RETURNNULL；RT >= consumeTimeout
        （分钟）→ TIME_OUT；RECONSUME_LATER → FAILED；CONSUME_SUCCESS → SUCCESS。
        顺序消费把「挂起」等价于 FAILED、成功等价于 SUCCESS，由调用方用
        failed/succeeded 两个标志传入。
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
        """消费耗时记数（Java ConsumeRequest.run 里那条**无条件**的 ``incConsumeRT``：并发
        ``:414-415``、顺序 ``:514-515``）。

        与 ok/failed 计数分开：RT 直方图每次消费都记，而 TPS 只在结果落在某个分支时才记
        （顺序消费的 ``COMMIT``/``ROLLBACK`` 分支一个 TPS 都不加，但 RT 照记）。
        """
        if self._stats_manager is None:
            return
        self._stats_manager.inc_consume_rt(self.consumer_group, topic,
                                           int(time.time() * 1000 - begin_ms))

    def _record_consume_stats(self, topic: str, msg_count: int, begin_ms: float,
                              failed: bool, ack_count: Optional[int] = None) -> None:
        """消费侧 TPS 记数（Java ``processConsumeResult`` 的 ok/failed 分支）。

        ``ack_count`` 对应 Java ``processConsumeResult:217-220`` 的 ``ok = ackIndex + 1``：
        部分 ack 时前缀算 OK、尾巴算 FAILED。不传则按整批算（顺序/POP 路径的旧口径）。
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
        """把 returnType/status/success 写回上下文并触发 after 钩子（对齐 Java）。

        ``status`` 是决定 returnType 的那一个；``hook_status`` 是写进上下文的那个 ——
        Java 里这两步用的**不是同一个值**：returnType 在 status 归一化（null →
        RECONSUME_LATER / 挂起）**之前**算好（并发 :381-393、顺序 :483-496），而
        context.setStatus 拿的是归一化**之后**的值（并发 :399-410、顺序 :502-507）。
        不传 ``hook_status`` 时两者相同。
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
        """对应 Java ``MQConsumerInner#subscriptions()``：本消费者当前的订阅集合。

        给 ``MQClientInstance#checkClientInBroker`` 用（它按实例遍历，不看具体消费者类型）。
        """
        return list(self.subscription_data.values())

    # ---------------- 订阅 ----------------
    def subscribe(self, topic: str, sub_expression: str = "*") -> None:
        """订阅 topic（对应 Java ``DefaultMQPushConsumerImpl#subscribe:1265-1275``）。

        与 Java 一致，``start()`` 之后**仍可**调用：订阅表是活的（心跳与重平衡每轮都重读它），
        put 进去之后立即发一次心跳（见 ``_notify_subscription_changed``）。
        """
        topic = self._with_namespace(topic)
        sub = FilterAPI.build_subscription_data(topic, sub_expression)
        with self._lock:
            self.subscription_data[topic] = sub
        self._notify_subscription_changed(topic)

    def subscribe_with_selector(self, topic: str, selector: MessageSelector) -> None:
        """对应 Java ``DefaultMQPushConsumerImpl#subscribe(topic, MessageSelector):1277-1287``。

        同 ``subscribe``：start() 之后可调用，并立即发一次心跳。
        """
        topic = self._with_namespace(topic)
        # 对齐 Java FilterAPI.build(topic, subString, type)：
        #   TAG（或 type 为空）→ 走 buildSubscriptionData（填 tagsSet + codeSet）
        #   其它（SQL92 / CLASS_FILTER）→ 只设 topic/subString/expressionType，两个集合留空
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
        """取消订阅（对应 Java ``DefaultMQPushConsumerImpl#unsubscribe:1317-1319``）。

        Java 这里**只删表项、不发心跳**（后一轮心跳自然带出新订阅集），所以本方法
        刻意不调 ``_notify_subscription_changed``，只有 ``subscribe`` 会立即推心跳。
        """
        with self._lock:
            self.subscription_data.pop(self._with_namespace(topic), None)

    def _notify_subscription_changed(self, topic: str) -> None:
        """订阅表新增/更新后的立即动作，对齐 Java ``subscribe`` 里的第二句。

        Java ``DefaultMQPushConsumerImpl:1265-1287`` 三处 subscribe 都是：
        ``subscriptionInner.put(...)`` 之后 ``if (this.mQClientFactory != null)
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();`` —— 同步推一轮心跳。
        为什么必须立即推：broker 的 ``ConsumerManager`` 只在收到心跳时才把 topic→group 记进
        **它自己那张** topicGroupTable（ClientManageProcessor → registerConsumer），而
        ``QUERY_TOPIC_CONSUME_BY_WHO(300)`` 读的正是这张表；晚一轮就是默认 30s 的空窗。

        另外把新 topic 登记为「在用」，让后台路由刷新任务覆盖到它 —— 对应 Java
        ``MQClientInstance:438-454`` 直接遍历消费者的**当前**订阅表收集要刷的 topic。
        未启动时没有 client（Java 的 ``mQClientFactory == null``），只落订阅表。
        """
        if not self._started or self._mq_client is None:
            return
        self._mq_client.register_topic_in_use(topic)
        try:
            self._send_heartbeat_to_all_broker()
        except Exception as e:  # noqa: BLE001
            logger.debug("immediate heartbeat after subscribe(%s) failed: %s", topic, e)

    def _with_namespace(self, topic: str) -> str:
        """topic 拼命名空间前缀（对应 Java DefaultMQPushConsumer.subscribe(withNamespace(topic))）。"""
        if not self.namespace:
            return topic
        return NamespaceUtil.wrap_namespace(self.namespace, topic)

    # ---------------- 配置数值闸门 ----------------
    def _check_config_ranges(self) -> None:
        """对应 Java ``DefaultMQPushConsumerImpl#checkConfig`` 的数值段（:1099-1209）。

        逐条照抄 Java 的**顺序、区间和文案**（Java 每条都拼
        ``FAQUrl.suggestTodo(CLIENT_PARAMETER_CHECK_URL)``，本仓库按约定不带后缀）。
        比较一律是 ``< lo || > hi`` 严格不等 —— ``popBatchNums`` 在 Java 写的是 ``<= 0``，
        对整数等价于 ``< 1``，这里跟随 Java 的字面写法。

        为什么要在 ``start()`` 拦：这些值直接决定缓冲水位与线程池规模。写成 0 或
        ``2**31-1`` 时**没有一道闸门**的话，要么每轮拉取都被 ``threshold <= 0`` 判成
        积压而永久停拉（消费端静默收不到消息），要么整数溢出把 drop/流控判断整个绕开。
        到 broker 侧再报错已经晚了几拍，且错误信息只到日志里。

        注意：``pullThresholdForTopic`` / ``pullThresholdSizeForTopic`` 的 ``-1`` 是
        Java 的"未设置，用 queue 级阈值"哨兵，**不**在区间内也**不**报错。
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

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            # 消费组也拼命名空间（对齐 Java DefaultMQPushConsumer.start:763
            # setConsumerGroup(withNamespace(consumerGroup))）。必须在算重试主题之前：
            # 重试主题 = %RETRY% + 带前缀的组名（Java MixAll.getRetryTopic(wrappedGroup)）。
            if self.namespace:
                self.consumer_group = NamespaceUtil.wrap_namespace(self.namespace, self.consumer_group)
            # 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1026）：先 Validators.checkGroup
            # （blank / 120 长度 / 字符表），再挡 DEFAULT_CONSUMER —— 共用默认组会让
            # broker 侧的订阅关系判定把两组混在一起，回投与重平衡都错乱。
            # Java 把 checkConfig 排在 copySubscription 之前，所以这里也领先于订阅/地址检查。
            validators.check_group(self.consumer_group)
            if self.consumer_group == MixAll.DEFAULT_CONSUMER_GROUP:
                raise MQClientException(
                    "consumerGroup can not equal %s, please specify another one."
                    % MixAll.DEFAULT_CONSUMER_GROUP)
            if not self.name_server_addrs and not DefaultTopAddressing.is_configured():
                # 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
                raise MQClientException("name server address is not set")
            if not self.subscription_data:
                raise MQClientException("subscription is not set, call subscribe() first")
            if self.message_listener is None:
                raise MQClientException("message listener is not set")
            # 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1058）：启动即无条件校验起点时间，
            # 而不是等到 rebalance 里抛错、被 compute_pull_from_where 的兜底吞掉。
            self._consume_timestamp_millis()
            # 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1067）：策略为 None 直接拒绝启动。
            if self.allocate_strategy is None:
                raise MQClientException("allocateMessageQueueStrategy is null")
            # 对应 Java DefaultMQPushConsumerImpl.checkConfig 的数值段（:1099-1209）。
            # 排在所有 null 检查之后、任何网络动作之前：坏配置必须在建 MQClientInstance
            # 之前失败，否则起了后台线程再抛错就泄漏线程了。
            self._check_config_ranges()
            # 对应 Java `DefaultMQPushConsumerImpl#start`:934-936：只有 CLUSTERING 才
            # `changeInstanceNameToPID`（BROADCASTING 保持 "DEFAULT"，Java 的
            # MQClientManager 因此让同进程的广播消费者复用同一份实例），再由
            # `ClientConfig#buildMQClientId` 拼
            # `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`。
            if self.message_model == MessageModel.CLUSTERING:
                self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
            if self.client_id is None:
                self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                      self.enable_stream_request_type)
            self._mq_client = MQClientInstance(self.client_id, self.name_server_addrs,
                                               tls_enable=self.tls_enable,
                                               enable_stream_request_type=self.enable_stream_request_type,
                                               unit_name=self.unit_name,
                                               poll_name_server_interval=self.poll_name_server_interval)
            if self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            self._mq_client.start()
            # 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本消费者，
            # 让 consumerRunningInfo 等处能看到（Java 由共享的 ClientConfig 天然同步）。
            if not self.name_server_addrs and self._mq_client.name_server_addrs:
                self.name_server_addrs = list(self._mq_client.name_server_addrs)
            # 消费统计（Java MQClientFactory.getConsumerStatsManager，实例级共享）
            self._stats_manager = self._mq_client.consumer_stats_manager
            self._mq_client.register_consumer(self.consumer_group, self)
            self._started = True
            self._start_time = time.time()
            self._stop.clear()
            # 集群模式自动订阅重试 topic（对齐 Java copySubscription →
            # retryTopic = MixAll.getRetryTopic(consumerGroup)），broker 重投的消息写到这里
            if self.message_model != MessageModel.BROADCASTING:
                retry_topic = MixAll.get_retry_topic(self.consumer_group)
                if retry_topic not in self.subscription_data:
                    # 对齐 Java copySubscription → FilterAPI.buildSubscriptionData(group, retryTopic, SUB_ALL)：
                    # SUB_ALL 下 tagsSet / codeSet 都为**空**（不是 {"*"}），见 subscription_data.FilterAPI
                    sub = FilterAPI.build_subscription_data(retry_topic, "*")
                    self.subscription_data[retry_topic] = sub
            # broker 主动通知 40（消费者上下线 → 立即重算）**不**在这里注册处理器：
            # Java 把它注册在 MQClientAPIImpl（实例级），一个 code 只有一个处理器，
            # 每个消费者各自注册会互相覆盖。改由 MQClientInstance 收到后扇给
            # consumerTable 里的每个消费者（见 ``rebalance_immediately``）。
        # POP 模式的消费线程池必须在 rebalance 之前建好：rebalance 会立刻起每队列的 POP
        # 循环，而循环拿到消息后要投递到这里（Java 的 consumeExecutor）。
        if self.pop_mode:
            # Java ConsumeMessagePopConcurrentlyService 用的就是这个线程池：
            # core=consumeThreadMin、max=consumeThreadMax、队列无界（因此**真实并发度
            # == core**，与 Java 一致）。为了拿到 core/max 两档语义，这里用自实现的
            # ConsumeExecutor（标准库 ThreadPoolExecutor 只有 max 一个上限）。
            self._pop_executor = ConsumeExecutor(
                core_pool_size=max(1, self._core_pool_size),
                maximum_pool_size=max(1, self.consume_thread_max),
                thread_name_prefix="rmq-popconsume-%s" % self.consumer_group)
        # 对齐 Java DefaultMQPushConsumerImpl.start 的顺序：
        # 拉一次路由 → 发心跳（broker 先认识本消费者）→ 立即 rebalance → 起消费线程。
        # 心跳必须在 rebalance 之前：rebalance 要向 broker 查消费者列表。
        self._refresh_routes()
        # 对应 Java DefaultMQPushConsumerImpl.start:1013-1020：路由到手之后、心跳之前，把
        # 非 TAG 订阅发给 broker 校验（CHECK_CLIENT_CONFIG 46）。SQL92 写错时 broker 的过滤层
        # 拿不到编译数据会**静默放行全部消息**，只有这一步能让它变成启动期错误；
        # Java 在这一步失败时 shutdown() 并把异常抛给调用方。
        try:
            self._mq_client.check_client_in_broker()
        except Exception:
            self.shutdown()
            raise
        try:
            self._send_heartbeat_to_all_broker()
        except Exception as e:  # noqa: BLE001
            logger.debug("initial heartbeat failed: %s", e)
        # 首轮分配必须同步完成：否则拉取线程会在空分配集上白转，直到第一轮 rebalance 才生效
        try:
            self._do_rebalance()
        except Exception as e:  # noqa: BLE001
            logger.warning("initial rebalance failed: %s", e)
        self._start_heartbeat_loop()
        self._start_pull_loop()
        self._start_dispatch_loop()
        self._start_offset_persist_loop()
        self._start_lock_loop()
        t = threading.Thread(target=self._rebalance_loop, daemon=True,
                             name="rmq-rebalance-%s" % self.consumer_group)
        t.start()
        self._rebalance_thread = t
        # 消息轨迹分发器（对应 Java DefaultMQPushConsumer.start():765-785）：
        # 放在消费循环全部起来之后，避免轨迹生产者的初始化拖慢首次 rebalance。
        self._start_trace_dispatcher()

    def _start_trace_dispatcher(self) -> None:
        """enable_trace=true 时建 AsyncTraceDispatcher（Type=CONSUME）并注册消费钩子。"""
        if self.enable_trace:
            try:
                dispatcher = AsyncTraceDispatcher(
                    self.consumer_group, TraceDispatcherType.CONSUME,
                    self.trace_msg_batch_num, self.trace_topic, self.rpc_hook)
                dispatcher.set_host_consumer(self)
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
        with self._lock:
            if not self._started:
                return
            self._stop.set()
            # POP：把所有队列标成 dropped，在途批次不再 ack（交给 broker 复活重投）
            if self.pop_mode:
                for pq in self._pop_queues.values():
                    pq.set_dropped(True)
                self._pop_queues.clear()
                if self._pop_executor is not None:
                    self._pop_executor.shutdown(wait=False)
                    self._pop_executor = None
        # 退出前把已消费位点持久化一次（对齐 Java MQClientInstance.shutdown →
        # persistAllConsumerOffset）。注意必须在 _started=False 之前调（_require_client）。
        try:
            self._persist_offsets_once()
        except Exception as e:  # noqa: BLE001
            logger.debug("persist offsets on shutdown failed: %s", e)
        # 顺序消费清退时解锁队列（对齐 Java ConsumeMessageOrderlyService.shutdown → unlockAll）
        if self._is_orderly() and self.message_model != MessageModel.BROADCASTING:
            try:
                mqs = self._assigned_queues()
                if mqs:
                    self._require_client().unlock_batch_mq(self.consumer_group, self.client_id or "", mqs)
            except Exception as e:  # noqa: BLE001
                logger.debug("unlock on shutdown failed: %s", e)
        # 优雅注销（对齐 Java MQClientInstance.unregisterClient）：立刻从各 broker 的
        # ConsumerManager 摘除，不必等心跳超时（默认 ~120s）——否则这段时间内
        # 消费者变更通知/事务回查仍可能发往本已退出的实例。
        try:
            self._require_client().unregister_client_all_brokers(
                self.client_id or "", "", self.consumer_group)
        except Exception as e:  # noqa: BLE001
            logger.debug("unregister on shutdown failed: %s", e)
        with self._lock:
            self._started = False
        for t in (self._persist_thread, self._lock_thread, self._dispatch_thread,
                  self._rebalance_thread, self._heartbeat_thread):
            if t is not None and t.is_alive():
                t.join(timeout=2)
        for t in list(self._queue_threads.values()):
            if t.is_alive():
                t.join(timeout=2)
        self._queue_threads.clear()
        with self._lock:
            self._last_pull_table.clear()
        if self._mq_client is not None:
            try:
                self._mq_client.shutdown()
            except Exception:  # noqa: BLE001
                pass
        for t in self._consume_threads:
            if t.is_alive():
                t.join(timeout=2)
        self._consume_threads.clear()
        # 轨迹分发器最后关（flush 剩余轨迹）—— 对应 Java DefaultMQPushConsumer.shutdown:794
        if self.trace_dispatcher is not None:
            try:
                self.trace_dispatcher.shutdown()
            except Exception as e:  # noqa: BLE001
                logger.warning("trace dispatcher shutdown failed: %s", e)

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        return self._mq_client

    # ---------------- 心跳（消费者注册） ----------------
    def _refresh_routes(self) -> None:
        """订阅 topic 的路由拉一遍，顺带把 broker 地址表填上（心跳要靠它）。

        同时把订阅 topic 登记为「在用」，交给 MQClientInstance 的后台任务周期刷新路由
        （对应 Java 的 MQConsumerInner.subscriptions() → updateTopicRouteInfoFromNameServer()）。
        这样新 topic 被 broker 创建、队列扩容等变化无需等下一次 rebalance 才发现。
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
        # Java `MQClientInstance`:1039 把 `impl.isUnitMode()` 写进 ConsumerData；
        # broker 端据此给自动建出来的 %RETRY% topic 打 UNIT 系统标志
        # （ClientManageProcessor:113 / :186）。
        cd.unit_mode = self.unit_mode
        with self._lock:
            subs = list(self.subscription_data.values())
        for sub in subs:
            cd.subscription_data_set.add(sub)
        hb.consumer_data_set.add(cd)
        return hb

    def _send_heartbeat_to_all_broker(self) -> int:
        """向所有已知 broker 发心跳（对齐 Java MQClientInstance.sendHeartbeatToAllBrokerWithLock）。

        消费者**必须**注册到 broker：broker 的 ConsumerManager 只有收到心跳才知道
        消费组里有哪些 clientId，rebalance 的 GET_CONSUMER_LIST_BY_GROUP 才有返回。
        本实现此前从未发消费者心跳（订阅靠 pull 请求里的 subscription 属性带过去），
        因为当时队列分配是"全给自己"所以没暴露；一旦做真实 rebalance，
        消费者列表为空就分不到任何队列。返回成功台数，供真机验证断言。
        """
        client = self._require_client()
        hb = self._build_heartbeat()
        ok = 0
        for addr in client.get_route_of_all_brokers():
            try:
                client.send_heartbeat(addr, hb, 5000)
                ok += 1
            except Exception as e:  # noqa: BLE001
                logger.debug("heartbeat to %s failed: %s", addr, e)
        if ok > 0:
            self._heartbeat_count += 1
        return ok

    def heartbeat_count(self) -> int:
        """心跳成功轮数（真机验证用）。"""
        return self._heartbeat_count

    def assigned_queue_count(self) -> int:
        """当前分给本实例的队列数（真机验证多实例分配用）。"""
        return len(self._assigned_queues())

    def assigned_queue_keys(self) -> List[str]:
        """当前分配队列的 key 列表（真机验证"同组两实例不重不漏"用）。"""
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

    # ---------------- 消费循环 ----------------
    def _start_pull_loop(self) -> None:
        self._pulling = True
        # 对齐 Java PullMessageService 的并发长轮询语义：broker 会为每个队列挂起
        # 长轮询请求，消息到达立即返回——因此每个队列必须各有一个拉取线程，
        # 否则一个空队列的长轮询（~15s suspend）会阻塞其余队列的投递。
        self._rebalance_pull_threads()

    def _rebalance_pull_threads(self) -> None:
        """按当前分配的队列同步拉取线程集，并清理被撤销/已停摆队列的状态。

        对齐 Java ``RebalanceImpl.updateProcessQueueTableInRebalance``：队列被撤走时必须
        ①persist 该队列**已消费**位点 ②丢弃 ProcessQueue（在途消息不再消费，交新属主重投）
        ③顺序消费集群模式还要 UNLOCK_BATCH_MQ。少任何一步，被撤销队列里的在途消息都会被
        **旧实例继续消费**，与新属主重复（真机 S6 多出重复消息的根因）。

        Java 在同一趟里还做**自愈**：仍归本实例、但拉取停摆超过 ``PULL_MAX_IDLE_TIME`` 的
        ProcessQueue 也按撤走处理（``isPullExpired`` 分支，:442），紧接着的 add 分支用新的
        ProcessQueue 重建拉取，从而把死掉/卡住的消费循环救回来。

        这里刻意保持 Java 的**先撤后建**顺序，并且把撤的收尾（持久化位点 / UNLOCK）放在
        建线程**之前**：反过来会让新循环拿旧位点起拉、又把更小的位点写回去，白增重复投递。
        """
        current = {self._mq_key(mq): mq for mq in self._assigned_queues()}
        retired: List[Tuple[MessageQueue, Optional[int]]] = []
        pop = self.pop_mode
        with self._lock:
            # ①撤：被分走的 + 拉取停摆的（Java 的 !mqSet.contains / isPullExpired 两支）
            for key in list(self._queue_threads.keys()):
                if key not in current:
                    self._retire_queue_locked(key, None, retired)
                    continue
                if self._started and self._pull_stalled_locked(key):
                    # Java RebalanceImpl:449 的告警原文，用于排查"消费停摆被自愈"的现场
                    logger.error("[BUG]doRebalance, %s, try remove unnecessary mq, %s, "
                                 "because pull is pause, so try to fixed it",
                                 self.consumer_group, key)
                    self._retire_queue_locked(key, current[key], retired)
        # 收尾在锁外做（网络 RPC），且必须早于 ②建
        if retired:
            self._on_queues_revoked(retired)
        with self._lock:
            # ②建：为缺失的队列起循环
            for key, mq in current.items():
                if key in self._queue_threads:
                    continue
                if pop and key not in self._pop_queues:
                    self._pop_queues[key] = PopProcessQueue()
                # 线程刚建、循环还没跑到盖章处，先用当前时刻占位，避免下一趟误判停摆
                self._last_pull_table[key] = time.time()
                # 队列一旦分配就进 _mq_map（Java ProcessQueueTable 的键集即"已分配"），
                # 不等第一条消息：位点持久化、307 运行信息、220 重置都靠这份映射。
                self._mq_map[key] = mq
                # POP 模式：每队列起一个 POP 循环（不拉位点、不建拉取缓冲区）
                target = self._queue_pop_loop if pop else self._queue_pull_loop
                t = threading.Thread(target=target, args=(mq,), daemon=True,
                                     name="rmq-%s-%s-%s" % ("pop" if pop else "pull",
                                                            self.consumer_group, key))
                self._queue_threads[key] = t
                t.start()

    def _retire_queue_locked(self, key: str, fallback_mq: Optional[MessageQueue],
                             retired: List[Tuple[MessageQueue, Optional[int]]]) -> None:
        """丢弃一个队列的全部本地状态（调用方须持锁），位点交给调用方在锁外持久化。

        线程表里的条目一删，原循环线程下一轮 ``_owns_queue`` 即失败并自行退出；
        ``_last_pull_table`` 一并清掉，避免同名队列复用线程时继承旧时刻。
        ``fallback_mq`` 用于自愈分支：停摆队列可能一条消息都没拉过，_mq_map 里还没有条目，
        但它的已消费位点是真实的，漏 persist 就会让新属主从头重投。
        """
        self._queue_threads.pop(key, None)  # 循环内检测到退出
        self._last_pull_table.pop(key, None)
        mq = self._mq_map.pop(key, fallback_mq)
        self._pending.pop(key, None)
        self._lock_ok.discard(key)
        off = self._consume_offsets.pop(key, None)
        self._offset_table.pop(key, None)
        # POP：标记 dropped，在途批次不再消费也不 ack（交给 broker 复活）
        pq = self._pop_queues.pop(key, None)
        if pq is not None:
            pq.set_dropped(True)
        if mq is not None:
            retired.append((mq, off))

    def _pull_stalled_locked(self, key: str) -> bool:
        """该队列的拉取/弹出循环是否已停摆（Java ProcessQueue.isPullExpired）。须持锁调用。

        两个判据都算死：线程已退出（异常穿透），或线程还在但超过 ``PULL_MAX_IDLE_TIME``
        没发起过任何一次拉取（卡在锁/流控/网络之外的地方）。从未盖过章的新循环不算。
        """
        t = self._queue_threads.get(key)
        if t is not None and not t.is_alive():
            return True
        began = self._last_pull_table.get(key)
        if began is None:
            return False
        return time.time() - began > PULL_MAX_IDLE_TIME

    def _on_queues_revoked(self, revoked: List[Tuple[MessageQueue, Optional[int]]]) -> None:
        """被撤销队列的收尾（对应 Java RebalanceImpl.removeUnnecessaryMessageQueue）。"""
        broadcast = self.message_model == MessageModel.BROADCASTING
        if broadcast:
            # 广播模式位点只存本地
            self._save_local_offsets()
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
            if self._is_orderly():
                # 顺序消费：释放 broker 队列锁，新属主才能立刻接上
                try:
                    client.unlock_batch_mq(self.consumer_group, self.client_id or "", [mq])
                except Exception as e:  # noqa: BLE001
                    logger.debug("unlock on revoke failed for %s: %s", mq, e)
        logger.info("queues revoked, group=%s count=%d", self.consumer_group, len(revoked))

    def _owns_queue(self, key: str) -> bool:
        """本线程是否仍持有该队列（rebalance 撤走或换了拉取线程后即失效）。"""
        with self._lock:
            return self._queue_threads.get(key) is threading.current_thread()

    @staticmethod
    def _mq_key(mq: MessageQueue) -> str:
        return "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id)

    def _all_queues_of_topic(self, topic: str) -> List[MessageQueue]:
        """topic 的全部队列（对应 Java RebalanceImpl.topicSubscribeInfoTable）。"""
        client = self._require_client()
        try:
            publish = client.get_topic_publish_info(topic)
        except MQClientException as e:  # noqa: BLE001
            logger.debug("rebalance: no route for topic %s: %s", topic, e)
            return []
        return [MessageQueue(topic, mq.broker_name, mq.queue_id) for mq in publish.msg_queue_list]

    def _assigned_queues(self) -> List[MessageQueue]:
        """当前分给本实例的队列集（_do_rebalance 计算，对应 Java ProcessQueueTable 的键集）。"""
        with self._lock:
            return list(self._assigned)

    def _do_rebalance(self) -> None:
        """按 Java RebalanceImpl.rebalanceByTopic 计算分配，再同步拉取线程集。

        BROADCASTING：全部队列都归自己（不做 broker 协调）。
        CLUSTERING：查 broker 上的消费者列表 → 排序 → 分配策略 → 取本实例那一份。
        查不到消费者列表时**保留现有分配**（Java 仅告警；绝不回退成"独占全部队列"，
        否则同组多实例会互相重复消费）。
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
        # 新分配的队列**立刻**解析初始位点（对齐 Java RebalanceImpl.updateProcessQueueTableInRebalance：
        # 新队列 removeDirtyOffset → computePullFromWhereWithException → offsetStore.updateOffset）。
        # 不能留到第一次拉取时才惰性解析：CONSUME_FROM_LAST_OFFSET 的语义是"分配时刻的最新位点"，
        # 惰性解析会把「分配之后、首次拉取之前」新产生的消息一并跳过（真机上表现为消费者一直收不到）。
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
                off = self._resolve_initial_offset(client, mq, sub)
            except Exception as e:  # noqa: BLE001
                logger.debug("resolve initial offset for %s failed: %s", mq, e)
                continue
            with self._lock:
                self._offset_table.setdefault(key, off)
        self._rebalance_pull_threads()

    def rebalance_immediately(self) -> None:
        """实例收到 broker 的 40 通知后叫醒本消费者的重平衡循环。

        对应 Java ``MQClientInstance#rebalanceImmediately`` → ``RebalanceService#wakeup``。
        """
        self._rebalance_now.set()

    def _rebalance_loop(self) -> None:
        """周期重算分配（对齐 Java RebalanceService 默认 20s），或被通知时立即重算。

        启动阶段且**当前没有任何分配**时缩短为 2s 重试：消费者可能先于 topic 被创建启动
        （``autoCreateTopicEnable`` 下 broker 由生产者的首次发送建 topic），此时真实路由还
        拉不到——消费端不做默认 topic 兜底（见 MQClientInstance.update_topic_route_info_
        from_name_server），死等 20s 会长时间不消费。该快速重试只在启动后 60s 内生效，
        避免长期订阅了不存在 topic 的客户端持续高频打 NameServer。
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
        """单队列拉取循环：长轮询拉取 → 推入待消费缓冲（Java PullMessageService+ProcessQueue）。"""
        client = self._require_client()
        orderly = self._is_orderly()
        key = self._mq_key(mq)
        while not self._stop.is_set() and self._started:
            if not self._owns_queue(key):
                return
            # Java DefaultMQPushConsumerImpl.pullMessage:253 —— 每次**发起**拉取就盖时刻，
            # 在流控/锁判定之前：判据是"这条循环还在跑"，不是"这轮真的打了网络"。
            with self._lock:
                self._last_pull_table[key] = time.time()
            with self._lock:
                sub = self.subscription_data.get(mq.topic)
            if sub is None:
                return
            # 顺序消费：broker 未确认锁定（LOCK_BATCH_MQ）的队列不拉取
            if orderly and key not in self._lock_ok:
                time.sleep(0.2)
                continue
            # 流控（对齐 Java ProcessQueue.putMessage / checkReconsumeTimes）：
            # 条数 / 字节数 / topic 级累计 / 并发跨度任一超限就暂停本队列拉取
            if self._flow_control_hit(mq, key):
                time.sleep(0.1)
                continue
            offset = self._offset_table.get(key)
            if offset is None:
                try:
                    offset = self._resolve_initial_offset(client, mq, sub)
                except Exception as e:  # noqa: BLE001
                    logger.debug("resolve initial offset failed for %s: %s", mq, e)
                    time.sleep(1.0)
                    continue
                self._offset_table[key] = offset
            try:
                sys_flag = PullSysFlag.build_sys_flag(commit_offset=False,
                                                      suspend=True,
                                                      subscription=True,
                                                      class_filter=False)
                pull_began = time.time()
                result = client.pull_message(self.consumer_group, mq, offset,
                                             self.pull_batch_size, sys_flag, 0,
                                             sub.sub_string or "*", sub.sub_version,
                                             sub.expression_type,
                                             timeout_millis=self.pull_timeout_millis,
                                             max_msg_bytes=self.pull_batch_size_in_bytes,
                                             suspend_timeout_millis=self.pull_suspend_timeout_millis)
                # 消费统计（Java PullCallback.onSuccess：RT 每次都记，TPS 只在有消息时记）
                if self._stats_manager is not None:
                    self._stats_manager.inc_pull_rt(self.consumer_group, mq.topic,
                                                    int((time.time() - pull_began) * 1000))
                    if result.status == PullStatus.FOUND and result.msg_found_list:
                        self._stats_manager.inc_pull_tps(self.consumer_group, mq.topic,
                                                         len(result.msg_found_list))
            except RemotingTimeoutException as e:
                # 长轮询在 suspend 期间无新消息触发客户端超时属正常行为：broker 将
                # suspend 时间钳制为其自身 brokerSuspendMaxTimeMillis（默认 ~15s），
                # 忽略客户端下发的 suspend_timeout_millis，故空闲队列会周期性超时。
                # 这不是错误，仅 debug 级别，避免污染客户端运行日志（见 logging.py）。
                logger.debug("pull long-poll timeout for %s (benign, will retry): %s", mq, e)
                continue
            except Exception as e:  # noqa: BLE001
                # broker 侧非 SUCCESS 码（TOPIC_NOT_EXIST / PULL_NOT_FOUND 等）或其他错误：
                # 多为 topic 尚未创建等预期路径，debug + 短暂退避，避免热循环
                logger.debug("pull error for %s: %s: %s", mq, type(e).__name__, e)
                time.sleep(0.5)
                continue

            # 投递前的客户端侧过滤（对齐 Java PullAPIWrapper.processPullResult:113-128）：
            # 先二次 tag 过滤，再跑 FilterMessageHook。**必须在拿 _lock 之前做** ——
            # 钩子是用户代码，可能阻塞，不能压在入队的临界区里。
            # 拉取路径被摘掉的消息不 ack（Java 亦然）：位点照常推进 = 静默跳过。
            if result.status == PullStatus.FOUND and result.msg_found_list:
                result.msg_found_list = self._filter_messages_for_delivery(
                    mq, sub, result.msg_found_list)

            # 入队与"是否仍持有该队列"的判断必须原子：长轮询期间被 rebalance 撤走的队列，
            # 这批消息按 Java 语义（ProcessQueue.isDropped()）**直接丢弃**——不消费、不推进位点，
            # 由新属主从我们最后持久化的位点重投，否则两实例会重复消费同一条消息。
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
                # 拉取游标推进到 nextBeginOffset；"已消费位点"由 _consume_offsets 单独跟踪并持久化
                if result.next_begin_offset is not None:
                    self._offset_table[key] = result.next_begin_offset

    def _update_msg_acc_cnt(self, key: str, msgs: List[MessageExt]) -> None:
        """对应 Java ``ProcessQueue.putMessage`` 里的 ``msgAccCnt`` 计算。

        ``ProcessQueue.java:148-158``::

            long accTotal = Long.parseLong(msg.getProperty(MAX_OFFSET)) - msg.getQueueOffset();
            if (accTotal > 0) this.msgAccCnt = accTotal;

        取的是**本批最后一条**消息；用来喂 ``adjust_thread_pool`` 的阈值比较。
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
        """读 ``msgAccCnt``：给 key 读单队列，不给则求和（等价 computeAccumulationTotal）。"""
        with self._lock:
            if key is None:
                return sum(self._msg_acc_cnt_table.values())
            return int(self._msg_acc_cnt_table.get(key, 0))

    # ---------------------------------------------------------------- POP 消费循环

    def _queue_pop_loop(self, mq: MessageQueue) -> None:
        """单队列 POP 循环（对应 Java DefaultMQPushConsumerImpl.popMessage 的回调部分）。

        与 pull 循环的关键差别：
          - **不查、不提交消费位点**：进度由 broker 侧的 checkpoint 跟踪，确认只靠 ack；
          - 弹出即投递给消费线程，本轮循环立刻继续（不等消费结果）；
          - ``POLLING_NOT_FOUND``（队列暂时没消息）是**正常态**，直接下一轮，不算错误。
        """
        client = self._require_client()
        key = self._mq_key(mq)
        invisible = self.pop_invisible_time
        if invisible < MIN_POP_INVISIBLE_TIME or invisible > MAX_POP_INVISIBLE_TIME:
            # Java 的钳制：超出 [5s, 300s] 一律回落到 60s
            invisible = 60000
        # Java PopRequest 默认 ConsumeInitMode.MAX；这里按 consume_from_where 映射，
        # 让"从头消费"的语义在 POP 模式下也成立。
        init_mode = (CONSUME_INIT_MODE_MIN
                     if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
                     else CONSUME_INIT_MODE_MAX)
        while not self._stop.is_set() and self._started:
            if not self._owns_queue(key):
                return
            pq = self._pop_queues.get(key)
            if pq is None or pq.is_dropped():
                return
            # Java PopProcessQueue.isPullExpired 读的是 lastPopTimestamp，而它在**发起**弹出时
            # 就被盖章（DefaultMQPushConsumerImpl.popMessage 的入口）：流控/长轮询挂起都不该
            # 让一条还在跑的循环被判成停摆。
            with self._lock:
                now = time.time()
                self._last_pull_table[key] = now
            pq.last_pop_timestamp = now
            with self._lock:
                sub = self.subscription_data.get(mq.topic)
            if sub is None:
                return
            # 流控：已弹未 ack 太多就先缓一缓（Java popThresholdForQueue）
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
                # 长轮询挂起期间没有消息 → 客户端先超时，属正常行为，直接下一轮
                logger.debug("pop long-poll timeout for %s (benign, will retry)", mq)
                continue
            except Exception as e:  # noqa: BLE001
                logger.debug("pop error for %s: %s: %s", mq, type(e).__name__, e)
                time.sleep(0.5)
                continue

            # 弹出后队列被 rebalance 撤走：这一批**既不消费也不 ack**
            # （Java 对应 PopProcessQueue.isDropped() 分支），交给 invisibleTime 到期后
            # broker 自动复活重投给新属主。
            if not self._owns_queue(key) or pq.is_dropped():
                logger.debug("queue %s revoked during pop, discard %d messages un-acked",
                             mq, len(result.msg_found_list or ()))
                return
            found = result.status == PopStatus.FOUND
            if found:
                # 消费统计（Java popMessage 的 PopCallback.onSuccess:556-563）：POP 路径
                # 与 pull 路径一样要把 RT/TPS 记进状态表，否则 POP 消费者的
                # consumerRunningInfo() 里 pull 侧永远是 0 —— 运维看板上"这个消费者没在
                # 拉取"和"这个消费者根本没起来"就分不出来了。RT 在 FOUND 分支入口就记
                # （Java 在判空之前记），TPS 只按真正弹到的条数记。
                if self._stats_manager is not None:
                    self._stats_manager.inc_pull_rt(self.consumer_group, mq.topic,
                                                    int((time.time() - began) * 1000))
                    if result.msg_found_list:
                        self._stats_manager.inc_pull_tps(self.consumer_group, mq.topic,
                                                         len(result.msg_found_list))
            if found and result.msg_found_list:
                pq.inc_found_msg(len(result.msg_found_list))
                # 投递前过滤（对齐 Java processPopResult:621-661）：POP 路径**必须 ack 被摘掉的**，
                # 否则 invisibleTime 到期后 broker 会复活重投 —— 表现为"过滤没生效"。
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
                # 空结果：若 broker 没按 poll_time 挂起（立即返回）就会变成热循环，
                # 这里按"本轮耗时过短"兜底退避，避免打爆 broker。
                if (time.time() - began) * 1000.0 < 200:
                    time.sleep(0.2)
            # NO_NEW_MSG / POLLING_NOT_FOUND / POLLING_FULL 都直接进下一轮

    def _submit_pop_consume_request(self, msgs: List[MessageExt], pq: PopProcessQueue,
                                    mq: MessageQueue) -> None:
        """按 consume_message_batch_max_size 切批后投给消费线程池。

        对应 Java ConsumeMessagePopConcurrentlyService.submitPopConsumeRequest。
        """
        size = max(1, self.consume_message_batch_max_size)
        batches = [msgs[i:i + size] for i in range(0, len(msgs), size)] or [msgs]
        for batch in batches:
            if not batch:
                continue
            if self._pop_executor is not None:
                self._pop_executor.submit(self._consume_pop_batch, batch, pq, mq)
            else:
                # 未起线程池（单测或未 start）：同步执行
                self._consume_pop_batch(batch, pq, mq)

    def _consume_pop_batch(self, msgs: List[MessageExt], pq: PopProcessQueue,
                           mq: MessageQueue) -> None:
        """消费一个 POP 批次并按结果 ack / 延长不可见时间。

        对应 Java ConsumeMessagePopConcurrentlyService$ConsumeRequest.run。
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
            # 已经超过 invisibleTime：ack 也不会被承认，直接放弃本批（等 broker 复活重投）
            logger.debug("pop timeout, abort consume for %s: popTime=%s invisible=%s",
                         mq, pop_time, invisible)
            pq.dec_found_msg(-len(msgs))
            return

        self._reset_retry_topic_and_namespace(msgs)
        context = ConsumeConcurrentlyContext(mq)
        # 对齐 Java ConsumeConcurrentlyContext.ackIndex = Integer.MAX_VALUE（本端口的默认值
        # 已经是它，这里显式钳成 size-1 只是省掉一次 clamp）：CONSUME_SUCCESS 默认全部 ack。
        # 若这里写成 -1，一条都不会 ack，消息在 invisibleTime 到期后被 broker 复活重投 ——
        # 短观测窗口下会伪装成通过。
        context.ack_index = len(msgs) - 1
        hook_ctx = None
        if self.consume_message_hook_list:
            hook_ctx = self._build_consume_hook_context(msgs, mq)
            self.execute_consume_hook_before(hook_ctx)
        begin_ms = time.time() * 1000
        has_exception = False
        try:
            status = self.message_listener.consume_message(msgs, context)
        except Exception as e:  # noqa: BLE001
            # Java：消费抛异常按 RECONSUME_LATER 处理
            logger.debug("pop listener error, treat as RECONSUME_LATER: %s", e)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
            has_exception = True
        # Java POP :396-405 —— listener 返回 null 同样按 RECONSUME_LATER 处理（与经典并发同一套
        # ConsumeRequest.run）。归一化**前**的 status 留给 returnType（返回 null 记 RETURNNULL，
        # 不是 FAILED）；TPS 与钩子上下文都用归一化**后**的值（Java 的 processConsumeResult
        # 拿到的也是归一化后的 —— 返回 null 的 listener 在 Java 里算**失败**，不是成功）
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
            # 消费期间队列被撤走或已超时：结果不再处理
            pq.dec_found_msg(-len(msgs))
            return
        self._process_pop_consume_result(status, context, msgs, pq, mq)

    @staticmethod
    def _is_pop_timeout(msgs: List[MessageExt], pop_time: int, invisible: int) -> bool:
        """Java ConsumeRequest.isPopTimeout：不能解析出 popTime/invisibleTime 时按超时处理。"""
        if not msgs or pop_time <= 0 or invisible <= 0:
            return True
        return int(time.time() * 1000) - pop_time >= invisible

    def _process_pop_consume_result(self, status, context: ConsumeConcurrentlyContext,
                                    msgs: List[MessageExt], pq: PopProcessQueue,
                                    mq: MessageQueue) -> None:
        """对应 Java ConsumeMessagePopConcurrentlyService.processConsumeResult。"""
        ack_index = context.ack_index
        if status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
            if ack_index >= len(msgs):
                ack_index = len(msgs) - 1
        else:
            # RECONSUME_LATER：一条都不 ack
            ack_index = -1

        for i in range(0, ack_index + 1):
            self._ack_pop_msg(msgs[i])
            pq.ack()

        for i in range(ack_index + 1, len(msgs)):
            pq.ack()
            msg = msgs[i]
            # 超过最大重试次数：Java 走 checkNeedAckOrDelay（太老就直接 ack 丢弃，
            # 否则按消息已存活时间选一个延迟档位）
            if self.max_reconsume_times >= 0 and msg.reconsume_times >= self.max_reconsume_times:
                self._check_need_ack_or_delay(msg)
                continue
            self._change_pop_invisible_time(msg, context.delay_level_when_next_consume)

    def _check_need_ack_or_delay(self, msg: MessageExt) -> None:
        """Java checkNeedAckOrDelay：重试次数用尽后的兜底。

        消息存活时间已经超过最大延迟档位的 2 倍 → 直接 ack 丢弃（不再无限重试）；
        否则按存活时间选一个档位继续延长不可见时间。
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
        """从 POP_CK 解出 ack/延长不可见时间需要的 (topic, brokerName, queueId, offset, ck)。

        ⚠ 两处都不能想当然：
          1. topic 要用 ``ExtraInfoUtil.getRealTopic`` 按 CK 的 retryFlag 还原 —— 复活消息
             （retryFlag=1）的真实 topic 是 ``%RETRY%<group>_<topic>``，**不是**消息上的 topic；
          2. 地址要按 CK 里的 brokerName 反查，不能按 topic 查路由 —— retry topic 通常没有
             独立路由表项，按 topic 查会失败（Java 同理走 findBrokerAddressInSubscribe）。
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
        """单条 ack（对应 Java DefaultMQPushConsumerImpl.ackAsync）。"""
        target = self._pop_ck_target(msg)
        if target is None:
            return
        topic, broker_name, queue_id, offset, ck = target
        try:
            client = self._require_client()
            client.ack_message(self.consumer_group, topic, queue_id, ck, offset,
                               broker_name=broker_name,
                               addr=client.broker_addr_of(broker_name))
        except Exception as e:  # noqa: BLE001
            # ack 失败不致命：消息会在 invisibleTime 到期后被 broker 复活重投
            logger.debug("ack failed for %s: %s", msg.msg_id, e)

    def _change_pop_invisible_time(self, msg: MessageExt, delay_level: int) -> None:
        """延长不可见时间（对应 Java changePopInvisibleTime）。

        ``delay_level == 0`` 时 Java 用消息已重试次数当档位；档位表是**秒**，接口要毫秒。
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
            client.change_invisible_time(self.consumer_group, topic, queue_id, ck, offset,
                                         delay_second * 1000,
                                         broker_name=broker_name,
                                         addr=client.broker_addr_of(broker_name))
        except Exception as e:  # noqa: BLE001
            logger.debug("change invisible time failed for %s: %s", msg.msg_id, e)

    def _flow_control_hit(self, mq: MessageQueue, key: str) -> bool:
        """是否触发流控（对齐 Java ProcessQueue.putMessage 的五个阈值检查）。

        - ``pull_threshold_for_queue``：本队列已拉未消费**条数**（默认 1000）
        - ``pull_threshold_size_for_queue``：本队列已拉未消费**字节数 MB**（默认 100）
        - ``pull_threshold_for_topic`` / ``pull_threshold_size_for_topic``：同 topic 全部队列累计（-1 关闭）
        - ``consume_concurrently_max_span``：已拉未消费消息 queueOffset 的**跨度**（默认 2000，
          防止"某条消息一直消费失败、后面的堆着"导致位点跨度失控）
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

    def _resolve_initial_offset(self, client: MQClientInstance, mq: MessageQueue,
                                sub: SubscriptionData) -> int:
        if sub.expression_type == ExpressionType.SQL92:
            # SQL 过滤无 offset 语义，默认最新
            return client.get_max_offset(mq)
        if self.message_model == MessageModel.BROADCASTING:
            # 广播模式：offset 只存本地（对齐 Java LocalFileOffsetStore）
            stored = self._load_local_offsets()
            key = "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id)
            if key in stored:
                return stored[key]
            if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
                return client.get_min_offset(mq)
            return client.get_max_offset(mq)
        # 集群模式：先查 broker 上已提交的位点（对齐 Java RemoteBrokerOffsetStore.readOffset）
        try:
            stored = client.query_consumer_offset(self.consumer_group, mq, set_zero_if_not_found=False)
            if stored is not None and stored >= 0:
                return stored
        except Exception as e:  # noqa: BLE001
            logger.debug("query consumer offset for %s not found: %s", mq, e)
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
            return client.get_min_offset(mq)
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
            return client.search_offset_by_timestamp(mq, self._consume_timestamp_millis())
        if NamespaceUtil.is_retry_topic(mq.topic):
            # Java RebalancePushImpl:181-182：首次消费且无已提交位点时，%RETRY% 主题从 0 开始
            # （重试消息要全量重试），而不是从最大位点跳过
            return 0
        # 默认 CONSUME_FROM_LAST_OFFSET
        return client.get_max_offset(mq)

    # ---------------- 分发消费 ----------------
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
                # 取值与"是否仍持有该队列"必须同一把锁内完成：key 是本轮开始时的快照，
                # 队列可能已被 rebalance 撤走（那时缓冲已被清理，不能再消费）
                with self._lock:
                    mq = self._mq_map.get(key)
                    dq = self._pending.get(key)
                    if mq is None or dq is None or not dq:
                        continue
                    n = min(len(dq), max(1, self.consume_message_batch_max_size))
                    batch = [dq.popleft() for _ in range(n)]
                if not batch:
                    continue
                try:
                    done = self._consume_batch(key, mq, batch)
                    progressed = progressed or done
                except Exception as e:  # noqa: BLE001
                    # 分发路径意外异常：批次塞回队首，稍后重试（不要让它杀死分发线程）
                    logger.error("dispatch batch error (will retry): %s: %s", type(e).__name__, e)
                    with self._lock:
                        dq2 = self._pending.get(key)
                        if dq2 is not None:
                            for m in reversed(batch):
                                dq2.appendleft(m)
                    time.sleep(0.1)
            if not progressed:
                time.sleep(0.05)

    def _reset_retry_topic_and_namespace(self, msgs: List[MessageExt]) -> None:
        """对应 Java DefaultMQPushConsumerImpl.resetRetryAndNamespace（分发前调用）。

        重投消息实际存在 ``%RETRY%consumerGroup`` 下，broker 会把原始 topic 写进
        ``RETRY_TOPIC`` 属性。Java 在交给 listener **之前**用它把 topic 还原，
        listener 才能看到业务原始 topic；不做的话用户按 topic 分支的代码会走错。
        """
        group_topic = MixAll.get_retry_topic(self.consumer_group)
        for msg in msgs:
            retry_topic = msg.get_property(MessageConst.PROPERTY_RETRY_TOPIC)
            if retry_topic and msg.topic == group_topic:
                msg.topic = retry_topic
            if self.namespace:
                msg.topic = NamespaceUtil.without_namespace(msg.topic, self.namespace)

    # ---------------- broker 主动请求：220 / 221 / 307 / 309 ----------------
    # 这四个请求由 broker（或 mqadmin 经 broker）反向打给客户端，对应 Java
    # ClientRemotingProcessor。处理入口注册在 MQClientInstance 上（见
    # mq_client.py），因为它要按 consumerGroup 找到**对应的**消费者实例，而不是
    # 每个消费者各注册一次（那样同一进程里多消费组时只有最后一个生效）。

    def reset_offset(self, topic: str, offset_table: Dict[MessageQueue, int]) -> None:
        """对应 Java MQClientInstance.resetOffset（220 的处理逻辑）。

        顺序：suspend → 命中的队列 drop+clear → 等一会儿让在途消费跑完 →
        写新位点 → 撤销该队列（触发 rebalance 重新分配并从新位点开始）。
        """
        if topic is None or not offset_table:
            return
        with self._lock:
            hit: List[Tuple[MessageQueue, str]] = []
            for key, mq in list(self._mq_map.items()):
                if mq.topic != topic:
                    continue
                off = offset_table.get(mq)
                if off is None:
                    continue
                self._pending.pop(key, None)   # 等价 ProcessQueue.clear()
                self._offset_table.pop(key, None)
                self._consume_offsets[key] = int(off)
                hit.append((mq, key))
        if not hit:
            return
        # Java 用 RESET_OFFSET_MAX_WAIT（5 秒）等并发消费跑完；这里缩短以免阻塞
        # 读线程太久（220 是 oneway，broker 不等响应，但仍应尽快返回）。
        time.sleep(0.2)
        self._on_queues_revoked([(mq, None) for mq, _ in hit])
        try:
            self._do_rebalance()
        except Exception as e:  # noqa: BLE001
            logger.debug("rebalance after reset offset failed: %s", e)
        logger.info("reset offset applied, group=%s topic=%s queues=%d",
                    self.consumer_group, topic, len(hit))

    def get_consumer_status(self, topic: str) -> Dict[MessageQueue, int]:
        """对应 Java MQClientInstance.getConsumerStatus（221 的应答数据源）。

        Java 返回 ``offsetStore.cloneOffsetTable(topic)``，即**已消费位点**表
        （不是拉取游标）。
        """
        out: Dict[MessageQueue, int] = {}
        with self._lock:
            for key, mq in list(self._mq_map.items()):
                if topic is not None and mq.topic != topic:
                    continue
                off = self._consume_offsets.get(key)
                if off is not None:
                    out[mq] = int(off)
        return out

    def consumer_running_info(self) -> ConsumerRunningInfo:
        """对应 Java DefaultMQPushConsumerImpl.consumerRunningInfo（307 的应答）。"""
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
            # POP 模式下弹出去 pop_queues（Java popProcessQueueTable），classic 的
            # processQueueTable 是空的 —— 两把表在 Java 里互斥，307 里也互斥：同一把队列
            # 既出现在 mqTable 又出现在 mqPopTable 会让控制台把一路消费数成两路。
            # _mq_map 是"已分配"注册表（停摆自愈、位点持久化、220 重置都靠它），两种模式都写，
            # 所以这里按模式过滤而不是不写。
            pop_keys = set(self._pop_queues or ()) if self.pop_mode else ()
            for key, mq in self._mq_map.items():
                if key in pop_keys:
                    continue
                pqi = ProcessQueueInfo()
                pqi.commit_offset = int(self._consume_offsets.get(key, 0))
                pqi.cached_msg_count = len(self._pending.get(key) or ())
                pqi.droped = False
                # Java ProcessQueue.fillOutRunningInfo:456 —— 运维看的就是这个时刻；
                # 写死 0 会让"这一路有没有停摆"在 307 应答里完全看不出来。
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
                    # Java PopProcessQueue 用 lastPopTimestamp 顶替 lastPullTimestamp
                    # 判停摆（isPullExpired:74），这里填同一个时刻保持可比。
                    pqi.last_pull_timestamp = int(pq.last_pop_timestamp * 1000)
                    info.mq_pop_table[mq] = pqi.to_dict()
        info.subscription_set = [s.to_dict() if hasattr(s, "to_dict") else dict(s.__dict__)
                                 for s in subs]
        # statusTable（Java consumerRunningInfo：consumeStatus(group, topic)，minute 快照）
        for s in subs:
            if self._stats_manager is not None:
                info.status_table[s.topic] = self._stats_manager.consume_status(
                    self.consumer_group, s.topic).to_dict()
            else:
                info.status_table[s.topic] = ConsumeStatus().to_dict()
        return info

    def consume_message_directly(self, msg: MessageExt,
                                 broker_name: Optional[str]) -> ConsumeMessageDirectlyResult:
        """对应 Java 的 consumeMessageDirectly（并发 :102-139 / 顺序 :103-161）。

        由管理端 ``CONSUME_MESSAGE_DIRECTLY`` 触发，把 broker 上的一条消息直接丢给
        listener，应答里的 ``order`` 标志区分消费模式（broker 侧按它选 DLQ 口径）。
        顺序侧的映射比并发侧多两个成员：COMMIT → ``CR_COMMIT``、ROLLBACK →
        ``CR_ROLLBACK``（Java 顺序 :125-140），并发侧返回它们只会落到 ``default:``。
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
        # Java 顺序 :156 —— autoCommit 读的是 **listener 跑完之后**的上下文值（放在
        # try/catch 之外、异常路径同样读）。listener 里置 false 的 binlog 用法靠这条
        # 回传让 broker 知道「这条直接消费没提交」；提前读初值等于把它吞掉，
        # 真机上只表现为 mqadmin 返回的 autoCommit 恒为 true。
        result.auto_commit = context.auto_commit if orderly else True
        result.spent_time_mills = int(time.time() * 1000) - begin
        return result

    def _requeue_pending(self, key: str, batch: List[MessageExt]) -> None:
        """把这一批塞回本地队列队首，等价 Java ``makeMessageToConsumeAgain``/``rollback``。

        本端口的待消费缓冲是 ``_pending`` 双端队列，批次在分发给 listener **之前**就被
        弹出队首了；而拉取游标（``_offset_table``）在拉取那一刻已经推到 ``nextBeginOffset``，
        所以"位点不前进"并不会让 broker 把这几条再发一遍 —— 想让它们原地重试就必须显式
        放回去（Java 那边消息始终留在 ProcessQueue 里，不需要这一步）。
        """
        with self._lock:
            dq = self._pending.get(key)
            if dq is not None:
                for m in reversed(batch):
                    dq.appendleft(m)

    def _consume_batch(self, key: str, mq: MessageQueue, batch: List[MessageExt]) -> bool:
        """消费一个批次并处理回投/挂起。返回消费位点是否前进。"""
        listener = self.message_listener
        broadcast = self.message_model == MessageModel.BROADCASTING
        self._reset_retry_topic_and_namespace(batch)
        # ---- 顺序消费（Java ConsumeMessageOrderlyService）----
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
                # Java:463-469 —— 异常只置 hasException，status 留 null，由下面统一
                # 变成挂起：顺序消费永远没有"异常即 ack"这条路
                logger.debug("orderly listener error (retry in place): %s", e)
                status = None
                ohas_exception = True
            # Java:474-481 —— null / ROLLBACK / 挂起都要留一条 warn
            if (status is None or status == ConsumeOrderlyStatus.ROLLBACK
                    or status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT):
                logger.warning("consumeMessage Orderly return not OK, Group: %s Msgs: %d MQ: %s",
                               self.consumer_group, len(batch), mq)
            raw_status = status
            if status is None or status not in _ORDERLY_STATUSES:
                # Java:502-504 —— null 在钩子之前就按挂起处理；Java 靠静态类型保证
                # status 只能是枚举成员，动态类型下"返回了别的东西"走同一条兜底：
                # 否则它会一路落进 SUCCESS 分支，把没消费成功的消息静默 ack 掉。
                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
            # 钩子拿**归一化后**的 status，success 判据是 SUCCESS||COMMIT（Java:506-511），
            # 而 returnType 用归一化**前**的（Java:483-496）
            self._finish_consume_hook(
                ohook_ctx, raw_status, ohas_exception, obegin_ms,
                failed=status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT,
                succeeded=status in (ConsumeOrderlyStatus.SUCCESS, ConsumeOrderlyStatus.COMMIT),
                hook_status=status)
            if ocontext.auto_commit:
                # Java processConsumeResult:244-269
                if status in (ConsumeOrderlyStatus.COMMIT, ConsumeOrderlyStatus.ROLLBACK):
                    # Java:246-250 —— autoCommit=true 时 COMMIT/ROLLBACK 是非法用法
                    # （只给 binlog 消费用），Java 只警告、**不写 break**，顺势落进
                    # SUCCESS 分支：消息照 ack，不当成回滚
                    logger.warning("the message queue consume result is illegal, "
                                   "we think you want to ack these message %s", mq)
                    status = ConsumeOrderlyStatus.SUCCESS
                if status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=True)
                    # Java:256-266 —— 挂起之前先过 checkReconsumeTimes：只有"还在重试
                    # 次数内 / 回投失败"才原地重试；已经交给 broker 的（回投成功）要
                    # 前进位点，否则一条毒消息永久占住这条队列
                    if self._check_orderly_reconsume_times(batch):
                        self._requeue_pending(key, batch)
                        time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
                        return False
                else:
                    self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=False)
                self._advance_consume_offset(key, batch)
                return True
            # ---- autoCommit=False（Java:270-300，binlog 消费场景）----
            if status == ConsumeOrderlyStatus.COMMIT:
                # Java:275-277 —— 显式提交：位点前进，**不记 TPS**（RT 在分支外照记）
                self._record_consume_rt(mq.topic, obegin_ms)
                self._advance_consume_offset(key, batch)
                return True
            if status == ConsumeOrderlyStatus.ROLLBACK:
                # Java:278-285 —— rollback() 把消息退回 ProcessQueue 并延后重试
                self._record_consume_rt(mq.topic, obegin_ms)
                self._requeue_pending(key, batch)
                time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
                return False
            if status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=True)
                if self._check_orderly_reconsume_times(batch):
                    self._requeue_pending(key, batch)
                    time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
                # Java:288-296 —— 与自动提交分支的差别：毒消息交给 broker 后**不 commit**，
                # 位点前不前进由 binlog 消费方自己拿主意
                return False
            # SUCCESS + autoCommit=False：Java:272-274 只记 OK TPS、不提交。有意偏差：
            # Java 把消息留在 ProcessQueue.consumingMsgOrderlyTreeMap 里等显式 commit()，
            # 而四个端口都没把 ProcessQueue 暴露给 listener（没有 commit 的口子），
            # 照抄"什么都不做"会让这批消息被分发线程吞掉而位点又没动。这里等价地塞回
            # 队首并等一个挂起周期：位点同样不前进、消息不丢，也不会把消费线程变成忙等
            # （不 sleep 的话下一轮立刻又拿到同一批，DEFAULT 配置下就是 100% CPU 空转）。
            self._record_consume_stats(mq.topic, len(batch), obegin_ms, failed=False)
            self._requeue_pending(key, batch)
            time.sleep(self._orderly_suspend_millis(ocontext) / 1000.0)
            return False
        # ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
        context = ConsumeConcurrentlyContext(mq)
        hook_ctx = None
        if self.consume_message_hook_list:
            # 顺序与 Java 一致：before 钩子在 listener **之前**（用于生成 SubBefore 轨迹），
            # after 在拿到 status 之后（生成 SubAfter，带 contextCode）
            hook_ctx = self._build_consume_hook_context(batch, mq)
            self.execute_consume_hook_before(hook_ctx)
        begin_ms = time.time() * 1000
        has_exception = False
        try:
            status = listener.consume_message(batch, context)
        except Exception as e:  # noqa: BLE001
            # Java：消费抛异常按 RECONSUME_LATER 处理
            logger.debug("listener error, treat as RECONSUME_LATER: %s", e)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
            has_exception = True
        # Java:399-405 —— listener 返回 null 同样按 RECONSUME_LATER 处理（默认的 ackIndex
        # 只对 CONSUME_SUCCESS 有意义，null 落到下面就是整批回投）。归一化前的 status 要
        # 留给 returnType（返回 null 记 RETURNNULL，不是 FAILED）
        raw_status = status
        if status is None:
            logger.warning("consumeMessage return null, Group: %s Msgs: %d MQ: %s",
                           self.consumer_group, len(batch), mq)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
        # Java processConsumeResult:207-229 —— CONSUME_SUCCESS 用 listener 设的 ackIndex
        # 划分「已认可前缀 / 待回投后缀」（默认 Integer.MAX_VALUE，钳到 size-1 即整批认可）；
        # RECONSUME_LATER 强制 ackIndex=-1，整批回投。
        ack_index = context.ack_index
        if status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
            if ack_index >= len(batch):
                ack_index = len(batch) - 1
        else:
            ack_index = -1
        # 统计口径同 Java 的 ok/failed 计数（:217-225）：部分 ack 时尾巴算 failed
        self._record_consume_stats(mq.topic, len(batch), begin_ms,
                                   failed=status == ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                   ack_count=ack_index + 1)
        self._finish_consume_hook(
            hook_ctx, raw_status, has_exception, begin_ms,
            failed=status == ConsumeConcurrentlyStatus.RECONSUME_LATER,
            succeeded=status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
            hook_status=status)
        if broadcast:
            # Java:232-237 —— 广播模式不回投：未认可的尾巴只打一条 warn 就丢掉，
            # 整批位点照样前进（:266 的 removeMessage 拿到的就是整批）
            dropped = len(batch) - ack_index - 1
            if dropped > 0:
                logger.warning("BROADCASTING, the message consume failed, drop it: %d msgs in %s",
                               dropped, mq)
            self._advance_consume_offset(key, batch)
            return True
        if ack_index + 1 >= len(batch):
            # 整批认可（默认路径）：什么都不用回投，位点直接前进
            self._advance_consume_offset(key, batch)
            return True
        # 集群模式：未认可的 [ack_index+1, size) 逐条回投 %RETRY%topic（延迟梯度
        # 3+reconsumeTimes；超过 maxReconsumeTimes 由 broker 自动转 %DLQ%）
        msg_back_failed = self._send_back_batch(batch[ack_index + 1:], context)
        # Java:256-260 —— 回投失败的那几条从本批摘掉后 submitConsumeRequestLater 重投，
        # 这里等价地塞回队首稍后再消费
        if msg_back_failed:
            with self._lock:
                dq = self._pending.get(key)
                if dq is not None:
                    for m in reversed(msg_back_failed):
                        dq.appendleft(m)
            time.sleep(0.2)
        failed_ids = {id(m) for m in msg_back_failed}
        # Java:266-269 —— 提交的是「本批已处理条目里最大的 queueOffset + 1」，且不能越过
        # 仍留在 ProcessQueue 里的那几条（removeMessage 这时返回它们的最小 offset）
        self._advance_consume_offset(
            key, [m for m in batch if id(m) not in failed_ids],
            floor=min((m.queue_offset or 0) for m in msg_back_failed) if msg_back_failed else None)
        return not msg_back_failed

    def _send_back_batch(self, batch: List[MessageExt],
                         context: ConsumeConcurrentlyContext) -> List[MessageExt]:
        """把未认可的条目逐条回投 broker，返回回投失败的那些。

        对齐 Java ``ConsumeMessageConcurrentlyService#processConsumeResult:238-254``。
        失败条目按 Java``:251`` 就地 ``reconsumeTimes + 1`` —— broker 那边没记上这次数，
        客户端不补就永远进不了 DLQ。
        """
        failed: List[MessageExt] = []
        for msg in batch:
            try:
                # 重投次数在 MessageExt 线上格式第 13 字段（Java msg.getReconsumeTimes()），
                # broker 重投时会 +1；不是 properties 键（Java 的 PROPERTY_RECONSUME_TIME
                # 实际值是 "RECONSUME_TIME"，仅由 MessageAccessor.setReconsumeTime 写入）
                delay_level = context.delay_level_when_next_consume
                if not delay_level:
                    # Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
                    delay_level = 3 + msg.get_reconsume_times()
                self.send_message_back(msg, delay_level)
            except Exception as e:  # noqa: BLE001
                logger.debug("send message back failed for msg %s: %s", msg.msg_id, e)
                msg.set_reconsume_times(msg.get_reconsume_times() + 1)
                failed.append(msg)
        return failed

    # ---------------- 顺序消费的重试计数与回投 ----------------
    # 对位 Java ConsumeMessageOrderlyService 的 getMaxReconsumeTimes / checkReconsumeTimes
    # / sendMessageBack（:313-360），与上面并发那一套**不是同一条链路**，两处差异都要守住：
    #   1. `-1` 在顺序侧是「不设限」（Integer.MAX_VALUE），在并发侧才是 16；
    #   2. 顺序侧的回投是**普通消息发送**（发到 %RETRY%<group>），不是 CONSUMER_SEND_MSG_BACK(3)。

    def _orderly_suspend_millis(self, context: ConsumeOrderlyContext) -> int:
        """Java ``ConsumeMessageOrderlyService#submitConsumeRequestLater:211-234``。

        先解析 ``-1``（``ConsumeOrderlyContext.suspendCurrentQueueTimeMillis`` 的默认值，
        意思是"没指定"）→ 回落到消费者配置 ``suspendCurrentQueueTimeMillis``（默认 1000），
        再把结果**钳到 [10, 30000]**。这个钳位不是装饰：listener 传 0 时 Java 仍然等 10ms
        （否则一个返回挂起的 listener 会把消费线程变成忙等），传 1 小时也只等 30s。
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
        """Java ConsumeMessageOrderlyService#getMaxReconsumeTimes:313-320。

        顺序消费没有 broker 侧的重投计数（消息一直停在本地队列里原地重试），所以默认
        就该一直重试到成功为止 —— `-1` 在这里读成 `Integer.MAX_VALUE`。并发消费的
        `-1 → 16`（`DefaultMQPushConsumerImpl#getMaxReconsumeTimes`）是另一套语义：那边
        每轮回投都要过一遍 broker，16 是 broker 的默认 retryMaxTimes。把两者并成一个
        常量，等于要么给顺序消费凭空造出死信，要么让并发消息无限重投。
        """
        if self.max_reconsume_times == -1:
            return _JAVA_INT_MAX
        return self.max_reconsume_times

    def _check_orderly_reconsume_times(self, msgs: Optional[List[MessageExt]]) -> bool:
        """Java ConsumeMessageOrderlyService#checkReconsumeTimes:322-336。

        返回「这一批是否还要原地挂起重试」。逐条两种走法：
          - 次数没用尽：本地 `reconsumeTimes + 1`（broker 那边压根没记，客户端不补就
            永远到不了阈值），继续挂起；
          - 次数已用尽：交给 broker 回投。**回投成功就不再挂起**（Java 此时 commit 位点，
            毒消息让路，队列继续往前），回投失败才 +1 并挂起。
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
        """Java ConsumeMessageOrderlyService#sendMessageBack:338-360。

        拿实例自带的内部生产者，把这条消息**当普通消息**发到 `%RETRY%<group>`：
        broker 的 `handleRetryAndDLQ`（`SendMessageProcessor:199-234`）见该组还持有未过期的
        队列锁（正是顺序消费组的特征），直接把它改投 `%DLQ%<group>`。属性置法逐条照抄
        Java，其中 `RECONSUME_TIME` / `MAX_RECONSUME_TIMES` 会被发送侧抬进请求头
        （`mq_client._build_send_request`，Java `sendKernelImpl:1004-1018`）。

        失败只返回 false、绝不抛：Java 整段包在 try/catch 里，抛给消费线程等于这条既没
        ack 也没回投，只能等锁超时。
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
            # 半消息标记必须清掉，否则 broker 会把它再当事务回查消息处理
            MessageAccessor.clear_property(new_msg,
                                           MessageConst.PROPERTY_TRANSACTION_PREPARED)
            new_msg.set_delay_time_level(3 + msg.get_reconsume_times())
            publish = client.get_topic_publish_info(new_msg.topic, is_default=True)
            mq = publish.select_one_message_queue() if publish is not None else None
            if mq is None:
                raise MQClientException("no writable queue for retry topic %s" % new_msg.topic)
            # unitMode 跟着消费者：Java 构造内部生产者时调过 resetClientConfig(clientConfig)，
            # 不带的话重投出去的消息会丢单元标记。
            client.send_message(MixAll.CLIENT_INNER_PRODUCER_GROUP, new_msg, mq, 3000,
                                unit_mode=self.unit_mode)
            return True
        except Exception as e:  # noqa: BLE001
            logger.debug("orderly send message back failed, group=%s msg=%s: %s",
                         self.consumer_group, msg.msg_id, e)
            return False

    def _advance_consume_offset(self, key: str, batch: List[MessageExt],
                                floor: Optional[int] = None) -> None:
        if not batch:
            # 整批回投都失败时没有任何条目被认可，位点原地不动
            return
        next_off = max((m.queue_offset or 0) for m in batch) + 1
        if floor is not None:
            next_off = min(next_off, floor)
        with self._lock:
            cur = self._consume_offsets.get(key)
            self._consume_offsets[key] = max(cur or 0, next_off)

    # ---------------- 位点持久化 ----------------
    def _start_offset_persist_loop(self) -> None:
        t = threading.Thread(target=self._offset_persist_loop, daemon=True,
                             name="rmq-offset-persist-%s" % self.consumer_group)
        t.start()
        self._persist_thread = t

    def _offset_persist_loop(self) -> None:
        # Java MQClientInstance.startScheduledTask:417-423：
        #   scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10, persistConsumerOffsetInterval)
        # ——首个任务延迟 10s，之后的周期取 clientConfig.persistConsumerOffsetInterval（默认 5s）。
        # 与 Java 一致：这两步都只在 start() 时读一次，运行期改值不改变已排定的节奏。
        if self._stop.wait(10.0):    # Java initialDelay = 1000 * 10
            return
        # 周期在循环入口取一次（Java 排定的是固定周期，运行期改字段不改变已排定的任务）。
        # 首笔落盘发生在 initialDelay 之后，而不是 initialDelay + 一个周期后。
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
        # Java LocalFileOffsetStore：$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
        base = os.path.join(os.path.expanduser("~"), ".rocketmq_offsets",
                            self.client_id or "DEFAULT", self.consumer_group)
        return os.path.join(base, "offsets.json")

    def _save_local_offsets(self) -> None:
        with self._lock:
            items = dict(self._consume_offsets)
        path = self._local_offset_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f)
        os.replace(tmp, path)

    def _load_local_offsets(self) -> Dict[str, int]:
        try:
            with open(self._local_offset_path(), "r", encoding="utf-8") as f:
                return {k: int(v) for k, v in json.load(f).items()}
        except (OSError, ValueError):
            return {}

    # ---------------- 顺序消费队列锁 ----------------
    def _start_lock_loop(self) -> None:
        if not self._is_orderly() or self.message_model == MessageModel.BROADCASTING:
            return
        t = threading.Thread(target=self._lock_loop, daemon=True,
                             name="rmq-lock-%s" % self.consumer_group)
        t.start()
        self._lock_thread = t

    def _lock_loop(self) -> None:
        client = self._require_client()
        # Java ConsumeMessageOrderlyService.lockMQ：每 20s 批量锁分到的队列；
        # 启动时立刻尝试一次，避免首个 20s 空转
        while not self._stop.is_set():
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

    # ---------------- 管理能力 ----------------
    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return [MessageQueue(q.topic, q.broker_name, q.queue_id) for q in publish.msg_queue_list]

    def send_message_back(self, msg: MessageExt, delay_level: int,
                          broker_name: Optional[str] = None) -> None:
        """消息重投（对应 Java sendMessageBack）。"""
        client = self._require_client()
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import ConsumerSendMsgBackRequestHeader
        from ..remoting.protocol.remoting_command import RemotingCommand
        if broker_name is None:
            broker_name = msg.broker_name
        addr = client.broker_addr_of(broker_name)
        if addr is None:
            raise MQClientException("broker %s not found" % broker_name)
        # Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
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
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)


class DefaultMQPullConsumer:
    """拉模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPullConsumer）。"""

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id: Optional[str] = None
        # ---- unit / stream 配置（对应 Java ClientConfig 的同名字段）----
        # unitName 参与 clientId 的 `@<unitName>` 后缀与动态取址 URL；unitMode 随
        # ConsumerData 心跳、消息回投请求头和过滤钩子上报给 broker；
        # enableStreamRequestType 既给 clientId 加 `@STREAM` 后缀（Java 的注释写明是
        # 为了"prevent unexpected reuses of MQClientInstance"），也给每个请求加
        # `ReqT=0` 扩展字段。Java 的 DefaultMQPullConsumer 每个构造函数都写 `enableStreamRequestType = true`（:113/:126）。
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = True
        self.message_model = message_model
        self.broker_suspend_max_time_millis = 20000
        self.consumer_pull_timeout_millis = 10000
        self.consumer_timeout_millis_when_suspend = 30000
        # 路由刷新周期（对应 Java ClientConfig.pollNameServerInterval 默认 30000ms）；
        # 只在 start() 建 MQClientInstance 时透传一次。
        self.poll_name_server_interval = 30000
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self.register_topics: Set[str] = set()
        self.message_queue_lists: List[MessageQueue] = []
        self.message_queue_listener: Optional[MessageQueueListener] = None
        # 队列分配策略，对应 Java DefaultMQPullConsumer.allocateMessageQueueStrategy
        # （字段初值 new AllocateMessageQueueAveragely():89，getter/setter:196-202）。
        # 本端口拉模式由调用方自己管队列，所以它只作为配置存在并被 start() 校验，
        # 不像 Java 那样注入 RebalancePullImpl（本端口没有拉模式后台重平衡）。
        self.allocate_message_queue_strategy: Optional[AllocateMessageQueueStrategy] = \
            AllocateMessageQueueAveragely()
        # 投递前过滤钩子（Java DefaultMQPullConsumerImpl.filterMessageHookList:80，
        # start() 时注册进 PullAPIWrapper:726）
        self.filter_message_hook_list: List[FilterMessageHook] = []
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False

    # ---------------- 投递前过滤钩子 ----------------
    def register_filter_message_hook(self, hook: FilterMessageHook) -> None:
        """注册投递前过滤钩子（Java DefaultMQPullConsumerImpl.registerFilterMessageHook:844）。"""
        if hook is not None:
            self.filter_message_hook_list.append(hook)

    def has_filter_message_hook(self) -> bool:
        return len(self.filter_message_hook_list) > 0

    def execute_filter_message_hook(self, context: FilterMessageContext) -> None:
        execute_filter_hooks(self.filter_message_hook_list, context)

    def _filter_messages_for_delivery(self, mq: MessageQueue, msgs: List[MessageExt]) -> List[MessageExt]:
        """拉模式也要过过滤钩子（Java pullSyncImpl → pullAPIWrapper.processPullResult）。

        拉模式的 tag 过滤交给调用方（Java 的 pull 接口不传 subscriptionData 时
        processPullResult 里 tagsSet 为空 → 不筛），这里只跑钩子。
        """
        if not msgs or not self.filter_message_hook_list:
            return msgs
        context = FilterMessageContext(self.consumer_group, list(msgs), mq)
        context.unit_mode = self.unit_mode
        self.execute_filter_message_hook(context)
        return list(context.msg_list)

    # ---------------- 配置 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        """对应 Java `ClientConfig#setUnitName`：影响 clientId 后缀与动态取址 URL。"""
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        """对应 Java `ClientConfig#setUnitMode`：随心跳/请求头透传给 broker。"""
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        """对应 Java `ClientConfig#setEnableStreamRequestType`。"""
        self.enable_stream_request_type = bool(enable)

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_message_queue_listener(self, listener: MessageQueueListener) -> None:
        self.message_queue_listener = listener

    def set_allocate_message_queue_strategy(
            self, strategy: Optional[AllocateMessageQueueStrategy]) -> None:
        """对应 Java DefaultMQPullConsumer.setAllocateMessageQueueStrategy(:200)：
        setter 不校验，置 None 由 start() 拒绝。"""
        self.allocate_message_queue_strategy = strategy

    def get_register_topics(self) -> Set[str]:
        return self.register_topics

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        if self._started:
            return
        # 对应 Java DefaultMQPullConsumerImpl.checkConfig（:772）——纯本地校验，
        # 排在建立客户端实例之前，非法组名不需要等到连不上才报。
        validators.check_group(self.consumer_group)
        if self.consumer_group == MixAll.DEFAULT_CONSUMER_GROUP:
            raise MQClientException(
                "consumerGroup can not equal %s, please specify another one."
                % MixAll.DEFAULT_CONSUMER_GROUP)
        if not self.name_server_addrs:
            raise MQClientException("name server address is not set")
        # 对应 Java DefaultMQPullConsumerImpl.checkConfig（:803）：策略为 None 直接拒绝启动。
        if self.allocate_message_queue_strategy is None:
            raise MQClientException("allocateMessageQueueStrategy is null")
        # Java `DefaultMQPullConsumerImpl#start`:712-714：CLUSTERING 才改写 instanceName，
        # clientId 口径是 `ClientConfig#buildMQClientId` 的
        # `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`。
        if self.message_model == MessageModel.CLUSTERING:
            self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
        if self.client_id is None:
            self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                  self.enable_stream_request_type)
        self._mq_client = MQClientInstance(self.client_id, self.name_server_addrs,
                                           enable_stream_request_type=self.enable_stream_request_type,
                                           unit_name=self.unit_name,
                                           poll_name_server_interval=self.poll_name_server_interval)
        if self.rpc_hook is not None:
            self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
        self._mq_client.start()
        self._started = True

    def shutdown(self) -> None:
        if not self._started:
            return
        self._started = False
        if self._mq_client is not None:
            self._mq_client.shutdown()

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        return self._mq_client

    # ---------------- 拉取 ----------------
    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return [MessageQueue(q.topic, q.broker_name, q.queue_id) for q in publish.msg_queue_list]

    def pull(self, mq: MessageQueue, sub_expression: str = "*", offset: int = 0,
             max_nums: int = 32, timeout_millis: Optional[int] = None) -> PullResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.consumer_pull_timeout_millis
        sub = FilterAPI.build_subscription_data(mq.topic, sub_expression)
        # 对齐 Java DefaultMQPullConsumerImpl.pullSyncImpl（:248）：
        #   sysFlag = PullSysFlag.buildSysFlag(false, block, true, false)
        # 即 commit_offset=False、suspend=block。**pull() 的 block=false** ——
        # 位点由调用方自己 update_consume_offset 提交，且这是**短轮询**不挂起。
        # ⚠ 曾在这里写成 suspend=True：broker 在队尾会挂起到 brokerSuspendMaxTimeMillis
        # （默认 20s），而客户端 5s 就超时 → RemotingTimeoutException（真机必现）。
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                              subscription=True, class_filter=False)
        result = client.pull_message(self.consumer_group, mq, offset, max_nums,
                                     sys_flag, 0, sub.sub_string or "*",
                                     # Java：TAG 类型时 subVersion 传 0（isTagType ? 0L : subVersion）
                                     0,
                                     ExpressionType.TAG, timeout_millis=timeout,
                                     max_msg_bytes=-1, suspend_timeout_millis=15000)
        if result.status == PullStatus.FOUND and result.msg_found_list:
            result.msg_found_list = self._filter_messages_for_delivery(mq, result.msg_found_list)
        return result

    def pull_block_if_not_found(self, mq: MessageQueue, sub_expression: str, offset: int,
                                max_nums: int) -> PullResult:
        """长轮询拉取（对应 Java pullBlockIfNotFound，block=true → 挂起等消息）。"""
        client = self._require_client()
        sub = FilterAPI.build_subscription_data(mq.topic, sub_expression)
        # block=true：suspend=True 让 broker 挂起到有消息；超时用
        # consumer_timeout_millis_when_suspend（Java :250 的 `block ? ... : timeout`）。
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                              subscription=True, class_filter=False)
        result = client.pull_message(self.consumer_group, mq, offset, max_nums,
                                     sys_flag, 0, sub.sub_string or "*", 0,
                                     ExpressionType.TAG,
                                     timeout_millis=self.consumer_timeout_millis_when_suspend,
                                     max_msg_bytes=-1,
                                     suspend_timeout_millis=self.broker_suspend_max_time_millis)
        if result.status == PullStatus.FOUND and result.msg_found_list:
            result.msg_found_list = self._filter_messages_for_delivery(mq, result.msg_found_list)
        return result

    # ---------------- Offset 管理 ----------------
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
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import (GetEarliestMsgStoretimeRequestHeader,
                                                 GetEarliestMsgStoretimeResponseHeader)
        from ..remoting.protocol.remoting_command import RemotingCommand
        addr = client._broker_addr(mq)
        header = GetEarliestMsgStoretimeRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_EARLIEST_MSG_STORETIME, header)
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)
        resp_header = GetEarliestMsgStoretimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.timestamp or 0

    def send_message_back(self, msg: MessageExt, delay_level: int) -> None:
        """消息回投（对应 Java DefaultMQPullConsumer.sendMessageBack）。

        注意两点（真机踩过）：
        1. 地址靠 `broker_addr_of(msg.broker_name)` 反查**路由表**，所以调用方必须先用本
           consumer 访问过该 topic（Java 同理，走 findBrokerAddressInPublish 读 brokerAddrTable）。
        2. 与 Java 的**有意差异**：Java 在失败时会吞掉异常、改用内部默认生产者把消息直接发到
           `%RETRY%group`（见 DefaultMQPullConsumerImpl:666 的 catch 分支）。本实现不做这个
           兜底 —— 回投失败就抛，让调用方看见，而不是换一条路径静默重发。
        """
        client = self._require_client()
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import ConsumerSendMsgBackRequestHeader
        from ..remoting.protocol.remoting_command import RemotingCommand
        addr = client.broker_addr_of(msg.broker_name or "")
        if addr is None:
            raise MQClientException("broker %s not found" % msg.broker_name)
        header = ConsumerSendMsgBackRequestHeader()
        header.offset = msg.commit_log_offset
        header.group = self.consumer_group
        header.delay_level = delay_level
        header.origin_msg_id = msg.msg_id
        header.origin_topic = msg.topic
        header.unit_mode = self.unit_mode
        # ⚠ 这里**不能**照抄 Java 弃用的 DefaultMQPullConsumerImpl#sendMessageBack
        # （它直接传 getMaxReconsumeTimes()，默认 -1）。客户端版本 ≥ V3_4_9 后 broker
        # 会无条件采用该字段（AbstractSendMessageProcessor:172-179），-1 会让
        # reconsumeTimes(0) >= -1 成立、消息直接进 %DLQ%。留 None 交给订阅组的
        # retryMaxTimes 判定。
        header.max_reconsume_times = None
        request = RemotingCommand.create_request_command(RequestCode.CONSUMER_SEND_MSG_BACK, header)
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)

    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        client.create_topic_in_route(new_topic, queue_num, queue_num, 6)


class DefaultLitePullConsumer:
    """轻量拉取消费者（对应 org.apache.rocketmq.client.consumer.DefaultLitePullConsumer）。

    与 DefaultMQPullConsumer 的本质区别：
    - DefaultMQPullConsumer：调用方自己 ``pull(mq, offset)`` 逐队列拉、自己管位点；没有本地缓冲、
      没有后台拉取线程、不做 rebalance。
    - DefaultLitePullConsumer：支持两种模式——
      * **subscribe 模式**：登记订阅后自动 rebalance 分配队列（与 push 一致），后台拉取线程把
        消息灌进**本地缓冲**；``poll()`` 只从本地缓冲取消息，不用调用方管位点；
      * **assign 模式**：调用方 ``assign([mq...])`` 显式指定队列，不走 rebalance，同样后台灌本地缓冲。
    两种模式都用 ``poll(timeout)`` 取批量消息；位点默认 autoCommit（拉完即向 broker 提交）。

    设计取舍（与既有三门语言实现一致）：
    - 后台**单个** pull 服务线程顺序遍历所有已分配队列做短轮询（suspend=False），把消息塞进
      一个线程安全的本地缓冲 ``_local_buffer``；``poll()`` 用 Condition 等待并 drain 该缓冲。
      不按队列起独立线程（与 Java 的 PullTask 不同，但语义等价：本地缓冲 + poll）。
    - subscribe 模式的 rebalance 复用既有 ``get_consumer_id_list_by_group`` + ``AllocateMessageQueueAveragely``，
      与 push 消费者同一套分配算法；查询不到消费组列表时按 Java 语义「保留当前分配」，不回退独占。
    - 不做 POP / 推模式；不做 broker 主动请求（309/313）处理（那是 push 消费者的职责）。
    """

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id: Optional[str] = None
        # ---- unit / stream 配置（对应 Java ClientConfig 的同名字段）----
        # unitName 参与 clientId 的 `@<unitName>` 后缀与动态取址 URL；unitMode 随
        # ConsumerData 心跳、消息回投请求头和过滤钩子上报给 broker；
        # enableStreamRequestType 既给 clientId 加 `@STREAM` 后缀（Java 的注释写明是
        # 为了"prevent unexpected reuses of MQClientInstance"），也给每个请求加
        # `ReqT=0` 扩展字段。Java 的 DefaultLitePullConsumer 每个构造函数都写 `enableStreamRequestType = true`（:213/:228）。
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = True
        self.message_model = message_model
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook

        # subscribe 模式的订阅表（topic -> sub_expression）
        self.subscription: Dict[str, str] = {}
        # 订阅对应的 SubscriptionData（带 tagsSet），用于发给 broker 的心跳做 tag 过滤注册
        self.subscription_data: Dict[str, SubscriptionData] = {}
        # assign 模式：调用方显式指定队列时的 tag 过滤表达式（透传给 pull）
        self._assign_sub_expr: Dict[str, str] = {}
        self._assign_mode = False
        self._assigned: Set[MessageQueue] = set()

        # rebalance 算法（subscribe 模式）
        self.allocate_message_queue_strategy = AllocateMessageQueueAveragely()

        # 拉取 / poll 配置
        self.consume_from_where = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        # Java DefaultLitePullConsumer.consumeTimestamp 字段初值：now - 30 分钟。
        # 留空会让 CONSUME_FROM_TIMESTAMP 退化成「从当前时刻起消费」。
        self.consume_timestamp = time.strftime(
            "%Y%m%d%H%M%S", time.localtime(time.time() - 30 * 60))
        self.pull_batch_size = 32
        self.poll_timeout_millis = 5000
        # 路由刷新周期（对应 Java ClientConfig.pollNameServerInterval 默认 30000ms）；
        # 只在 start() 建 MQClientInstance 时透传一次。
        self.poll_name_server_interval = 30000
        self.auto_commit = True
        self.auto_commit_interval_millis = 5000
        self.consumer_timeout_millis_when_suspend = 30000
        self.broker_suspend_max_time_millis = 20000
        self.pull_interval_millis = 50  # 队尾空轮询时的退避，避免空转打爆 broker
        self.pull_thread_nums = 1

        # 运行状态
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._running = False
        self._pull_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

        # 本地缓冲 + 位点游标
        self._local_buffer: Deque[MessageExt] = deque()
        self._buffer_lock = threading.Lock()
        self._buffer_cond = threading.Condition(self._buffer_lock)
        # 拉取游标：对位 Java AssignedMessageQueue.MessageQueueState.pullOffset，
        # 只表示"已经取到本地缓冲的最后一条之后"，由 _pull_one 前进。
        self._next_offset: Dict[MessageQueue, int] = {}
        # 已消费游标：对位同一处的 consumeOffset，只有 poll() 把消息真交到调用方手上才前进
        # （Java poll() 里的 assignedMessageQueue.updateConsumeOffset(mq, removeMessage(msgs))）。
        # 与拉取游标分成两张表，才谈得上 commit(Map, persist)：调用方指定"提交到哪"时
        # 绝不能顺手改掉"下次从哪拉"。
        self._consume_offset: Dict[MessageQueue, int] = {}
        # 提交落点：对位 Java OffsetStore 的内存位点表（RemoteBrokerOffsetStore.offsetTable）。
        # commit* 三个入口都先写这张表，persist=true 才把它发给 broker；committed() 也先读它
        # （Java 的 ReadOffsetType.MEMORY_FIRST_THEN_STORE）。
        self._offset_table: Dict[MessageQueue, int] = {}
        self._seek_offset: Dict[MessageQueue, int] = {}
        # Java DefaultLitePullConsumerImpl.nextAutoCommitDeadline：初值 -1 ⇒ 第一次 poll 就提交一次。
        self._next_auto_commit_deadline = -1
        self._paused: Set[MessageQueue] = set()
        self._message_queue_listener = None
        self._last_rebalance_ts = 0

    # ---------------- 命名空间 ----------------
    def _with_namespace(self, topic: str) -> str:
        if self.namespace and not topic.startswith(self.namespace + "%"):
            return self.namespace + "%" + topic
        return topic

    # ---------------- 配置 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        """对应 Java `ClientConfig#setUnitName`：影响 clientId 后缀与动态取址 URL。"""
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        """对应 Java `ClientConfig#setUnitMode`：随心跳/请求头透传给 broker。"""
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        """对应 Java `ClientConfig#setEnableStreamRequestType`。"""
        self.enable_stream_request_type = bool(enable)

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

    def set_allocate_message_queue_strategy(self, strategy) -> None:
        self.allocate_message_queue_strategy = strategy

    def set_message_queue_listener(self, listener) -> None:
        self._message_queue_listener = listener

    # ---------------- 订阅 / 分配 ----------------
    def subscribe(self, topic: str, sub_expression: str = "*") -> None:
        self._assign_mode = False
        ns = self._with_namespace(topic)
        self.subscription[ns] = sub_expression
        # 同步构建 SubscriptionData，供心跳把 tag 订阅注册给 broker（否则 broker 不认 tag 过滤）
        try:
            self.subscription_data[ns] = FilterAPI.build_subscription_data(ns, sub_expression)
        except Exception:
            self.subscription_data.pop(ns, None)

    def subscribe_with_selector(self, topic: str, selector) -> None:
        # Lite 仅支持 tag 表达式订阅；MessageSelector 一律按 tag 处理
        self.subscribe(topic, getattr(selector, "expression", "*"))

    def unsubscribe(self, topic: str) -> None:
        ns = self._with_namespace(topic)
        self.subscription.pop(ns, None)
        self.subscription_data.pop(ns, None)

    def set_sub_expression_for_assign(self, topic: str, sub_expression: str) -> None:
        """assign 模式下给某个 topic 的队列指定 tag 过滤表达式（对应 Java setSubExpressionForAssign）。

        同时构建 SubscriptionData 注册到心跳，使 broker 按该 tag 过滤（assign 模式 broker 也需要订阅）。
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
        # Java assignedMessageQueue.updateAssignedMessageQueue：撤掉的队列连着整份
        # MessageQueueState 丢掉（拉取游标与已消费游标一起消失）。**不**动 offsetStore 的
        # 内存位点表——那份清理挂在 rebalance 的 removeUnnecessaryMessageQueue 上，
        # assign 模式不走 rebalance，所以 persist 也发生在这里之外（Java 同）。
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

    # ---------------- 生命周期 ----------------
    def _create_client(self) -> MQClientInstance:
        return MQClientInstance(self.client_id, self.name_server_addrs,
                                enable_stream_request_type=self.enable_stream_request_type,
                                unit_name=self.unit_name,
                                poll_name_server_interval=self.poll_name_server_interval)

    def start(self) -> None:
        if self._started:
            return
        # 对应 Java DefaultLitePullConsumerImpl.checkConfig（:415）：纯本地校验，最先做
        validators.check_group(self.consumer_group)
        if self.consumer_group == MixAll.DEFAULT_CONSUMER_GROUP:
            raise MQClientException(
                "consumerGroup can not equal %s, please specify another one."
                % MixAll.DEFAULT_CONSUMER_GROUP)
        if not self.name_server_addrs:
            raise MQClientException("name server address is not set")
        if not self.subscription and not self._assign_mode:
            raise MQClientException("subscription is not set, call subscribe() or assign() first")
        # 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1058）：启动即无条件校验。
        # 下面解析初始位点的循环会吞异常，晚抛等于静默退化成从 max offset 消费。
        self._parse_consume_timestamp(self.consume_timestamp)
        # 对应 Java DefaultLitePullConsumerImpl.checkConfig（:435）：策略为 None 直接拒绝启动，
        # 而不是等 rebalance 里 None.allocate 抛 AttributeError 被静默吞掉。
        if self.allocate_message_queue_strategy is None:
            raise MQClientException("allocateMessageQueueStrategy is null")
        # Java `DefaultLitePullConsumerImpl#start`:287-289：CLUSTERING 才改写 instanceName，
        # clientId 口径是 `ClientConfig#buildMQClientId` 的
        # `<本机 IP>@<instanceName>[@<unitName>][@STREAM]`。
        if self.message_model == MessageModel.CLUSTERING:
            self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
        if self.client_id is None:
            self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                  self.enable_stream_request_type)
        self._mq_client = self._create_client()
        if self.rpc_hook is not None:
            self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
        self._mq_client.start()
        # assign 模式在 start 时解析初始位点
        if self._assign_mode:
            for mq in list(self._assigned):
                if mq not in self._next_offset:
                    try:
                        self._next_offset[mq] = self._resolve_initial_offset(mq)
                    except Exception:
                        logger.debug("start: resolve initial offset failed for %s", mq)
        # 先把 tag 订阅注册给 broker（心跳），再启动后台拉取，避免首轮拉取因 broker 不认订阅而丢消息。
        # 心跳要靠「已知 broker 列表」发送，而该列表只来自 topic 路由表：start 时先把订阅/指派的
        # topic 拉一遍路由并登记为「在用」（对应 Java sendHeartbeatToAllBrokerWithLock 之前必然先
        # updateTopicRouteInfoFromNameServer）。少了这步，首轮心跳因为没有 broker 而静默不发 ⇒
        # broker 侧看不到本实例 ⇒ 多实例 rebalance 各自独占全部队列（互相重复消费）。
        self._refresh_route_for_heartbeat()
        self._running = True
        self._send_heartbeat_to_all_broker()
        self._start_heartbeat_loop()
        self._started = True
        self._pull_thread = threading.Thread(
            target=self._pull_service_loop, daemon=True,
            name="rmq-lite-pull-%s" % self.consumer_group)
        self._pull_thread.start()

    def _refresh_route_for_heartbeat(self) -> None:
        """为心跳准备 broker 地址：拉取并登记本实例关注的 topic 路由。"""
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
        # Java 的 shutdown 走 persistConsumerOffset()：把内存位点表按当下持有的队列刷一遍，
        # 与 auto_commit 无关（手动模式用 persist=False 攒下的值同样要落盘）。
        # 自动提交模式再多走一步 commit()：本端口没有 Java 那份 5s 定时器，
        # "poll 交出去但还没到截止时刻"的位点得在这里补上，否则重启后从上一格重投。
        try:
            if self.auto_commit:
                self.commit()
            else:
                self._persist_offset_table(self._assigned)
        except Exception:  # noqa: BLE001
            logger.debug("shutdown commit failed")
        # 唤醒可能的 poll() 等待，让其在关闭后尽快返回
        with self._buffer_cond:
            self._buffer_cond.notify_all()
        if self._pull_thread is not None:
            self._pull_thread.join(timeout=2.0)
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2.0)
        if self._mq_client is not None:
            self._mq_client.shutdown()

    def is_running(self) -> bool:
        return self._running

    # ---------------- 心跳（把 tag 订阅注册给 broker）----------------
    def _build_heartbeat(self) -> HeartbeatData:
        hb = HeartbeatData(self.client_id or "")
        cd = ConsumerData(self.consumer_group, ConsumeType.CONSUME_PASSIVELY,
                          self.message_model, self.consume_from_where)
        # 见推送消费者 _build_heartbeat 的同名注释
        cd.unit_mode = self.unit_mode
        for sub in self.subscription_data.values():
            cd.subscription_data_set.add(sub)
        hb.consumer_data_set.add(cd)
        return hb

    def _send_heartbeat_to_all_broker(self) -> int:
        if self._mq_client is None:
            return 0
        hb = self._build_heartbeat()
        ok = 0
        for addr in self._mq_client.get_route_of_all_brokers():
            try:
                self._mq_client.send_heartbeat(addr, hb, 5000)
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
            # 5s 心跳间隔（与 push 消费者一致）
            for _ in range(50):
                if not self._running:
                    break
                time.sleep(0.1)

    # ---------------- 拉取服务 ----------------
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
            # 退避：轮询空时放慢，避免打爆 broker；有消息则尽快回填缓冲
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
        # 短轮询（suspend=False），位点由 auto-commit 单独提交（与 Java LitePull 一致）
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                              subscription=True, class_filter=False)
        try:
            result = self._mq_client.pull_message(
                self.consumer_group, mq, offset, self.pull_batch_size,
                sys_flag, 0, sub, 0, ExpressionType.TAG,
                timeout_millis=30000, max_msg_bytes=-1,
                suspend_timeout_millis=15000)
        except Exception as e:  # noqa: BLE001
            logger.debug("lite pull_one failed for %s@%d: %s", mq.topic, mq.queue_id, e)
            return False
        if result.status == PullStatus.FOUND and result.msg_found_list:
            msgs = self._filter_tags(mq.topic, result.msg_found_list, sub)
            if msgs:
                self._enqueue(msgs)
                last = msgs[-1]
                self._next_offset[mq] = last.queue_offset + 1
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
            # 注意：真实拉取回来的 MessageExt 把 tag 放在 properties["TAGS"]（get_tags() 读它），
            # 实例属性 .tags 为 None；单元测试里会显式 set_tags，这里两种来源都兼容。
            tag = getattr(m, "tags", None) or m.get_tags()
            if tag in sub_data.tags_set:
                kept.append(m)
        return kept

    def _enqueue(self, msgs: List[MessageExt]) -> None:
        with self._buffer_cond:
            self._local_buffer.extend(msgs)
            self._buffer_cond.notify_all()

    def _maybe_auto_commit(self) -> None:
        """对位 Java ``DefaultLitePullConsumerImpl#maybeAutoCommit``：到点提交**全部**已分配队列。

        调用点和 Java 一样只有 ``poll()`` 开头一处（外加 ``shutdown()`` 的兜底提交）：
        Java 里空闲消费者靠 MQClientInstance 每 5s 的 ``persistConsumerOffset`` 定时器刷的
        是**内存位点表**，而那张表也只有 ``commit`` 路径会写，所以停掉 poll 之后 Java 同样
        不会往前推进 broker 位点。这里若偷放在拉取循环里查，就成了"没人 poll 也提交"，
        比 Java 激进，故不放。

        提交的是"已消费游标"而不是拉取游标——缓冲里还没交给调用方的消息不算已消费，
        这和 Java 的 ``processQueue.removeMessage`` 只在 poll 里前进 consumeOffset 同口径。
        """
        now = time.time() * 1000.0
        if now < self._next_auto_commit_deadline:
            return
        self._next_auto_commit_deadline = now + self.auto_commit_interval_millis
        self.commit()

    def _resolve_initial_offset(self, mq: MessageQueue) -> int:
        if mq in self._seek_offset:
            return self._seek_offset[mq]
        # 有已提交位点则沿用（保证重启续消费）
        try:
            off = self._mq_client.query_consumer_offset(self.consumer_group, mq)
            if off is not None:
                return off
        except Exception:
            pass
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
            return self._mq_client.get_min_offset(mq)
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
            ts = self._parse_consume_timestamp(self.consume_timestamp)
            return self._mq_client.search_offset_by_timestamp(mq, ts)
        return self._mq_client.get_max_offset(mq)

    def _parse_consume_timestamp(self, ts: str) -> int:
        # 对应 Java UtilAll.parseDate(ts, UtilAll.YYYYMMDDHHMMSS)：该字段**只**是 14 位本地
        # 墙钟日期。旧实现先走 isdigit() 分支把纯数字当 epoch 秒/毫秒，"20230101000000"
        # 因此被解释成公元 2611 年，而下面的 strptime 分支永远不可达。
        try:
            if len(str(ts)) != 14:
                raise ValueError(ts)
            return int(time.mktime(time.strptime(str(ts), "%Y%m%d%H%M%S")) * 1000)
        except (ValueError, TypeError, OverflowError):
            raise MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received %s" % ts)

    # ---------------- rebalance（subscribe 模式）----------------
    def _rebalance(self) -> None:
        # 对应 Java DefaultLitePullConsumerImpl.rebalance → RebalanceImpl.doRebalance：
        # 走的是与 push **完全相同**的 rebalanceByTopic 路径，所以 mqAll / cidAll 都要先排序。
        # 顺序不一致会让同组不同实例算出互相冲突的分配（同一队列被两个实例同时消费）。
        new_set: Set[MessageQueue] = set()
        for topic in list(self.subscription.keys()):
            try:
                info = self._mq_client.get_topic_publish_info(topic)
                mq_all = sorted([MessageQueue(q.topic, q.broker_name, q.queue_id)
                                 for q in info.msg_queue_list], key=_mq_sort_key)
            except Exception:
                mq_all = []
            cid_all = sorted(self._mq_client.get_consumer_id_list_by_group(topic, self.consumer_group) or [])
            if self.client_id not in cid_all:
                cid_all = sorted(cid_all + [self.client_id])
            try:
                allocated = self.allocate_message_queue_strategy.allocate(
                    self.consumer_group, self.client_id, mq_all, cid_all)
            except Exception as e:  # noqa: BLE001
                # 对应 Java RebalanceImpl.rebalanceByTopic 的 catch (Throwable)：只记日志、
                # 本轮跳过该 topic（保留它当前的分配），绝不能因为一次策略异常就把队列撤走。
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
                # Java RebalanceLitePullImpl#removeUnnecessaryMessageQueue：先 persist(mq)
                # 再 removeOffset(mq)——撤手之前把最后一次提交的位点补发出去，
                # 别让已经消费过的队列从上一个 5s 窗口起重新投一遍。
                self._persist_offset(mq)
                self._next_offset.pop(mq, None)
                # AssignedMessageQueue 的条目也一起丢掉：Java 撤队列时 removeAssignMessageQueue
                # 丢掉的是**整份 MessageQueueState**（pullOffset / consumeOffset / seekOffset），
                # 留着 seekOffset 就是让这条队列哪天回到本实例时静默跳回用户很久以前
                # 手动钉过的位置。offsetStore 的那一格上面 persist 已经清掉了。
                self._consume_offset.pop(mq, None)
                self._offset_table.pop(mq, None)
                self._seek_offset.pop(mq, None)
            if self._message_queue_listener is not None:
                try:
                    self._message_queue_listener.message_queue_changed(list(new_set), list(old))
                except Exception:
                    pass

    # ---------------- poll / 位点 ----------------
    def poll(self, timeout: Optional[int] = None) -> List[MessageExt]:
        if timeout is None:
            timeout = self.poll_timeout_millis
        # Java poll() 进来先按截止时刻试一次自动提交（拿锁之前做：提交要发 RPC，
        # 抱着缓冲锁等网络会把 _enqueue 一起卡住）。
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
        # 对位 Java poll()：消息交到调用方手上才推进"已消费游标"
        # （assignedMessageQueue.updateConsumeOffset(mq, processQueue.removeMessage(msgs))）。
        # 拉取游标 _next_offset 一律不动——提交位点绝不能改掉"下次从哪拉"。
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
        # Java 的 seek 只置 seekOffset，下一次拉取时 nextPullOffset() 才把它同时写进
        # consumeOffset（"跳回去"意味着"那里之前都还没消费"，否则重放的消息会被位点跳过）。
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
        """拉取游标（观测点：真机对拍要看的正是"拉了多少"与"交了多少"这两格的差）。"""
        return self._next_offset.get(mq, -1)

    def consume_cursor_of(self, mq: MessageQueue) -> int:
        """已消费游标（``poll()`` 交出去的那一格）。"""
        return self._consume_offset.get(mq, -1)

    def pending_commit_of(self, mq: MessageQueue) -> int:
        """内存位点表里那一格（``persist=False`` 提交但还没落盘的值就在这）。"""
        return self._offset_table.get(mq, -1)

    def committed(self, mq: MessageQueue) -> Optional[int]:
        """Java ``committed()`` 走 ``offsetStore.readOffset(MEMORY_FIRST_THEN_STORE)``：
        先看内存位点表（``persist=False`` 刚提交、还没发给 broker 的值也算数），
        再问 broker，并把 broker 的值回填进表里（Java 同一处也回填）。"""
        cached = self._offset_table.get(mq)
        if cached is not None:
            return cached
        broker_offset = self._mq_client.query_consumer_offset(self.consumer_group, mq)
        if broker_offset is not None:
            self._offset_table[mq] = broker_offset
        return broker_offset

    def _persist_offset(self, mq: MessageQueue) -> None:
        """Java ``OffsetStore#persist(mq)``：只把这一条队列的内存位点发给 broker，不做清理。"""
        offset = self._offset_table.get(mq)
        if offset is None:
            return
        try:
            self._mq_client.update_consumer_offset(self.consumer_group, mq, offset)
        except Exception as e:  # noqa: BLE001
            logger.debug("lite persist failed for %s: %s", mq, e)

    def _persist_offset_table(self, mqs) -> None:
        """Java ``RemoteBrokerOffsetStore#persistAll(Set)``：内存位点表里落在 ``mqs`` 上的
        那部分写给 broker，**不在**其中的条目顺手从表里删掉（Java 日志里那句 ``remove unused mq``）。

        后半句是 Java 的真实行为：这张表只服务于当下持有的队列，撤走的队列留在表里没人再
        提交，persistAll 一路扫过去就清掉。代价是 ``commit(部分队列, persist=True)`` 会把其余
        队列**尚未落盘**的内存值一起丢掉——要提交谁就一次给全，别指望上一轮的其余队列还在表里。
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
        """提交消费位点。三个入口对位 Java ``DefaultLitePullConsumer`` 的三个重载：

        - ``commit()``                       → ``commitAll()``：按"已消费游标"提交所有已分配队列
        - ``commit({MessageQueue: offset})`` → ``commit(Map, persist)``：调用方指定位点
        - ``commit([MessageQueue, ...])``     → ``commit(Set, persist)``：只提交这几条队列

        指定位点**只改提交位置，不改拉取游标**：缓冲里已有的消息照旧交给调用方，下一轮
        ``commit()`` 又会按游标把表覆盖回去（Java 同一处就是这个次序：Map 走
        ``updateConsumeOffset`` 只写 offsetStore，``commitAll`` 每次重新从
        ``assignedMessageQueue.getConsumerOffset`` 取数）。

        两处已知的偏离，都在这条链路上：
        ① Java 的 ``commitAll()`` 只写内存表，真正发给 broker 靠 MQClientInstance 每
           ``persistConsumerOffsetInterval``（5s）一次的 ``persistConsumerOffset()``；本端口的
           lite 消费者没挂那个定时器，所以 ``persist=True``（默认）就地发出去。
        ② Java 的 ``persistAll`` 用 oneway（``updateConsumeOffsetToBroker(mq, offset)`` 那个
           私有重载 ``isOneway=true``）、异常只记日志；这里发同步带应答，坏位点当场就能从日志看到。
        """
        if offsets is None:
            # Java commitAll()：只遍历当下持有的队列
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
                # Java 的 processQueue != null && !isDropped() 守卫：不是本实例持有的队列
                # 一律不替它提交，静默跳过（Java 原文这里连日志都没有）。
                continue
            self._offset_table[mq] = offset

        if persist:
            self._persist_offset_table(scope)

    def offset_for_timestamp(self, mq: MessageQueue, timestamp: int) -> int:
        return self._mq_client.search_offset_by_timestamp(mq, timestamp)

    def assignment(self) -> List[MessageQueue]:
        return list(self._assigned)

    def fetch_message_queues(self, topic: str) -> List[MessageQueue]:
        info = self._mq_client.get_topic_publish_info(self._with_namespace(topic))
        return [MessageQueue(q.topic, q.broker_name, q.queue_id) for q in info.msg_queue_list]

    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        return self.fetch_message_queues(topic)

    def pause(self, message_queues) -> None:
        self._paused |= set(message_queues)

    def resume(self, message_queues) -> None:
        self._paused -= set(message_queues)


class SimpleMessageListener(MessageListenerConcurrently):
    """便捷监听器包装：把消费逻辑转成纯函数。"""

    def __init__(self, fn: Callable[[List[MessageExt]], ConsumeConcurrentlyStatus]):
        self.fn = fn

    def consume_message(self, msgs: List[MessageExt],
                        context: ConsumeConcurrentlyContext) -> ConsumeConcurrentlyStatus:
        return self.fn(msgs)


__all__ = [
    "DefaultMQPushConsumer", "DefaultMQPullConsumer", "MessageSelector",
    "MessageQueueListener", "AllocateMessageQueueStrategy",
    "AllocateMessageQueueAveragely", "AllocateMessageQueueAveragelyByCircle",
    "AllocateMessageQueueByConfig", "AllocateMessageQueueConsistentHash",
    "AllocateMessageQueueByMachineRoom", "AllocateMachineRoomNearby",
    "MachineRoomResolver", "ConsistentHashRouter", "MD5Hash", "HashFunction",
    "SimpleMessageListener",
    "PullResult", "PullStatus", "MessageListener", "MessageListenerConcurrently",
    "MessageListenerOrderly", "ConsumeConcurrentlyStatus", "ConsumeOrderlyStatus",
]
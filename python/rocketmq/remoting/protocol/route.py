# -*- coding: utf-8 -*-
"""Topic 路由数据（对应 org.apache.rocketmq.remoting.protocol.route.*）。"""
from __future__ import annotations

import random
from typing import Dict, List, Optional

from ...common.message import MessageQueue
from ...common.sysflag import PermName


class QueueData:
    def __init__(self, broker_name: str = "", read_queue_nums: int = 0, write_queue_nums: int = 0,
                 perm: int = 0, topic_sys_flag: int = 0):
        self.broker_name = broker_name
        self.read_queue_nums = read_queue_nums
        self.write_queue_nums = write_queue_nums
        self.perm = perm
        self.topic_sys_flag = topic_sys_flag

    def clone(self) -> "QueueData":
        return QueueData(self.broker_name, self.read_queue_nums, self.write_queue_nums,
                         self.perm, self.topic_sys_flag)

    def compare_to(self, o: "QueueData") -> int:
        if self.broker_name == o.broker_name:
            return 0
        return -1 if self.broker_name < o.broker_name else 1

    def to_dict(self) -> dict:
        return {
            "brokerName": self.broker_name,
            "readQueueNums": self.read_queue_nums,
            "writeQueueNums": self.write_queue_nums,
            "perm": self.perm,
            "topicSysFlag": self.topic_sys_flag,
        }

    @staticmethod
    def from_dict(d: dict) -> "QueueData":
        return QueueData(
            d.get("brokerName", ""),
            int(d.get("readQueueNums", 0)),
            int(d.get("writeQueueNums", 0)),
            int(d.get("perm", 0)),
            int(d.get("topicSysFlag", 0)),
        )

    def __eq__(self, other):
        if not isinstance(other, QueueData):
            return False
        return (self.broker_name == other.broker_name and self.read_queue_nums == other.read_queue_nums
                and self.write_queue_nums == other.write_queue_nums and self.perm == other.perm
                and self.topic_sys_flag == other.topic_sys_flag)

    def __hash__(self):
        return hash((self.broker_name, self.read_queue_nums, self.write_queue_nums, self.perm, self.topic_sys_flag))

    def __repr__(self):
        return "QueueData [brokerName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%d, topicSysFlag=%d]" % (
            self.broker_name, self.read_queue_nums, self.write_queue_nums, self.perm, self.topic_sys_flag)


class BrokerData:
    def __init__(self, cluster: str = "", broker_name: str = "",
                 broker_addrs: Optional[Dict[int, str]] = None, zone_name: str = ""):
        self.cluster = cluster
        self.broker_name = broker_name
        self.broker_addrs: Dict[Long, str] = broker_addrs if broker_addrs is not None else {}
        self.zone_name = zone_name
        self.enable_acting_master = False
        self._random = random.Random()

    def select_broker_addr(self) -> Optional[str]:
        if not self.broker_addrs:
            return None
        master_addr = self.broker_addrs.get(0)
        if master_addr is not None:
            return master_addr
        addrs = list(self.broker_addrs.values())
        return addrs[self._random.randint(0, len(addrs) - 1)]

    def clone(self) -> "BrokerData":
        return BrokerData(self.cluster, self.broker_name, dict(self.broker_addrs), self.zone_name)

    def compare_to(self, o: "BrokerData") -> int:
        if self.broker_name == o.broker_name:
            return 0
        return -1 if self.broker_name < o.broker_name else 1

    def to_dict(self) -> dict:
        d = {
            "cluster": self.cluster,
            "brokerName": self.broker_name,
            "brokerAddrs": {str(k): v for k, v in self.broker_addrs.items()},
            "zoneName": self.zone_name,
            "enableActingMaster": self.enable_acting_master,
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "BrokerData":
        bd = BrokerData(d.get("cluster", ""), d.get("brokerName", ""),
                        {int(k): v for k, v in (d.get("brokerAddrs") or {}).items()},
                        d.get("zoneName", ""))
        bd.enable_acting_master = d.get("enableActingMaster", False) or False
        return bd

    def __eq__(self, other):
        if not isinstance(other, BrokerData):
            return False
        return (self.cluster == other.cluster and self.broker_name == other.broker_name
                and self.broker_addrs == other.broker_addrs)

    def __hash__(self):
        return hash((self.cluster, self.broker_name, frozenset(self.broker_addrs.items())))

    def __repr__(self):
        return "BrokerData [brokerName=%s, brokerAddrs=%s]" % (self.broker_name, self.broker_addrs)


Long = int  # Java long 别名，便于阅读


class TopicRouteData:
    def __init__(self):
        self.order_topic_conf: Optional[str] = None
        self.queue_datas: List[QueueData] = []
        self.broker_datas: List[BrokerData] = []
        self.filter_server_table: Dict[str, List[str]] = {}
        self.topic_queue_mapping_by_broker: Optional[Dict[str, dict]] = None

    def get_broker_datas(self) -> List["BrokerData"]:
        """返回 broker 数据列表（与 Java topicRouteData.getBrokerDatas() 对齐）。"""
        return self.broker_datas

    def get_all_message_queue(self, topic: str = "") -> List["MessageQueue"]:
        """根据 queueDatas + brokerDatas 组装全部可写 MessageQueue（对应 Java 的 topicRouteData2TopicPublishInfo 组装逻辑）。

        topic 用于回填到每个 MessageQueue（Java 用真实 topic，而不是空串），否则后续
        send/pull 用 mq.topic 回查路由时会查不到。
        """
        mqs: List["MessageQueue"] = []
        for qd in self.queue_datas:
            if not PermName.check_perm(qd.perm, PermName.PERM_WRITE):
                continue
            broker_data = None
            for bd in self.broker_datas:
                if bd.broker_name == qd.broker_name:
                    broker_data = bd
                    break
            if broker_data is None:
                continue
            for i in range(qd.write_queue_nums):
                mqs.append(MessageQueue(topic, qd.broker_name, i))
        return mqs

    def clone_topic_route_data(self) -> "TopicRouteData":
        trd = TopicRouteData()
        trd.order_topic_conf = self.order_topic_conf
        trd.queue_datas = list(self.queue_datas)
        trd.broker_datas = list(self.broker_datas)
        trd.filter_server_table = {k: list(v) for k, v in self.filter_server_table.items()}
        if self.topic_queue_mapping_by_broker is not None:
            trd.topic_queue_mapping_by_broker = dict(self.topic_queue_mapping_by_broker)
        return trd

    def topic_route_data_changed(self, old: Optional["TopicRouteData"]) -> bool:
        if old is None:
            return True
        old_q = sorted(self.queue_datas, key=lambda x: (x.broker_name, x.read_queue_nums, x.write_queue_nums, x.perm))
        new_q = sorted(old.queue_datas, key=lambda x: (x.broker_name, x.read_queue_nums, x.write_queue_nums, x.perm))
        old_b = sorted(self.broker_datas, key=lambda x: x.broker_name)
        new_b = sorted(old.broker_datas, key=lambda x: x.broker_name)
        return not (old_q == new_q and old_b == new_b)

    def to_dict(self) -> dict:
        d = {
            "orderTopicConf": self.order_topic_conf,
            "queueDatas": [q.to_dict() for q in self.queue_datas],
            "brokerDatas": [b.to_dict() for b in self.broker_datas],
            "filterServerTable": self.filter_server_table,
        }
        if self.topic_queue_mapping_by_broker is not None:
            d["topicQueueMappingByBroker"] = self.topic_queue_mapping_by_broker
        return d

    @staticmethod
    def from_dict(d: dict) -> "TopicRouteData":
        trd = TopicRouteData()
        trd.order_topic_conf = d.get("orderTopicConf")
        trd.queue_datas = [QueueData.from_dict(q) for q in (d.get("queueDatas") or [])]
        trd.broker_datas = [BrokerData.from_dict(b) for b in (d.get("brokerDatas") or [])]
        trd.filter_server_table = d.get("filterServerTable") or {}
        trd.topic_queue_mapping_by_broker = d.get("topicQueueMappingByBroker")
        return trd

    def encode(self) -> bytes:
        from . import serialize
        data = serialize.RemotingSerializable.encode(self.to_dict())
        return data if data is not None else b""

    @staticmethod
    def decode(data: bytes) -> "TopicRouteData":
        from . import serialize
        obj = serialize.RemotingSerializable.decode_json(data)
        return TopicRouteData.from_dict(obj)

    def __repr__(self):
        return "TopicRouteData[orderTopicConf=%s, queueDatas=%s, brokerDatas=%s]" % (
            self.order_topic_conf, self.queue_datas, self.broker_datas)
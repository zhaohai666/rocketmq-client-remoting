# -*- coding: utf-8 -*-
"""Heartbeat data (the models of the protocol's heartbeat package).

The heartbeat body carries two extra fields next to the 4.x ones, both added for
the "heartbeat V2" registration path:

* ``heartbeatFingerprint`` -- a hash of the whole registration content. The broker
  dispatches on it: ``0`` takes the V1 registration path (the complete
  ``subscriptionDataSet`` is re-registered every round), a **non-zero** value takes the
  V2 path, where the broker keeps ``group -> fingerprint`` and answers with
  ``IS_SUB_CHANGE``.
* ``withoutSub`` -- the V2 "simple" heartbeat: the consumer groups are re-registered
  **without** their subscription set (a subscription-less re-registration), which the broker
  only accepts while the fingerprint it stored still matches.

There is **no** ``HEART_BEAT_V2`` request code on the wire: V2 reuses ``HEART_BEAT`` (34)
and is recognised from those two body fields (code 203 belongs to
``GET_CONSUMER_CONNECTION_LIST``).
"""
from __future__ import annotations

import json
import time
from typing import List, Optional, Set

from common.mix_all import MixAll
from common.subscription_data import SubscriptionData
from common.util_all import utf16_int32_hash


class ConsumeType:
    CONSUME_ACTIVELY = "CONSUME_ACTIVELY"
    CONSUME_PASSIVELY = "CONSUME_PASSIVELY"


class MessageModel:
    CLUSTERING = "CLUSTERING"
    BROADCASTING = "BROADCASTING"


class ConsumeFromWhere:
    CONSUME_FROM_LAST_OFFSET = "CONSUME_FROM_LAST_OFFSET"
    CONSUME_FROM_FIRST_OFFSET = "CONSUME_FROM_FIRST_OFFSET"
    CONSUME_FROM_TIMESTAMP = "CONSUME_FROM_TIMESTAMP"


class ProducerData:
    def __init__(self, group_name: str = ""):
        self.group_name = group_name

    def to_dict(self) -> dict:
        return {"groupName": self.group_name}

    @staticmethod
    def from_dict(d: dict) -> "ProducerData":
        return ProducerData(d.get("groupName", ""))

    def __eq__(self, other):
        return isinstance(other, ProducerData) and self.group_name == other.group_name

    def __hash__(self):
        return hash(self.group_name)

    def __repr__(self):
        return "ProducerData [groupName=%s]" % self.group_name


class ConsumerData:
    def __init__(self, group_name: str = "", consume_type: str = ConsumeType.CONSUME_PASSIVELY,
                 message_model: str = MessageModel.CLUSTERING, consume_from_where: str = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET):
        self.group_name = group_name
        self.consume_type = consume_type
        self.message_model = message_model
        self.consume_from_where = consume_from_where
        self.subscription_data_set: Set[SubscriptionData] = set()
        self.unit_mode = False

    def to_dict(self) -> dict:
        """Serialize using the 5.x protocol's ConsumerData fields (**without** the 4.x
        consumeTimestamp / maxReconsumeTimes); subscriptionDataSet uses
        SubscriptionData.to_dict(), it cannot be taken from __dict__ directly
        (that holds snake_case keys and set objects, which JSON cannot serialize).

        The subscription set is emitted in a stable order: the same subscription table must
        serialize to the **same bytes** in every heartbeat round, otherwise the broker (and the
        client's own fingerprint) would see a "change" that never happened.
        """
        return {
            "groupName": self.group_name,
            "consumeType": self.consume_type,
            "messageModel": self.message_model,
            "consumeFromWhere": self.consume_from_where,
            "subscriptionDataSet": [s.to_dict() for s in sorted(
                self.subscription_data_set, key=_subscription_sort_key)],
            "unitMode": self.unit_mode,
        }

    @staticmethod
    def from_dict(d: dict) -> "ConsumerData":
        cd = ConsumerData(d.get("groupName", ""), d.get("consumeType", ConsumeType.CONSUME_PASSIVELY),
                          d.get("messageModel", MessageModel.CLUSTERING),
                          d.get("consumeFromWhere", ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET))
        cd.unit_mode = d.get("unitMode", False) or False
        for sd in d.get("subscriptionDataSet") or []:
            sub = SubscriptionData(sd.get("topic"), sd.get("subString"))
            sub.sub_version = int(sd.get("subVersion", 0))
            sub.expression_type = sd.get("expressionType", "TAG")
            sub.class_filter_mode = bool(sd.get("classFilterMode", False))
            sub.tags_set = set(sd.get("tagsSet") or [])
            sub.code_set = set(int(v) for v in (sd.get("codeSet") or []))
            cd.subscription_data_set.add(sub)
        return cd

    def __repr__(self):
        return "ConsumerData [groupName=%s, consumeType=%s, messageModel=%s, consumeFromWhere=%s]" % (
            self.group_name, self.consume_type, self.message_model, self.consume_from_where)


class HeartbeatData:
    def __init__(self, client_id: str = ""):
        self.client_id = client_id
        self.producer_data_set: Set[ProducerData] = set()
        self.consumer_data_set: Set[ConsumerData] = set()
        # Both default to the "broker takes the V1 path" values, so a heartbeat built by a
        # client that does not opt into heartbeat V2 stays byte-compatible with the old one.
        self.heartbeat_fingerprint: int = 0
        self.without_sub: bool = False

    def to_dict(self) -> dict:
        # heartbeatFingerprint == 0 makes the broker take the V1 registration path
        # (registering with the complete subscriptionDataSet), which is the safest choice;
        # only a non-zero value reaches the V2 optimization -- that is exactly what
        # the opt-in heartbeat V2 round sets, see client/heartbeat_v2.py.
        # withoutSub is the same idea (the protocol field is isWithoutSub,
        # while the JSON key the broker decodes is withoutSub).
        return {
            "clientID": self.client_id,
            "producerDataSet": [p.to_dict() for p in sorted(
                self.producer_data_set, key=lambda p: p.group_name)],
            "consumerDataSet": [c.to_dict() for c in sorted(
                self.consumer_data_set, key=lambda c: c.group_name)],
            "heartbeatFingerprint": self.heartbeat_fingerprint,
            "withoutSub": self.without_sub,
        }

    @staticmethod
    def from_dict(d: dict) -> "HeartbeatData":
        hb = HeartbeatData(d.get("clientID", ""))
        for p in d.get("producerDataSet") or []:
            hb.producer_data_set.add(ProducerData.from_dict(p))
        for c in d.get("consumerDataSet") or []:
            hb.consumer_data_set.add(ConsumerData.from_dict(c))
        # The two V2 fields must survive a decode: the broker reads them back out of the body
        # (its registration path switches on heartbeatFingerprint), and a client that
        # dropped them here could not echo a peer's heartbeat faithfully.
        hb.heartbeat_fingerprint = int(d.get("heartbeatFingerprint") or 0)
        hb.without_sub = bool(d.get("withoutSub") or False)
        return hb

    def fingerprint_payload(self) -> dict:
        """The canonical dict whose hash is the heartbeat fingerprint.

        Normalization before hashing: take the serialized dict, force every
        ``subVersion`` to 0, ``withoutSub`` to false, ``heartbeatFingerprint`` to 0 and
        ``clientID`` to "", then hash the JSON text. Dropping those four from the hash is the
        whole point: a subscription's version stamp is a wall-clock timestamp, the clientId
        differs per process and the two V2 flags describe *this round*, so including any of
        them would report a subscription change on every single heartbeat.
        """
        payload = self.to_dict()
        payload["clientID"] = ""
        payload["heartbeatFingerprint"] = 0
        payload["withoutSub"] = False
        for consumer in payload["consumerDataSet"]:
            for sub in consumer.get("subscriptionDataSet") or []:
                sub["subVersion"] = 0
        return payload

    def compute_fingerprint(self) -> int:
        """The fingerprint: the int32 string hash of the canonical JSON text.

        ``utf16_int32_hash`` hashes UTF-16 code units (the same function the
        tag hash ``codeSet`` uses), so the arithmetic is fixed even though the JSON
        text itself is produced by this port's serializer. The value only ever gets compared
        with **its own history** (the broker stores ``group -> fingerprint`` and this client
        stores ``broker addr -> fingerprint``), never with a value another client produced.

        Always compute it on the **with-subscription** body: this port computes it once per round
        on the full heartbeat and stamps the same value onto the ``withoutSub`` variant, so the
        light copy's own ``compute_fingerprint()`` (its subscription sets are empty) is meaningless.
        """
        text = json.dumps(self.fingerprint_payload(), sort_keys=True, ensure_ascii=False,
                          separators=(",", ":"))
        return utf16_int32_hash(text)

    def without_sub_copy(self) -> "HeartbeatData":
        """The V2 "simple" heartbeat body (the light variant of the registration).

        Same client / producer / consumer groups, **empty** subscription sets and
        ``withoutSub=true``; the broker then re-registers the groups as subscription-less
        ones instead of re-parsing the subscriptions.
        """
        copy = HeartbeatData(self.client_id)
        copy.heartbeat_fingerprint = self.heartbeat_fingerprint
        for pd in self.producer_data_set:
            copy.producer_data_set.add(ProducerData(pd.group_name))
        for cd in self.consumer_data_set:
            light = ConsumerData(cd.group_name, cd.consume_type, cd.message_model,
                                 cd.consume_from_where)
            light.unit_mode = cd.unit_mode
            copy.consumer_data_set.add(light)
        copy.without_sub = True
        return copy

    def encode(self) -> bytes:
        from .serialize import RemotingSerializable
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "HeartbeatData":
        from .serialize import RemotingSerializable
        return HeartbeatData.from_dict(RemotingSerializable.decode_json(data))


def _subscription_sort_key(sub: SubscriptionData):
    """Stable ordering for one subscription (sets have no defined iteration order)."""
    return (sub.topic or "", sub.sub_string or "", sub.expression_type or "",
            bool(sub.class_filter_mode), tuple(sorted(sub.tags_set or ())),
            tuple(sorted(sub.code_set or ())), sub.filter_class_source or "")


class HeartbeatV2Result:
    """The pair of flags a HEART_BEAT(34) response tells the client about heartbeat V2.

    ``support_v2`` comes from the ``IS_SUPPORT_HEART_BEAT_V2`` extField and ``sub_change`` from
    ``IS_SUB_CHANGE``; a broker that predates heartbeat V2 sends **neither**, which the client
    reads as "this broker only understands the V1 heartbeat" and keeps sending it the full
    subscription set.
    """

    def __init__(self, version: int = 0, sub_change: bool = False, support_v2: bool = False):
        self.version = version
        self.sub_change = sub_change
        self.support_v2 = support_v2

    @staticmethod
    def from_response(version: int, ext_fields: Optional[dict]) -> "HeartbeatV2Result":
        """Parse the two flags the way the broker writes them: case-insensitive ``"true"`` text.

        A missing key / missing extFields block therefore parses as ``False``.
        """
        ext = ext_fields or {}
        return HeartbeatV2Result(
            version,
            str(ext.get(MixAll.IS_SUB_CHANGE, "")).strip().lower() == "true",
            str(ext.get(MixAll.IS_SUPPORT_HEART_BEAT_V2, "")).strip().lower() == "true")

    def __eq__(self, other):
        return (isinstance(other, HeartbeatV2Result) and self.version == other.version
                and self.sub_change == other.sub_change and self.support_v2 == other.support_v2)

    def __hash__(self):
        return hash((self.version, self.sub_change, self.support_v2))

    def __repr__(self):
        return "HeartbeatV2Result [version=%s, isSubChange=%s, isSupportV2=%s]" % (
            self.version, self.sub_change, self.support_v2)

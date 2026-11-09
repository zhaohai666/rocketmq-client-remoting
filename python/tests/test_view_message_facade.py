# viewMessage facade tests -- corrected to the real Java layering:
#   * MQClientInstance.view_message is Java MQAdminImpl#viewMessage
#     (MQAdminImpl.java:267): a SINGLE leg -- decode the offset msgId and ask
#     the broker that stamped it VIEW_MESSAGE_BY_ID(33). A decode failure maps
#     to the NO_MESSAGE client error "query message by id finished, but no
#     message."; a broker miss raises MQBrokerException(NO_MESSAGE). There is
#     NO uniq-key fallback at this layer -- the producer / pull / push facades
#     ride this single-leg path in Java.
#   * Admin.view_message is Java DefaultMQAdminExtImpl#viewMessage
#     (DefaultMQAdminExtImpl.java:578): any leg-1 failure ("the msgId maybe
#     created by new client") falls back to the UNIQ index across brokers; when
#     neither leg matches it returns None instead of raising.
import unittest
from typing import Any, Dict, List, Optional

from client.consumer import DefaultMQPullConsumer, DefaultMQPushConsumer
from client.exception import MQBrokerException, MQClientException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import MessageExt
from common.message_decoder import (create_message_id, encode_message_ext,
                                    ip_and_port_to_bytes)
from remoting.protocol.codes import RequestCode, ResponseCode

TOPIC = "ViewFacadeUnitTestTopic"
GROUP = "GID_ViewFacadeUnitTest"


class FakeResponse:
    def __init__(self, code: int, body: Optional[bytes] = None):
        self.code = code
        self.body = body
        self.remark = ""


class FakeClient:
    """The direct broker invoke and the cross-broker UNIQ query."""

    def __init__(self, view_body: Optional[bytes] = None,
                 view_code: int = ResponseCode.SUCCESS,
                 uniq_hits: Optional[List[MessageExt]] = None):
        self.view_body = view_body
        self.view_code = view_code
        self.uniq_hits = uniq_hits or []
        self.view_calls: List[Dict[str, Any]] = []
        self.uniq_calls: List[Dict[str, Any]] = []

    def _invoke_sync(self, addr: str, request: Any, timeout_millis: int) -> FakeResponse:
        self.view_calls.append({"addr": addr, "code": request.code,
                                "ext": dict(request.ext_fields),
                                "timeout": timeout_millis})
        if self.view_code != ResponseCode.SUCCESS:
            raise MQClientException("broker said no", self.view_code)
        return FakeResponse(self.view_code, self.view_body)

    def _check_response(self, response: FakeResponse) -> None:
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(response.remark or "failed", response.code)

    def view_message(self, topic: str, msg_id: str, timeout_millis: int = 3000,
                     vip_channel_enabled: bool = False) -> MessageExt:
        # The facade-delegation tests stub the instance method itself: what they
        # pin is "the facade hands over to the client instance", not the legs.
        self.view_calls.append({"addr": "delegated", "code": -1, "ext": {},
                                "timeout": timeout_millis})
        return make_msg(b"facade")

    def query_message_all_brokers(self, topic: str, key: str, max_num: int,
                                  begin: int, end: int, index_type=None,
                                  uniq_key: bool = False, timeout_millis: int = 15000):
        self.uniq_calls.append({"topic": topic, "key": key, "uniq": uniq_key,
                                "index_type": index_type})
        return list(self.uniq_hits)


def offset_msg_id(offset: int) -> str:
    return create_message_id(ip_and_port_to_bytes("127.0.0.1", 10911), offset)


def make_msg(body: bytes) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=body)
    m.msg_id = "leg-msg"
    return m


class InstanceSingleLegTest(unittest.TestCase):
    """MQAdminImpl#viewMessage parity: one leg, no fallback, ever."""

    def test_leg1_offset_msgid_goes_to_that_broker_and_decodes(self):
        body = encode_message_ext(make_msg(b"hello-leg1"))
        fake = FakeClient(view_body=body)
        out = MQClientInstance.view_message(fake, TOPIC, offset_msg_id(100))
        self.assertEqual(out.get_body(), b"hello-leg1")
        self.assertEqual(len(fake.view_calls), 1)
        call = fake.view_calls[0]
        self.assertEqual(call["code"], RequestCode.VIEW_MESSAGE_BY_ID)
        self.assertTrue(call["addr"].startswith("127.0.0.1:10911"))
        self.assertEqual(call["ext"], {"topic": TOPIC, "offset": "100"})
        self.assertEqual(fake.uniq_calls, [], "the single leg must not touch the UNIQ index")

    def test_decode_failure_is_the_no_message_client_error_without_traffic(self):
        fake = FakeClient()
        with self.assertRaises(MQClientException) as ctx:
            MQClientInstance.view_message(fake, TOPIC, "UNIQ-STRING-NOT-AN-OFFSET-ID")
        self.assertIn("query message by id finished, but no message.", str(ctx.exception))
        self.assertIsNotNone(ctx.exception.cause)
        self.assertEqual(fake.view_calls, [], "a decode failure must not touch the network")
        self.assertEqual(fake.uniq_calls, [], "no uniq-key fallback at this layer")

    def test_empty_body_raises_broker_no_message_without_fallback(self):
        fake = FakeClient(view_body=None)
        with self.assertRaises(MQBrokerException) as ctx:
            MQClientInstance.view_message(fake, TOPIC, offset_msg_id(101))
        self.assertEqual(ctx.exception.response_code, ResponseCode.NO_MESSAGE)
        self.assertIn("message not found", str(ctx.exception))
        self.assertEqual(fake.uniq_calls, [])

    def test_broker_failure_surfaces_without_fallback(self):
        fake = FakeClient(view_code=ResponseCode.SYSTEM_ERROR)
        with self.assertRaises(MQClientException) as ctx:
            MQClientInstance.view_message(fake, TOPIC, offset_msg_id(102))
        self.assertIn("broker said no", str(ctx.exception))
        self.assertEqual(fake.uniq_calls, [])


class AdminTwoLegTest(unittest.TestCase):
    """DefaultMQAdminExtImpl#viewMessage parity: fallback + None on no-match."""

    def test_leg1_failure_falls_back_to_uniq_index(self):
        hit = make_msg(b"hello-admin-leg2")
        fake = ThrowingViewClient(uniq_hits=[hit])
        admin = self._admin(fake)
        out = admin.view_message(TOPIC, offset_msg_id(110))
        self.assertIs(out, hit)
        self.assertEqual(len(fake.uniq_calls), 1)
        self.assertTrue(fake.uniq_calls[0]["uniq"])

    def test_neither_leg_returns_none_instead_of_raising(self):
        fake = ThrowingViewClient(uniq_hits=[])
        admin = self._admin(fake)
        self.assertIsNone(admin.view_message(TOPIC, offset_msg_id(111)))

    def test_consume_message_directly_rejects_a_no_match_msgid(self):
        fake = ThrowingViewClient(uniq_hits=[])
        admin = self._admin(fake)
        with self.assertRaises(MQClientException) as ctx:
            admin.consume_message_directly(GROUP, "cid@instance", TOPIC, offset_msg_id(112))
        self.assertIn("not found", str(ctx.exception))

    def _admin(self, fake: FakeClient):
        from client.admin import DefaultMQAdminExt
        admin = DefaultMQAdminExt()
        admin._started = True
        admin._mq_client = fake
        return admin


class ThrowingViewClient(FakeClient):
    """The leg-1 view_message always fails, driving the admin fallback."""

    def view_message(self, *a: Any, **kw: Any):
        raise MQClientException("no such message")


class FacadeDelegationTest(unittest.TestCase):
    """The producer / push / pull facades are thin delegates over the
    instance's single-leg method."""

    def _fake(self) -> FakeClient:
        return FakeClient(view_body=encode_message_ext(make_msg(b"facade")))

    def _check(self, facade: Any, fake: FakeClient) -> None:
        out = facade.view_message(TOPIC, offset_msg_id(200))
        self.assertEqual(out.get_body(), b"facade")
        self.assertEqual(len(fake.view_calls), 1)

    def test_producer_facade(self):
        fake = self._fake()
        producer = DefaultMQProducer(GROUP)
        producer._started = True
        producer._mq_client = fake
        self._check(producer, fake)

    def test_push_consumer_facade(self):
        fake = self._fake()
        consumer = DefaultMQPushConsumer(GROUP)
        consumer._started = True
        consumer._mq_client = fake
        self._check(consumer, fake)

    def test_pull_consumer_facade(self):
        fake = self._fake()
        consumer = DefaultMQPullConsumer(GROUP)
        consumer._started = True
        consumer._mq_client = fake
        self._check(consumer, fake)

    def test_facade_before_start_is_refused(self):
        producer = DefaultMQProducer(GROUP)
        with self.assertRaises(MQClientException):
            producer.view_message(TOPIC, offset_msg_id(300))


if __name__ == "__main__":
    unittest.main()

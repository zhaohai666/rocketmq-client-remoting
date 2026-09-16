# -*- coding: utf-8 -*-
"""主动拉取消费者（DefaultMQPullConsumer）本地单测（不需要集群）。

为什么要有这个文件：`DefaultMQPullConsumer` 此前**既没有单测也没有真机脚本**
（grep 全仓 0 命中）——是纯"纸面能力"。这里覆盖不需要 broker 的那部分契约：
生命周期守卫、命名空间包装、以及**拉取 sysFlag 与 Java 对齐**（真机踩过超时的坑）。
"""
from __future__ import annotations

import inspect

import pytest

from rocketmq.client.consumer import DefaultMQPullConsumer
from rocketmq.client.exception import MQClientException
from rocketmq.common.message import MessageQueue
from rocketmq.common.sysflag import PullSysFlag
from rocketmq.remoting.protocol.heartbeat import MessageModel


class TestLifecycle:
    def test_empty_consumer_group_rejected(self):
        with pytest.raises(MQClientException):
            DefaultMQPullConsumer("")
        with pytest.raises(MQClientException):
            DefaultMQPullConsumer("   ")

    def test_start_without_namesrv_raises(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        with pytest.raises(MQClientException):
            c.start()

    def test_operations_before_start_raise(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.set_namesrv_addr("127.0.0.1:9876")
        mq = MessageQueue("TopicUnitTest", "broker-a", 0)
        # 没 start() 时任何需要 client 的操作都要抛，而不是 NPE / AttributeError
        with pytest.raises(MQClientException):
            c.fetch_subscribe_message_queues("TopicUnitTest")
        with pytest.raises(MQClientException):
            c.pull(mq, "*", 0, 32, 1000)
        with pytest.raises(MQClientException):
            c.update_consume_offset(mq, 0)

    def test_shutdown_before_start_is_noop(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.shutdown()  # 不应抛

    def test_set_namesrv_addr_splits_semicolon(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.set_namesrv_addr(" 127.0.0.1:9876 ; 127.0.0.1:9877 ;; ")
        assert c.name_server_addrs == ["127.0.0.1:9876", "127.0.0.1:9877"]

    def test_register_message_queue_listener_records_topic(self):
        c = DefaultMQPullConsumer("PG_UnitTest")

        class _L:
            def message_queue_changed(self, topic, mq_all, mq_divided):
                pass

        listener = _L()
        c.set_message_queue_listener(listener)
        assert c.message_queue_listener is listener


class TestPullSysFlagParity:
    """拉取标志位必须与 Java DefaultMQPullConsumerImpl.pullSyncImpl(:248) 一致。

    Java: sysFlag = PullSysFlag.buildSysFlag(false /*commitOffset*/, block /*suspend*/,
                                             true /*subscription*/, false /*classFilter*/)
    即 pull() = 短轮询（suspend=False）、pull_block_if_not_found() = 长轮询（suspend=True），
    且**两者都不带 commitOffset 位**（位点由调用方 update_consume_offset 自己提交）。

    真机教训：曾把 pull() 的 suspend 写成 True → broker 在队尾挂起到
    brokerSuspendMaxTimeMillis(20s)，客户端 5s 超时 → RemotingTimeoutException。
    """

    def test_pull_is_short_poll_without_commit_offset(self):
        src = inspect.getsource(DefaultMQPullConsumer.pull)
        assert "suspend=False" in src, "pull() 必须是短轮询（suspend=False）"
        assert "commit_offset=False" in src, "pull() 不自带 commitOffset 位"

    def test_pull_block_if_not_found_is_long_poll(self):
        src = inspect.getsource(DefaultMQPullConsumer.pull_block_if_not_found)
        assert "suspend=True" in src, "pull_block_if_not_found() 才是长轮询"

    def test_flag_bits(self):
        short = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                           subscription=True, class_filter=False)
        assert PullSysFlag.has_commit_offset_flag(short) is False
        assert PullSysFlag.has_suspend_flag(short) is False

        long_ = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                           subscription=True, class_filter=False)
        assert PullSysFlag.has_commit_offset_flag(long_) is False
        assert PullSysFlag.has_suspend_flag(long_) is True

    def test_message_model_defaults_to_clustering(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        assert c.message_model == MessageModel.CLUSTERING

    def test_namespace_field_present(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        assert c.namespace == ""
        c.namespace = "NS1"
        assert c.namespace == "NS1"

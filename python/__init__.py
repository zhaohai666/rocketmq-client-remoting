# -*- coding: utf-8 -*-
"""rocketmq-client-remoting - Python implementation of Apache RocketMQ 4.x remoting client.

Mirrors the capability surface of the Java client module (org.apache.rocketmq.client)
plus the remoting protocol layer (org.apache.rocketmq.remoting), so that a Python
process can talk to a RocketMQ 4.x cluster (NameServer + Broker) directly with the
classic remoting protocol (JSON / RocketMQ binary serialization).

Main entry points:
    client.producer.DefaultMQProducer
    client.producer.TransactionMQProducer
    client.consumer.DefaultMQPushConsumer
    client.consumer.DefaultMQPullConsumer
    client.consumer.DefaultLitePullConsumer
    client.admin.MQAdmin / MqClientAdmin

注：本文件位于源码根 ``python/`` 之下，``client`` / ``common`` / ``remoting``
三个包直接平铺在该目录里（旧版的嵌套包 ``rocketmq.*`` 已取消），因此导入写成
``from client.producer import DefaultMQProducer``，前提是 ``python/`` 在 ``sys.path`` 上。
"""
__version__ = "4.9.4"
# -*- coding: utf-8 -*-
"""rocketmq-client-remoting - Python implementation of Apache RocketMQ 4.x remoting client.

Mirrors the capability surface of the Java client module (org.apache.rocketmq.client)
plus the remoting protocol layer (org.apache.rocketmq.remoting), so that a Python
process can talk to a RocketMQ 4.x cluster (NameServer + Broker) directly with the
classic remoting protocol (JSON / RocketMQ binary serialization).

Main entry points:
    rocketmq.producer.DefaultMQProducer
    rocketmq.producer.TransactionMQProducer
    rocketmq.consumer.DefaultMQPushConsumer
    rocketmq.consumer.DefaultMQPullConsumer
    rocketmq.consumer.DefaultLitePullConsumer
    rocketmq.admin.MQAdmin / MqClientAdmin
"""
__version__ = "4.9.4"
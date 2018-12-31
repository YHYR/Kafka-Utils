# -*- coding: utf-8 -*-
"""
Subject: 在客户端监听Group的Rebalance行为
    Tips:
        可以通过自定义Consumer Client_id 来实现Rebalance后Partition的分配结果
        Python Consumer 默认的 Client_id 命名规则为: kafka-python-{version}


@Author YH YR
@Time 2018/12/31 14:13
"""
import time
from kafka import KafkaConsumer, ConsumerRebalanceListener


class CustomHandleRebalance(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print('Before Rebalance, Assignment partitions is: {0}'.format(revoked))

    def on_partitions_assigned(self, assigned):
        print('After Rebalance, Assignment partition is: {0}'.format(assigned))


class ListenerRebalance:
    def __init__(self, broker_list, group_name, topic, enable_auto_commit=True, auto_offset_reset='latest',
                 client_id=None):
        self.broker_list = broker_list
        self.topic = topic
        self.group_name = group_name
        self.enable_auto_commit = enable_auto_commit
        self.auto_offset_reset = auto_offset_reset
        self.client_id = client_id

    def consumer(self, process_msg):
        consumer = KafkaConsumer(group_id=self.group_name, bootstrap_servers=self.broker_list, client_id=self.client_id,
                                 enable_auto_commit=self.enable_auto_commit, auto_offset_reset=self.auto_offset_reset)

        consumer.subscribe(self.topic, listener=CustomHandleRebalance())

        while True:
            consumer_records = consumer.poll(100, max_records=1)
            for partition_info, records in consumer_records.items():
                for record in records:
                    process_msg(record)
                    time.sleep(2)


def print_msg(msg_dic):
    print(msg_dic)


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    group_name = 'group_test'
    topic = ['topic_demo']

    action = ListenerRebalance(broker_list, group_name, topic)
    action.consumer(print_msg)

# -*- coding: utf-8 -*-
"""
Subject: 从指定Offset处开始消费消息
Tips:
    Kafka默认会自动提交Offset信息(enable_auto_commit=True),
    如果只是想基于特定Offset做数据嗅探, 而不改变broker端原始的offset信息, 可以指定enable_auto_commit=False

    消息的消费是基于具体的Partition而言, 所以指定Offset的值实际是作用于具体的一个或多个Partition, 而非是Topic

@Author YH YR
@Time 2018/12/31 14:09
"""

from kafka import KafkaConsumer


class ConsumerSpecialOffset:
    def __init__(self, broker_list, group_name, topic, enable_auto_commit=True, auto_offset_reset='latest'):
        self.broker_list = broker_list
        self.topic = topic
        self.group_name = group_name
        self.enable_auto_commit = enable_auto_commit
        self.auto_offset_reset = auto_offset_reset

    def consumer_from_offset(self, process_msg, offset):
        consumer = KafkaConsumer(group_id=self.group_name, bootstrap_servers=self.broker_list,
                                 enable_auto_commit=self.enable_auto_commit, auto_offset_reset=self.auto_offset_reset)
        consumer.subscribe(self.topic)
        consumer.poll(0)
        for topic_partition in consumer.assignment():
            consumer.seek(topic_partition, offset)
        while True:
            consumer_records = consumer.poll(100)
            for partition_info, records in consumer_records.items():
                for record in records:
                    process_msg(record)


def print_msg(msg_dic):
    print(msg_dic)


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    group_name = 'group_test'
    topic = ['topic_demo']

    action = ConsumerSpecialOffset(broker_list, group_name, topic, enable_auto_commit=True)
    action.consumer_from_offset(print_msg, 1000)

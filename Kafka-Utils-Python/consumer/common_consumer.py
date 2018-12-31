# -*- coding: utf-8 -*-
"""
@Author YH YR
@Time 2018/12/31 13:35
"""

from kafka import KafkaConsumer


class KafkaConsumerUtil:
    def __init__(self, broker_list, group_name, topic, enable_auto_commit=True, auto_offset_reset='latest'):
        self.broker_list = broker_list
        self.topic = topic
        self.group_name = group_name
        self.enable_auto_commit = enable_auto_commit
        self.auto_offset_reset = auto_offset_reset

    def consumer(self, process_msg):
        consumer = KafkaConsumer(*self.topic, group_id=self.group_name, bootstrap_servers=self.broker_list,
                                 enable_auto_commit=self.enable_auto_commit, auto_offset_reset=self.auto_offset_reset)
        for msg in consumer:
            process_msg(msg)

        consumer.highwater()


def print_msg(msg_dic):
    print(msg_dic)


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    group_name = 'group_test'
    topic = ['topic_demo']

    action = KafkaConsumerUtil(broker_list, group_name, topic)
    action.consumer(print_msg)

# -*- coding: utf-8 -*-
"""
Subject: 修改给定Group、Topic下各Partition的offset; 不触发Group的Rebalance
    Tips: offset的修改必须是基于Partition而言的, 而非是Topic

@Author YH YR
@Time 2018/12/31 14:10
"""
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata


class CommitSpecialOffset:
    def __init__(self, broker_list, group_name, topic):
        self.topic = topic
        self.consumer = KafkaConsumer(group_id=group_name, bootstrap_servers=broker_list)

    def reset_offset(self, reset_offset_value):
        partitions_offset = {}
        for partition_id in self.consumer.partitions_for_topic(self.topic):
            partitions_offset[TopicPartition(topic, partition_id)] = OffsetAndMetadata(reset_offset_value, '')

        self.consumer.commit(partitions_offset)


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    group_name = 'group_test'
    topic = 'topic_demo'

    action = CommitSpecialOffset(broker_list, group_name, topic)
    action.reset_offset(500)

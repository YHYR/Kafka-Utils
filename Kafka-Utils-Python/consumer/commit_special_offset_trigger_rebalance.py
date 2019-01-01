# -*- coding: utf-8 -*-
"""
Subject: 修改给定Group、Topic下各Partition的offset
        通过触发Group的Rebalance, 实现在不停止原始服务的前提下改变原始服务的消费位置
        kafka-python的client.id前缀为'kafka-python-'; 根据Rebalance的机制,
        可以通过特殊的client.id来指定分区的分配结果,从而实现offset的修改
    Tips: offset的修改必须是基于Partition而言的, 而非是Topic

@Author YH YR
@Time 2018/12/31 14:10
"""
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata


class CommitSpecialOffset:
    def __init__(self, broker_list, group_name, topic, client_id=None):
        self.topic = topic
        self.consumer = KafkaConsumer(group_id=group_name, bootstrap_servers=broker_list, client_id=client_id)

    def reset_offset(self, reset_offset_value):
        # Trigger Rebalance
        self.consumer.subscribe(self.topic)
        self.consumer.poll(0)
        # Reset Offset
        partitions_offset = {}
        for partition_id in self.consumer.partitions_for_topic(self.topic):
            partitions_offset[TopicPartition(topic, partition_id)] = OffsetAndMetadata(reset_offset_value, '')

        self.consumer.commit(partitions_offset)


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    group_name = 'group_test'
    topic = 'topic_demo'
    client_id = 'aaa'

    action = CommitSpecialOffset(broker_list, group_name, topic, client_id)
    action.reset_offset(10)

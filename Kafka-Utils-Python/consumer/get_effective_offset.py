# -*- coding: utf-8 -*-
"""
Subject: 获取给定Topic各Partition的最大、最小offset值; 适用于Kafka 1.0 以后
    Tips: 本质上获取的是指定Topic的有效数据集的偏移量; 和是否被消费无关

@Author YH YR
@Time 2018/12/31 14:12
"""
from kafka import KafkaConsumer, TopicPartition


class GetEffectiveOffset:
    def __init__(self, broker_list, group_name, topic):
        self.topic = topic
        self.consumer = KafkaConsumer(group_id=group_name, bootstrap_servers=broker_list)

    def get_offset(self):
        partitions_structs = []

        for partition_id in self.consumer.partitions_for_topic(self.topic):
            partitions_structs.append(TopicPartition(self.topic, partition_id))

        beginning_offset = self.consumer.beginning_offsets(partitions_structs)
        end_offset = self.consumer.end_offsets(partitions_structs)

        for partition, offset in beginning_offset.items():
            print('{0} => beginning offset = {1}; end offset = {2}'.format(partition, offset,
                                                                           end_offset[partition]))


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    group_name = 'group_test'
    topic = 'topic_demo'

    action = GetEffectiveOffset(broker_list, group_name, topic)
    action.get_offset()

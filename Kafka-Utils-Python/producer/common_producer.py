# -*- coding: utf-8 -*-
"""
@Author YH YR
@Time 2018/12/31 13:21
"""

from kafka import KafkaProducer


class CommonKafkaProducer:
    def __init__(self, broker_list):
        self.producer = KafkaProducer(bootstrap_servers=broker_list)

    def produce(self, topic, msg):
        self.producer.send(topic, bytes(msg, encoding='utf8'))

    def __del__(self):
        self.producer.close()


if __name__ == '__main__':
    broker_list = 'localhost:9092'
    topic = 'topic_demo'

    action = CommonKafkaProducer(broker_list)
    for i in range(1000):
        action.produce(topic, 'msg -> {0}'.format(i))

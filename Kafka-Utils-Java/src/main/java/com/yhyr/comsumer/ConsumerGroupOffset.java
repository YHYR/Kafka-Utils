package com.yhyr.comsumer;

import kafka.coordinator.group.GroupMetadataManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

/**
 * Class Subject: 消费指定Group对应 __consumer_offsets 中的Offset信息
 *
 * @author yhyr
 * @since 2019/01/08 10:48
 */
public class ConsumerGroupOffset {
    public static void main(String[] args) {
        String brokers = "172.31.7.40:9092";
        String group = "group_test";
        String topic = "__consumer_offsets";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 1);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        // 计算Group对应的Partition Id
        int partitionId = Math.abs(group.hashCode() % 50);
        // 消费指定Partition下的Offset信息
        consumer.assign(Collections.singletonList(new TopicPartition(topic, partitionId)));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                GroupMetadataManager.OffsetsMessageFormatter formatter = new GroupMetadataManager.OffsetsMessageFormatter();
                formatter.writeTo(record, System.out);
            }
        }
    }
}

package com.yhyr.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Class Subject: 从指定Offset处开始消费
 * <p>
 * Tips: Consumer默认会自动帮你提交Offset信息(enable.auto.commit=true); 如果只想基于特定Offset做数据嗅探,
 * 而不改变broker端原始的offset信息, 可以指定enable.auto.commit=false;
 *
 * @author yhyr
 * @since 2018/12/30 18:24
 */
public class ConsumerSpecialOffset {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String topic = "topic_demo";
        String group = "group_test";

        int customPartitionOffset = 500;

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        /**
         * KafkaConsumer.seek 是基于客户端的行为;
         *
         * 因此在订阅Topic后, 必须调用一次poll方法, 用来获取Client被分配的Partition, 方可通过seek重新指定消费位置
         */
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(0);
        consumer.assignment().forEach(topicPartition -> consumer.seek(topicPartition, customPartitionOffset));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
        }
    }
}

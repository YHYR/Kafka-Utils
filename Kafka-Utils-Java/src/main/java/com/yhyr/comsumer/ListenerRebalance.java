package com.yhyr.comsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Class Subject: 在客户端监听Group的Rebalance行为;
 *
 * * 在触发Rebalance前 记录当前正在处理各Partition的offset信息; 在Rebalance后记录被重新分配的Partition信息
 *
 * @author yhyr
 * @since 2018/12/30 18:25
 */
public class ListenerRebalance {
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private static String brokers = "localhost:9092";
    private static String group = "group_test";
    private static String topic = "topic_demo";

    private static KafkaConsumer<String, String> consumer;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    private class CustomHandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println(String.format(
                "Before Rebalance, Assignment partitions is: %s; Current each partition's latest offset is: %s",
                collection.toString(), currentOffsets.toString()));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            System.out.println(String.format(
                "After Rebalance, Assignment partitions is: %s; Current each partition's latest offset is: %s",
                collection.toString(), currentOffsets.toString()));
        }
    }

    private void consumer() {
        try {
            consumer.subscribe(Collections.singletonList(topic), new CustomHandleRebalance());
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    // record current msg's offset
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(
                        record.offset()));
                    // processing msg
                    System.out.println("Processing msg : " + record.toString());
                }
            }
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
        } finally {
            consumer.commitSync(currentOffsets);
        }
    }

    public static void main(String[] args) {
        ListenerRebalance action = new ListenerRebalance();
        action.consumer();
    }
}

package com.yhyr.comsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Class Subject: 获取指定Topic各Partition的最大、最小Offset值; 适用于Kafka 1.0 以后
 *
 * Tips: 本质上获取的是指定Topic的有效数据集的偏移量; 和是否被消费无关
 *
 * @author yhyr
 * @since 2018/12/30 18:32
 */
public class GetEffectiveOffset {
    public static void main(String[] args) {

        String brokers = "localhost:9092";
        String topic = "topic_demo";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
            .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            .collect(Collectors.toList());

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

        beginningOffsets.forEach((tp, offset) -> System.out.println(String
            .format("%s => beginning Offset = %s; end Offset = %s", tp, offset, endOffsets.get(tp))));
    }
}

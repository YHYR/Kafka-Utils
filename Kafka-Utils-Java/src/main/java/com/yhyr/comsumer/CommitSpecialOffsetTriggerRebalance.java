package com.yhyr.comsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Class Subject: 修改给定Group、指定Topic下各Partition的offset;
 * 通过触发Group的Rebalance, 实现在不停止原始服务的前提下改变原始服务的消费位置
 * Java API的默认client.id的前缀为: consumer-xxx; 根据Rebalance的机制, 可以通过特殊的client.id来指定分区的分配结果,
 * 从而实现offset的修改
 * <p>
 * Tips: offset的修改必须是基于Partition而言的, 而非是Topic
 *
 * @author yhyr
 * @since 2018/12/30 18:25
 */
public class CommitSpecialOffsetTriggerRebalance {
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String group = "group_test";
        String topic = "topic_demo";

        Properties props = new Properties();
        // 指定client.id的值, 保证在排序时在首位
        String clientId = "aaa";
        props.put("client.id", clientId);
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Trigger Rebalance
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(0);

        // 获取Topic的Partition信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        // 将所有Partition的offset设置为10
        int resetOffsetValue = 10;
        partitionInfos.forEach(partitionInfo -> currentOffsets
            .put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()),
                new OffsetAndMetadata(resetOffsetValue)));
        consumer.commitSync(currentOffsets);

    }
}

package com.yhyr.comsumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Class Subject: 查询broker在指定时间窗内写入消息的offset范围; 适用于Kafka 0.10.1.0 以后
 *
 * Tips: KafkaConsumer.offsetsForTimes 查找符合给定时间的第一条消息的offset; 如果不存在，则返回null
 *
 * @author yhyr
 * @since 2018/12/30 18:28
 */
public class GetOffsetWithTimestamp {
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String topic = "topic_demo";

        String beginTime = "2018-12-30 16:32:49";
        String endTime = "2018-12-30 16:35:51";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Find Begin Offset
        Map<TopicPartition, Long> beginOffsetSearchMap = new HashMap<>();
        consumer.partitionsFor(topic).forEach(
            partitionInfo -> beginOffsetSearchMap.put(
                new TopicPartition(partitionInfo.topic(), partitionInfo.partition()),
                stringToTimestamp(beginTime, DATE_PATTERN)));
        Map<TopicPartition, OffsetAndTimestamp> resultOfBeginOffset = consumer.offsetsForTimes(beginOffsetSearchMap);

        // Find End Offset
        Map<TopicPartition, Long> endOffsetSearchMap = new HashMap<>();
        consumer.partitionsFor(topic).forEach(
            partitionInfo -> endOffsetSearchMap.put(
                new TopicPartition(partitionInfo.topic(), partitionInfo.partition()),
                stringToTimestamp(endTime, DATE_PATTERN)));
        Map<TopicPartition, OffsetAndTimestamp> resultOfEndOffset = consumer.offsetsForTimes(endOffsetSearchMap);

        // Format Print
        resultOfBeginOffset.forEach((key, value) -> System.out.println(String.format(
            "From %s to %s, %s offset range = [%s, %s]", beginTime, endTime, key, formatPrint(value),
            formatPrint(resultOfEndOffset.get(key)))));
    }

    private static Object formatPrint(OffsetAndTimestamp offsetAndTimestamp) {
        return (offsetAndTimestamp == null) ? "null" : offsetAndTimestamp.offset();
    }

    private static Long stringToTimestamp(String date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            return sdf.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
}

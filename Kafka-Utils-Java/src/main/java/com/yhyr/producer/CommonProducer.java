package com.yhyr.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * @author yhyr
 * @since 2018/12/30 18:02
 */
public class CommonProducer {
    public static void main(String[] args) {
        String topic = "topic_demo";
        String brokerList = "localhost:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        IntStream.range(0, 1000).<ProducerRecord<String, String>>mapToObj(
            i -> new ProducerRecord<>(topic, "msg -> " + i)).forEach(producer::send);

        System.out.println("send message over");
    }
}

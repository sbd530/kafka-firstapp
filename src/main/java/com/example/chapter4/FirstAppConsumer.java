package com.example.chapter4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FirstAppConsumer {

    private static String topicName = "first-app";

    public static void main(String[] args) {
        // [1] KafkaConsumer Configuration
        Properties conf = new Properties();
        /*conf.setProperty("bootstrap.servers", "kafka-broker01:9092,kafka-broker02:9092,kafka-broker03:9092");*/
        conf.setProperty("bootstrap.servers", "localhost:9092");
        conf.setProperty("group.id", "FirstAppConsumerApp");
        conf.setProperty("enable.auto.commit", "false");
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // [2] Object for consuming messages from KafkaCluster
        Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);

        // [3] Register the subscribing Topic
        consumer.subscribe(Collections.singletonList(topicName));

        for (int count = 0; count < 300; count++) {
            // [4] Consume messages and print on console
            ConsumerRecords<Integer, String> records = consumer.poll(1);
            for (ConsumerRecord<Integer, String> record : records) {
                String msgString = String.format("key:%d, value: %s", record.key(), record.value());
                System.out.println(msgString);

                // [5] Commit Offset of completed message
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
                Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                consumer.commitSync(commitInfo);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // [6] Close KafkaConsumer
        consumer.close();
    }
}

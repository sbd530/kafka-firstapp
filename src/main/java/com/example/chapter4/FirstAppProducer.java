package com.example.chapter4;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class FirstAppProducer {

    private static String topicName = "first-app";

    public static void main(String[] args) {
        // KafkaProducer Configuration
        Properties conf = new Properties();
        /*conf.setProperty("bootstrap.servers", "kafka-broker01:9092,kafka-broker02:9092,kafka-broker03:9092");*/
        conf.setProperty("bootstrap.servers", "localhost:9092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Object for producing messages to KafkaCluster
        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key;
        String value;

        for (int i = 1; i <= 100; i++) {
            key = i;
            value = String.valueOf(i);

            // Record to be produced
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);

            // Callback for Ack after Producing
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        // Success
                        String infoString = String.format("Success partition:%d, offset:%d", recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    } else {
                        // Fail
                        String infoString = String.format("Failed:%s", e.getMessage());
                        System.out.println(infoString);
                    }
                }
            });
        }

        // Close KafkaProducer and exit
        producer.close();
    }
}
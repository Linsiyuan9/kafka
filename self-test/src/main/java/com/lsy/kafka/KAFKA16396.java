package com.lsy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KAFKA16396 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 设置 Kafka 生产者的配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka01:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(
                "event#Collector-2021-01-01-001#Probe-0001#1067267613#1002", "sendValue".getBytes());
        Future<RecordMetadata> recordResult = producer.send(producerRecord);

        RecordMetadata recordMetadata = recordResult.get();
        if (recordResult.isDone() && recordMetadata != null && recordMetadata.offset() >= 0) {
            System.out.println(recordMetadata);
        } else {
            System.out.println("G");
        }
    }

}

package com.lsy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TransactionTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 设置 Kafka 生产者的配置`
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                "test-topic", "sendKey", "sendValue");

        producer.initTransactions();
        producer.beginTransaction();
        Future<RecordMetadata> recordResult = producer.send(producerRecord);

        producer.commitTransaction();

        RecordMetadata recordMetadata = recordResult.get();
        if (recordResult.isDone() && recordMetadata != null && recordMetadata.offset() >= 0) {
            System.out.println(recordMetadata);
        } else {
            System.out.println("G");
        }
    }

}

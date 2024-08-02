/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lsy.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TransactionTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        t2();
//        t3();
//        t4();
//        t5();
    }

    public static void t2() throws ExecutionException, InterruptedException {
        // 设置 Kafka 生产者的配置`
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("acks", "all");
//        properties.put("batch.size", 16384);
//        properties.put("linger.ms", 1);
//        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id");
//        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(
                    "par", "1", "sendValue");
            ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(
                    "par", "2", "sendValue");
            ProducerRecord<String, String> producerRecord3 = new ProducerRecord<>(
                    "par", "3", "sendValue");

            Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
            Future<RecordMetadata> recordResult2 = producer.send(producerRecord2);
            Future<RecordMetadata> recordResult3 = producer.send(producerRecord3);

        }
        producer.close();
    }

    public static void t5() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test_group"); // 消费者组ID
        properties.put("enable.auto.commit", "true"); // 是否自动提交offset，默认为true
        properties.put("auto.commit.interval.ms", "1000"); // 自动提交offset的时间间隔

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("test"));

        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            consumer.commitAsync((offsets, e) -> System.out.println(e));
        }
    }

    public static void t3() {
        new Thread(() -> {
            try {
                // 设置 Kafka 生产者的配置`
                Properties properties = new Properties();
                properties.put("bootstrap.servers", "localhost:9092");
                properties.put("acks", "all");
                properties.put("batch.size", 16384);
                properties.put("linger.ms", 1);
                properties.put("buffer.memory", 33554432);
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id_1");
                properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                properties.put(ProducerConfig.RETRIES_CONFIG, 10);

                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

                ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(
                        "test-topic-1", "my_tx_id_1", "sendValue");

                ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(
                        "test-topic-1", "my_tx_id_1", "sendValue");


                ProducerRecord<String, String> producerRecord3 = new ProducerRecord<>(
                        "test-topic-1", "my_tx_id_1", "sendValue");

                producer.initTransactions();
                producer.beginTransaction();

                for (int i = 0; i < 10; i++) {
                    System.out.println(Thread.currentThread().getName() + ":start sleep");
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(Thread.currentThread().getName() + ":end sleep");
                    Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
                    System.out.println(Thread.currentThread().getName() + ":start sleep");
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(Thread.currentThread().getName() + ":end sleep");
                    Future<RecordMetadata> recordResult2 = producer.send(producerRecord2);
                    System.out.println(Thread.currentThread().getName() + ":start sleep");
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(Thread.currentThread().getName() + ":end sleep");
                    Future<RecordMetadata> recordResult3 = producer.send(producerRecord3);
                    recordResult1.get();
                    recordResult2.get();
                    recordResult3.get();
                }
                producer.commitTransaction();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                // 设置 Kafka 生产者的配置`
                Properties properties = new Properties();
                properties.put("bootstrap.servers", "localhost:9092");
                properties.put("acks", "all");
                properties.put("batch.size", 16384);
                properties.put("linger.ms", 1);
                properties.put("buffer.memory", 33554432);
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id_2");
                properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                properties.put(ProducerConfig.RETRIES_CONFIG, 10);

                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

                ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(
                        "test-topic-1", "my_tx_id_2", "sendValue");

                ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>(
                        "test-topic-1", "my_tx_id_2", "sendValue");


                ProducerRecord<String, String> producerRecord3 = new ProducerRecord<>(
                        "test-topic-1", "my_tx_id_2", "sendValue");

                producer.initTransactions();
                producer.beginTransaction();

                for (int i = 0; i < 10; i++) {
                    System.out.println(Thread.currentThread().getName() + ":start sleep");
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(Thread.currentThread().getName() + ":end sleep");
                    Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
                    System.out.println(Thread.currentThread().getName() + ":start sleep");
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(Thread.currentThread().getName() + ":end sleep");
                    Future<RecordMetadata> recordResult2 = producer.send(producerRecord2);
                    System.out.println(Thread.currentThread().getName() + ":start sleep");
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println(Thread.currentThread().getName() + ":end sleep");
                    Future<RecordMetadata> recordResult3 = producer.send(producerRecord3);
                    recordResult1.get();
                    recordResult2.get();
                    recordResult3.get();
                }

                producer.commitTransaction();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();
    }


    public static void t4() {
        new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {

                    // 设置 Kafka 生产者的配置`
                    Properties properties = new Properties();
                    properties.put("bootstrap.servers", "localhost:9092");
                    properties.put("acks", "all");
                    properties.put("batch.size", 16384);
                    properties.put("linger.ms", 1);
                    properties.put("buffer.memory", 33554432);
                    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id_1");
                    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                    properties.put(ProducerConfig.RETRIES_CONFIG, 10);

                    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

                    ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(
                            "test-topic-1", "my_tx_id_1", "sendValue");

                    producer.initTransactions();
                    producer.beginTransaction();
                    Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
                    recordResult1.get();
                    producer.commitTransaction();

                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    // 设置 Kafka 生产者的配置`
                    Properties properties = new Properties();
                    properties.put("bootstrap.servers", "localhost:9092");
                    properties.put("acks", "all");
                    properties.put("batch.size", 16384);
                    properties.put("linger.ms", 1);
                    properties.put("buffer.memory", 33554432);
                    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id_2");
                    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                    properties.put(ProducerConfig.RETRIES_CONFIG, 10);

                    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

                    ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(
                            "test-topic-1", "my_tx_id_2", "sendValue");

                    producer.initTransactions();
                    producer.beginTransaction();
                    Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
                    recordResult1.get();
                    producer.commitTransaction();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }).start();
    }
}

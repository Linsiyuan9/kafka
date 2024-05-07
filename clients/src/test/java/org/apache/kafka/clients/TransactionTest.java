package org.apache.kafka.clients;

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


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TransactionTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 设置 Kafka 生产者的配置`
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:7092");
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
        producer.initTransactions();

        {
            producer.beginTransaction();
            System.out.println(Thread.currentThread().getName() + ":start");
            ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>("test-topic-1", "1", "sendValue");
            ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>("test-topic-1", "2", "sendValue");
            ProducerRecord<String, String> producerRecord3 = new ProducerRecord<>("test-topic-1", "3", "sendValue");
            Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
            Future<RecordMetadata> recordResult2 = producer.send(producerRecord2);
            Future<RecordMetadata> recordResult3 = producer.send(producerRecord3);
            System.out.println(Thread.currentThread().getName() + ":end");
            producer.commitTransaction();
            recordResult1.get();
            recordResult2.get();
            recordResult3.get();
        }
        {
            producer.beginTransaction();
            System.out.println(Thread.currentThread().getName() + ":start");
            ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>("test-topic-1", "1", "sendValue");
            ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>("test-topic-1", "2", "sendValue");
            ProducerRecord<String, String> producerRecord3 = new ProducerRecord<>("test-topic-1", "3", "sendValue");
            Future<RecordMetadata> recordResult1 = producer.send(producerRecord1);
            Future<RecordMetadata> recordResult2 = producer.send(producerRecord2);
            Future<RecordMetadata> recordResult3 = producer.send(producerRecord3);
            System.out.println(Thread.currentThread().getName() + ":end");
            producer.commitTransaction();
            recordResult1.get();
            recordResult2.get();
            recordResult3.get();
        }

    }

}

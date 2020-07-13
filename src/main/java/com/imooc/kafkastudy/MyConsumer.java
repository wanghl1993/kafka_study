package com.imooc.kafkastudy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MyConsumer {

    private static KafkaConsumer<String, String> consumer;

    private static Properties properties;

    static {

        properties = new Properties();
        // 反序列化
        properties.put("bootstrap.servers", "192.168.1.5:9092");
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "KafkaStudy");
    }

    private static void generalConsumeMessageAutoCommit() {

        properties.put("enable.auto.commit", true); //自动提交
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("imooc-kafka-study-x"));

        try {
            while (true) {

                boolean flag = true;
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format(
                            "topic = %s, partition = %s, key = %s, value = %s",
                            record.topic(), record.partition(),
                            record.key(), record.value()
                    ));
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }

                if(!flag) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        generalConsumeMessageAutoCommit();
    }

}

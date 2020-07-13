package com.imooc.kafkastudy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer2 {

    private static KafkaProducer<String, String> producer;

    static {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.1.6:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

//        properties.put("partitioner.class",
//                "com.imooc.kafkastudy.CustomPartitioner");

        producer = new KafkaProducer<>(properties);
    }

    private static void sendMessageForgetResult() {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "imooc-kafka-study", "name", "ForgetResult"
        );
        producer.send(record);
        producer.close();
    }

    public static void main(String[] args) throws Exception {

        sendMessageForgetResult();
    }

}

package com.run.spark.kafka;

public class KafkaClientConsumerTest {

    public static void main(String[] args) {
        new MyKafkaConsumer(KafkaProperties.TOPIC).start();
    }
}

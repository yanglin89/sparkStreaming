package com.run.spark.kafka;

/**
 * kafka 测试
 */
public class KafkaClientAppTest {

    public static void main(String[] args) {
        new MyKafkaProducer(KafkaProperties.TOPIC).start();
    }

}

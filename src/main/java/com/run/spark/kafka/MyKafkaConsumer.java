package com.run.spark.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * kafka 消费者
 */
public class MyKafkaConsumer extends Thread{

    private String topic;

    private Consumer<Integer,String> consumer;

    public MyKafkaConsumer(String topic){
        this.topic = topic;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BROKER_LIST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaProperties.GROUP_ID); // 握手机制
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<Integer, String>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("hello_topic"));

        while(true){
            ConsumerRecords<Integer,String> records = consumer.poll(100);
            for(ConsumerRecord<Integer,String> record :records){
                System.out.println("received = " + record.value());
            }

        }
    }
}

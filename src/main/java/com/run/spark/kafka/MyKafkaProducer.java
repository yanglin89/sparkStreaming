package com.run.spark.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka 生产者
 */
public class MyKafkaProducer extends Thread{

    private String topic;

    private Producer<Integer,String> producer;

    public MyKafkaProducer(String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("acks", "all"); // 握手机制
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<Integer, String>(props);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while(true){
            String msg ="message_" + messageNo;
            producer.send(new ProducerRecord<Integer, String>(topic,msg));
            System.out.println("sent : " + msg);

            messageNo++;

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

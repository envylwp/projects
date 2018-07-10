package com.projects.producer;

import com.projects.Constant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by lancerlin on 2018/2/9.
 */
public class Producer10 {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
//        String topic = "downgrade_topic";
        String topic = "test01_topic";
        props.put("bootstrap.servers", Constant.MYVM_KAFKA_BROKER);

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000000; i++) {
            sb.append("0123456789");
        }
        System.out.println(sb.length());
            producer.send(new ProducerRecord<String, String>(topic, null, "", sb.toString()));
        for (int i = 0; i < 29; i++) {
            String  value = i + " hello world";
            System.out.println(value);
//            producer.send(new ProducerRecord<String, String>(topic, null, i+"", value));
            producer.send(new ProducerRecord<String, String>(topic, null, i+"", sb.toString()));
            Thread.sleep(1000L);
        }


        producer.close();
    }
}

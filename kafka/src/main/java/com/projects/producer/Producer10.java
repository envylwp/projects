package com.projects.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

/**
 * Created by lancerlin on 2018/2/9.
 */
public class Producer10 {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        String topic = "lancer_test_clickstream_topic";
        String broker = "10.1.50.122:9092,10.1.50.123:9092,10.1.50.124:9092";
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);


        String filePath = "G://click.log";

        BufferedReader br = null;
        try {
            String str = "";
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
            while ((str = br.readLine()) != null) {
                System.out.println(str);
                producer.send(new ProducerRecord<String, String>(topic, null, System.currentTimeMillis(), str, str));
                Thread.sleep(1000L);
            }
        } catch (FileNotFoundException e) {
            System.out.println("找不到指定文件");
        } catch (IOException e) {
            System.out.println("读取文件失败");
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        producer.close();
    }
}

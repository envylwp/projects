package com.projects.newjavaconsumerapi;

import com.projects.Constant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lancerlin on 2018/1/23.
 */
public class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer = null;

    public void run() {
        try {
            consumer.subscribe(Arrays.asList("topic"));
            while (!closed.get()) {
                ConsumerRecords records = consumer.poll(10000);
                // Handle new records
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public static void main(String[] args) throws InterruptedException {
        //autoCommitConsumer();


        Properties props = new Properties();
        props.put("bootstrap.servers", "10.1.50.122:9092,10.1.50.123:9092,10.1.50.124:9092");
        props.put("group.id", "cg-javaconsumerapi02");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("php_log_topic"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            Thread.sleep(1000L);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
                System.out.println(record.key());
                System.out.println(record.timestamp());
                System.out.println(record.timestampType());
                System.out.println(record.value());
            }
            if(buffer.size() == 10){
                break;
            }
//            if (buffer.size() >= minBatchSize) {
//                //insertIntoDb(buffer);
//                consumer.commitSync();
//                buffer.clear();
//            }
        }
        System.out.println("buffer===================");
        System.out.println(buffer);

        /**
        以上示例使用commitSync将所有收到的消息标记为已提交。 在某些情况下，您可能希望通过明确指定偏移量来更好地控制哪些消息已被提交。
         在下面的例子中，我们在完成处理每个分区中的消息之后提交offset。

        try {
            while(running) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
         */
    }

    private static void autoCommitConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFAK_BROKER);
        props.put("group.id", "cg-javaconsumerapi");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test02_topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
        }
    }
}

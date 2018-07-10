package utils

import java.util
import java.util.{ArrayList, Arrays, List, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

object KafkaUtilsV1 {

  def consumer() {
    val props = new Properties
    props.put("bootstrap.servers", "10.1.50.122:9092,10.1.50.123:9092,10.1.50.124:9092")
    props.put("group.id", "cg-javaconsumerapi02")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("php_log_topic"))
    val buffer = new util.ArrayList[ConsumerRecord[String, String]]
    while (true) {
      val records = consumer.poll(10)
      import scala.collection.JavaConversions._
      for (record <- records) {
        buffer.add(record)
      }
      if (buffer.size >= 10) return buffer.subList(0, 10)
    }

  }

}

package book.chapter21

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.streaming.StreamingQueryListener

class KafkaMetrics(servers:String) extends StreamingQueryListener{
  val kafkaProperties=new Properties()
  kafkaProperties.put(
  "bootstrap.servers",
  servers)
  kafkaProperties.put(
  "key.serializer",
  "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put(
  "value.serializer",
  "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")

  val producer=new KafkaProducer[String, String](kafkaProperties)

  import org.apache.spark.sql.streaming.StreamingQueryListener
  import org.apache.kafka.clients.producer.KafkaProducer

  override def onQueryProgress(event:
  StreamingQueryListener.QueryProgressEvent):Unit={
  producer.send(new ProducerRecord("streaming-metrics",
  event.progress.json))
}
  override def onQueryStarted(event:
  StreamingQueryListener.QueryStartedEvent):Unit={}
  override def onQueryTerminated(event:
  StreamingQueryListener.QueryTerminatedEvent):Unit={}
}

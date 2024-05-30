package services

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.{Collections, Properties}
import java.time.Duration
import javax.inject.Inject
import scala.concurrent.ExecutionContext

class Consumerkafka @Inject()(implicit ec: ExecutionContext,prod:ProducerKafka) {

  def callConsumer = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "recommendation-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("user-data"))
    try {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        records.forEach { record =>
          val data = record.value().split(",")
          val genre = data(0)
          println(s"Recommendations for genre '$genre' ")
          prod.publishUserAction(genre)
          //recommendBooks(genre) // A function that finds and recommends books based on the genre
        }
      }
    }
    finally {
      consumer.close() // Ensure the consumer is properly closed on termination
    }
  }
}


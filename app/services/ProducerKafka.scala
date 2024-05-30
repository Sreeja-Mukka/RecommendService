package services

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import javax.inject.Inject
import scala.concurrent.ExecutionContext

class ProducerKafka @Inject()(implicit ec: ExecutionContext) {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9093")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    def publishUserAction(genre: String): Unit = {
      val record = new ProducerRecord[String, String]("user-viewed-data", genre)
      producer.send(record)
    }
}

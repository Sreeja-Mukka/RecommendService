package services

import play.api.libs.json._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import models.Book

import java.util.{Collections, Properties}
import java.time.Duration
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala

class Consumerkafka  {

  val bookviewed = HashMap[String,Int]()
  var booksFilteredGenre = ListBuffer[Book]()

  def callConsumer = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "recommendation-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("user-data"))
    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100)) // poll every 100 ms
        for (record: ConsumerRecord[String, String] <- records.asScala) {

          val jsonString = record.value()
          val books = Json.parse(jsonString).validate[Seq[Book]]

          if(!bookviewed.contains(record.key())) {
            bookviewed(record.key()) = 1
            books match {
              case JsSuccess(books, _) =>
                books.foreach { book =>
                  booksFilteredGenre += book
                }
              case JsError(errors) =>
                println(s"Failed to parse JSON: $errors")
            }
          }
          else {
            val views = bookviewed.getOrElse(record.key(), 0) + 1
            bookviewed(record.key()) = views
            if (views > 2) {
              sendMail(record.key())
            }
          }
          println(s"Received message: (Key: ${record.key()}, Value: ${record.value()})")
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }

  private def sendMail(bgenre: String) = {
    println("In mail Service:")
    println("-------------------")
    println(s"You have viewed this $bgenre books , so please find the similar set of books")
    for(book <- booksFilteredGenre){
      if(book.bgenre == bgenre) {
        println("Book Details")
        println(book)
        println()
      }
    }
    println("-------------------")
  }

}


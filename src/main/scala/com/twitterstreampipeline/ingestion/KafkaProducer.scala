package com.twitterstreampipeline.ingestion

import com.twitterstreampipeline.config.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._

import java.util.Properties
import scala.concurrent.{ExecutionContext, Future}

class TweetKafkaProducer(kafkaConfig: KafkaConfig)(implicit ec: ExecutionContext) {
  import TwitterJsonProtocol._

  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfig.clientId)
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaConfig.batchSize)
  props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaConfig.bufferMemory)

  private val producer = new KafkaProducer[String, String](props)

  def sendTweet(tweet: Tweet): Future[Unit] = Future {
    try {
      val tweetJson = tweet.toJson.compactPrint
      val record = new ProducerRecord[String, String](
        kafkaConfig.topic,
        tweet.id_str,
        tweetJson
      )

      producer.send(record).get()
    } catch {
      case ex: Exception =>
        println(s"Error sending tweet to Kafka: ${ex.getMessage}")
        throw ex
    }
  }

  def close(): Unit = {
    producer.flush()
    producer.close()
  }
}

object TweetKafkaProducer {
  def apply(kafkaConfig: KafkaConfig)(implicit ec: ExecutionContext): TweetKafkaProducer = {
    new TweetKafkaProducer(kafkaConfig)
  }
}
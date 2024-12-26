package com.twitterstreampipeline

import com.twitterstreampipeline.config.KafkaConfig
import com.twitterstreampipeline.ingestion.{TweetKafkaProducer, TwitterDataGenerator}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.errors.WakeupException
import java.util.Properties
import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

object MainApp extends App {

  implicit val ec: ExecutionContext = ExecutionContext.global


  val kafkaConfig = KafkaConfig(
    bootstrapServers = "localhost:9092",
    topic = "twitter-tweets"
  )


  val tweetGenerator = new TwitterDataGenerator("boulder_flood_geolocated_tweets.json")
  val kafkaProducer = TweetKafkaProducer(kafkaConfig)

  println(s"Loaded ${tweetGenerator.getTotalTweets} tweets")


  def streamTweets(delayMs: Long): Unit = {
    var continue = true

    while (continue) {
      tweetGenerator.nextTweet() match {
        case Some(tweet) =>

          val future = kafkaProducer.sendTweet(tweet)
          future.onComplete {
            case Success(_) =>
              println(s"Sent tweet: ${tweet.id_str}")
            case Failure(e) =>
              println(s"Error sending tweet: ${e.getMessage}")
          }


          Thread.sleep(delayMs)

        case None =>
          println("No more tweets to process")
          continue = false
      }
    }


    kafkaProducer.close()
  }


  def startKafkaConsumer(): Unit = {
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-consumer-group")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(List("twitter-tweets").asJava)

    println("Starting to consume tweets...")

    try {
      while (true) {

        val records = consumer.poll(Duration.ofMillis(1000))


        for (record <- records.asScala) {
          println(s"Consumed tweet: key = ${record.key()}, value = ${record.value()}")
        }


        Thread.sleep(100)
      }
    } catch {
      case e: WakeupException =>
        println("WakeupException caught, closing consumer.")
      case e: Exception =>
        println(s"Error during consumption: ${e.getMessage}")
    } finally {

      consumer.close()
    }
  }


  try {
    println("Starting tweet streaming and Kafka Consumer...")


    val consumerThread = new Thread(new Runnable {
      def run(): Unit = startKafkaConsumer()
    })

    consumerThread.start()


    streamTweets(1000)

  } catch {
    case e: Exception =>
      println(s"Application error: ${e.getMessage}")
      kafkaProducer.close()
      System.exit(1)
  }
}

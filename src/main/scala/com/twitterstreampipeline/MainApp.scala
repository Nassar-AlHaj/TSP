package com.twitterstreampipeline

import com.twitterstreampipeline.config.KafkaConfig
import com.twitterstreampipeline.ingestion.{TweetKafkaProducer, TwitterDataGenerator}
import com.twitterstreampipeline.processing.TweetKafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import scala.concurrent.{ExecutionContext, Future}
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

    try {
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
    } catch {
      case e: WakeupException =>
        println("Kafka producer interrupted")
      case e: Exception =>
        println(s"Error in tweet streaming: ${e.getMessage}")
    } finally {
      kafkaProducer.close()
    }
  }

  try {
    println("Starting tweet streaming and Tweet Processor...")

    val processorThread = new Thread(new Runnable {
      def run(): Unit = {
        TweetKafkaConsumer.main(Array.empty)
      }
    })
    processorThread.setName("TweetProcessorThread")
    processorThread.start()


    Thread.sleep(5000)


    streamTweets(1000)

  } catch {
    case e: Exception =>
      println(s"Application error: ${e.getMessage}")
      kafkaProducer.close()
      System.exit(1)
  }
}

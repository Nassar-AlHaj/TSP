package com.twitterstreampipeline

import com.twitterstreampipeline.config.KafkaConfig
import com.twitterstreampipeline.ingestion.{TweetKafkaProducer, TwitterDataGenerator}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object MainApp extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  // Configuration
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

          try {
            Await.ready(future, 5.seconds)
            println(s"Sent tweet: ${tweet.id_str}")
          } catch {
            case e: Exception =>
              println(s"Error sending tweet: ${e.getMessage}")
              continue = false
          }

          Thread.sleep(delayMs)

        case None =>
          println("No more tweets to process")
          continue = false
      }
    }


    kafkaProducer.close()
  }

  try {
    println("Starting tweet streaming...")
    streamTweets(1000)
  } catch {
    case e: Exception =>
      println(s"Application error: ${e.getMessage}")
      kafkaProducer.close()
      System.exit(1)
  }
}
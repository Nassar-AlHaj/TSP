package com.twitterstreampipeline

import com.twitterstreampipeline.config.KafkaConfig
import com.twitterstreampipeline.ingestion.{TweetKafkaProducer, TwitterDataGenerator}
import com.twitterstreampipeline.processing.TweetKafkaConsumer
import com.twitterstreampipeline.storage.{MongoDBConnector, TweetRepository}
import org.apache.kafka.common.errors.WakeupException
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object MainApp extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  def initializeMongoDB(): Unit = {
    try {
      val indexesFuture = Future {
      }
      Await.result(indexesFuture, 30.seconds)
    } catch {
      case e: Exception =>
        println(s"Failed to initialize MongoDB: ${e.getMessage}")
        System.exit(1)
    }
  }


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


  def startProcessingWithStorage(): Unit = {
    val spark = TweetKafkaConsumer.createSparkSession()
    val kafkaDF = TweetKafkaConsumer.readFromKafka(spark)
    val pipeline = new com.johnsnowlabs.nlp.pretrained.PretrainedPipeline("analyze_sentiment")
    val processedDF = TweetKafkaConsumer.processTweets(kafkaDF, pipeline)

    val query = processedDF
      .writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        val tweetsData: Seq[Map[String, Any]] = batchDF
          .coalesce(1)
          .limit(10)
          .collect()
          .map { row =>
            Map[String, Any](
              "id" -> row.getAs[String]("id"),
              "text" -> row.getAs[String]("text"),
              "username" -> row.getAs[String]("username"),
              "timestamp" -> row.getAs[java.sql.Timestamp]("timestamp").toString,
              "hashtags" -> row.getAs[Seq[String]]("hashtags"),
              "sentimentLabel" -> row.getAs[String]("sentiment"),
              "processed_at" -> row.getAs[java.sql.Timestamp]("processed_at").toString
            )
          }.toSeq

        if (tweetsData.nonEmpty) {
          import scala.concurrent.duration._
          import scala.concurrent.Await

          try {
            Await.result(TweetRepository.storeTweets(tweetsData), 10.seconds)
            println(s"Batch $batchId: Successfully stored ${tweetsData.size} tweets.")
          } catch {
            case e: Exception =>
              println(s"Batch $batchId: Error storing tweets: ${e.getMessage}")
          }
        }
      }
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }


  try {
    println("Initializing MongoDB...")
    initializeMongoDB()

    println("Starting tweet streaming and Tweet Processor...")

    val processorThread = new Thread(new Runnable {
      def run(): Unit = {
        startProcessingWithStorage()
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
      MongoDBConnector.close()
      System.exit(1)
  }
}

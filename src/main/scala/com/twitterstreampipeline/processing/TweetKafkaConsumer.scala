package com.twitterstreampipeline.processing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

object TweetKafkaConsumer {

  private val tweetSchema = new StructType()
    .add("created_at", StringType)
    .add("id_str", StringType)
    .add("text", StringType)
    .add("entities", new StructType()
      .add("hashtags", ArrayType(new StructType()
        .add("indices", ArrayType(LongType))
        .add("text", StringType))))
    .add("coordinates", new StructType()
      .add("coordinates", ArrayType(DoubleType))
      .add("type", StringType))
    .add("user", new StructType()
      .add("screen_name", StringType)
      .add("location", StringType))

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("TweetProcessor")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "./checkpoints")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
  }


  def readFromKafka(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter-tweets")
      .option("startingOffsets", "latest")
      .load()
  }


  def processTweets(df: DataFrame): DataFrame = {
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), tweetSchema).as("tweet"))
      .select(
        col("tweet.id_str").as("id"),
        col("tweet.text").as("text"),
        to_timestamp(unix_timestamp(
          col("tweet.created_at"), "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
        ).cast("timestamp")).as("timestamp"),
        when(col("tweet.coordinates.coordinates").isNotNull,
          struct(
            col("tweet.coordinates.coordinates").getItem(0).as("longitude"),
            col("tweet.coordinates.coordinates").getItem(1).as("latitude")
          )).as("location"),
        expr("transform(tweet.entities.hashtags, h -> h.text)").as("hashtags"),
        col("tweet.user.screen_name").as("username"),
        col("tweet.user.location").as("user_location"),
        current_timestamp().as("processed_at")
      )
  }


  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    try {
      val kafkaDF = readFromKafka(spark)
      val processedDF = processTweets(kafkaDF)


      val query = processedDF.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      sys.addShutdownHook {
        println("Shutting down gracefully...")
        query.stop()
        spark.stop()
      }

      query.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"Error processing tweets: ${e.getMessage}")
        e.printStackTrace()
        spark.stop()
        System.exit(1)
    }
  }
}
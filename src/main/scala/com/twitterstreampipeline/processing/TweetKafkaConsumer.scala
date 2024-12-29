package com.twitterstreampipeline.processing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

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

  private val cleanTextUdf = udf((text: String) => {
    val withoutHttpUrls = text.replaceAll("https?://\\S+", "")

    val withoutWwwUrls = withoutHttpUrls.replaceAll("www\\.\\S+", "")

    withoutWwwUrls.replaceAll("\\s+", " ").trim
  })

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
      .option("startingOffsets", "earliest")
      .load()
  }

  def processTweets(df: DataFrame, pipeline: PretrainedPipeline): DataFrame = {
    val sentimentUdf = udf((text: String) => {
      try {
        val result = pipeline.annotate(text)
        if (result.nonEmpty && result.contains("sentiment")) {
          val sentiment = result("sentiment").headOption.getOrElse("unknown")
          sentiment match {
            case "positive" => "positive"
            case "negative" => "negative"
            case _ => "neutral"
          }
        } else {
          "neutral"
        }
      } catch {
        case e: Exception =>
          println(s"Error analyzing sentiment for text: $text. Error: ${e.getMessage}")
          "neutral"
      }
    })

    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), tweetSchema).as("tweet"))
      .select(
        col("tweet.id_str").as("id"),
        cleanTextUdf(col("tweet.text")).as("text"),
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
      .withColumn("sentiment", sentimentUdf(col("text")))
  }

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val pipeline = new PretrainedPipeline("analyze_sentiment")

    try {
      val kafkaDF = readFromKafka(spark)
      val processedDF = processTweets(kafkaDF, pipeline)

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
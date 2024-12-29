import com.google.cloud.language.v1.{LanguageServiceClient, Document, AnalyzeSentimentRequest, AnalyzeSentimentResponse}
import com.google.cloud.language.v1.Document.Type
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object TweetKafkaConsumer {

  def analyzeSentiment(text: String): Double = {
    if (text == null || text.trim.isEmpty) return 0.0

    val document = Document.newBuilder()
      .setContent(text)
      .setType(Type.PLAIN_TEXT)
      .build()

    val client = LanguageServiceClient.create()
    try {
      val request = AnalyzeSentimentRequest.newBuilder()
        .setDocument(document)
        .build()

      val response: AnalyzeSentimentResponse = client.analyzeSentiment(request)
      response.getDocumentSentiment.getScore
    } catch {
      case e: Exception =>
        println(s"Error in sentiment analysis: ${e.getMessage}")
        0.0
    } finally {
      client.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaTweetConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    spark.udf.register("analyzeSentiment", analyzeSentiment _)

    val tweetSchema = new StructType()
      .add("created_at", StringType)
      .add("entities", new StructType()
        .add("hashtags", ArrayType(new StructType()
          .add("indices", ArrayType(LongType))
          .add("text", StringType)))
      )
      .add("coordinates", new StructType()
        .add("coordinates", ArrayType(DoubleType))
        .add("type", StringType))
      .add("id", LongType)
      .add("text", StringType)
      .add("user", new StructType()
        .add("screen_name", StringType)
        .add("location", StringType))

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter-tweets")
      .option("startingOffsets", "earliest")
      .load()


    val processedTweets = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .select(F.from_json(F.col("value"), tweetSchema).as("tweet"))
      .select(
        $"tweet.id".as("id"),
        $"tweet.text".as("text"),
        $"tweet.created_at".as("created_at"),
        $"tweet.user.screen_name".as("user"),
        $"tweet.user.location".as("location"),
        F.expr("transform(tweet.entities.hashtags, h -> h.text)").as("hashtags")
      )

      .withColumn("sentiment_score", F.callUDF("analyzeSentiment", $"text"))
      .withColumn("sentiment",
        F.when($"sentiment_score" > 0.1, "positive")
          .when($"sentiment_score" < -0.1, "negative")
          .otherwise("neutral"))


    val query = processedTweets
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}
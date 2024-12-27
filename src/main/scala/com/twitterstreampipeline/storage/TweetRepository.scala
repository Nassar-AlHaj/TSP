package com.twitterstreampipeline.storage

import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.model._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import ExecutionContext.Implicits.global

object TweetRepository {
  private val logger = LoggerFactory.getLogger(getClass)

  // Use the collection from MongoDBConnector
  private val collection: MongoCollection[BsonDocument] = MongoDBConnector.getCollection("tweets")

  // Convert Observable to Future
  private def observableToFuture[T](observable: Observable[T]): Future[T] = {
    val promise = Promise[T]()
    observable.subscribe(
      (result: T) => promise.success(result),
      (error: Throwable) => promise.failure(error),
      () => ()
    )
    promise.future
  }

  // Create indexes for efficient querying
  def createIndexes(): Future[Seq[String]] = {
    val indexes = List(
      IndexModel(Indexes.ascending("userId"), IndexOptions().name("user_index")),
      IndexModel(Indexes.ascending("username"), IndexOptions().name("username_index")),
      IndexModel(Indexes.ascending("timestamp"), IndexOptions().name("timestamp_index")),
      IndexModel(Indexes.ascending("hashtags"), IndexOptions().name("hashtag_index")),
      IndexModel(Indexes.text("text"), IndexOptions().name("text_search_index")),
      IndexModel(Indexes.ascending("sentiment.label"), IndexOptions().name("sentiment_index")),
      IndexModel(Indexes.ascending("language"), IndexOptions().name("language_index"))
    )

    collection.createIndexes(indexes).toFuture().map { result =>
      logger.info(s"Created ${result.size} indexes successfully.")
      result
    }.recover {
      case e: Exception =>
        logger.error(s"Error creating indexes: ${e.getMessage}")
        throw e
    }
  }




  def storeTweet(tweetData: Map[String, Any]): Future[Unit] = {
    try {
      // Build BSON document with all required fields
      val document = BsonDocument(
        "id" -> BsonString(tweetData("id").toString),
        "text" -> BsonString(tweetData("text").toString),
        "userId" -> BsonString(tweetData.getOrElse("userId", "").toString),
        "username" -> BsonString(tweetData.getOrElse("username", "").toString),
        "timestamp" -> BsonString(tweetData.getOrElse("timestamp", "").toString),
        "hashtags" -> (tweetData.get("hashtags") match {
          case Some(tags: Seq[_]) =>
            BsonArray(tags.map(tag => BsonString(tag.toString)))
          case Some(tags: String) =>
            BsonArray(tags.split(",").map(tag => BsonString(tag.trim)))
          case _ => BsonArray()
        }),
        "language" -> BsonString(tweetData.getOrElse("language", "").toString),
        "sentiment" -> BsonDocument(
          "label" -> BsonString(tweetData.getOrElse("sentiment_label", "neutral").toString),
          "score" -> BsonInt32(tweetData.getOrElse("sentiment_score", 0).toString.toInt)
        ),
        "processed_at" -> BsonString(System.currentTimeMillis().toString)
      )

      // Insert the document into MongoDB
      collection.insertOne(document).toFuture().map { _ =>
        logger.info(s"Stored tweet with ID: ${tweetData("id")}")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error building or storing tweet: ${e.getMessage}")
        Future.failed(e)
    }
  }




  // Batch store multiple tweets
  def storeTweets(tweetsData: Seq[Map[String, Any]]): Future[Unit] = {
    try {
      val documents = tweetsData.map { tweetData =>
        // Build BSON document with fallback values for missing fields
        val document = BsonDocument(
          "id" -> BsonString(tweetData.getOrElse("id", "").toString),
          "text" -> BsonString(tweetData.getOrElse("text", "").toString),
          "userId" -> BsonString(tweetData.getOrElse("userId", "").toString),
          "username" -> BsonString(tweetData.getOrElse("username", "").toString),
          "timestamp" -> BsonString(tweetData.getOrElse("timestamp", "").toString),
          "hashtags" -> (tweetData.get("hashtags") match {
            case Some(tags: Seq[_]) => BsonArray(tags.map(tag => BsonString(tag.toString)))
            case Some(tags: String) => BsonArray(tags.split(",").map(tag => BsonString(tag.trim)))
            case _ => BsonArray()
          }),
          "language" -> BsonString(tweetData.getOrElse("language", "").toString),
          "sentiment" -> BsonDocument(
            "label" -> BsonString(tweetData.getOrElse("sentimentLabel", "neutral").toString),
            "score" -> BsonDouble(tweetData.getOrElse("sentimentScore", 0.0).toString.toDouble)
          ),
          "processed_at" -> BsonString(System.currentTimeMillis().toString)
        )

        // Return the document
        document
      }

      // Insert the documents into MongoDB
      collection.insertMany(documents).toFuture().map { _ =>
        logger.info(s"Stored ${documents.size} tweets in batch.")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error building or storing tweets in batch: ${e.getMessage}")
        Future.failed(e)
    }
  }
}
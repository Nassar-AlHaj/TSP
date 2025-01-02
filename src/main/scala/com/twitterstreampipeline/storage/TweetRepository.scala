package com.twitterstreampipeline.storage

import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.model._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global

object TweetRepository {
  private val logger = LoggerFactory.getLogger(getClass)

  private val collection: MongoCollection[BsonDocument] = MongoDBConnector.getCollection("tweets")












  def storeTweets(tweetsData: Seq[Map[String, Any]]): Future[Unit] = {
    try {
      val validDocuments = tweetsData.filter(tweetData => tweetData.contains("id") && tweetData.contains("text")).map { tweetData =>
        BsonDocument(
          "id" -> BsonString(tweetData.getOrElse("id", "").toString),
          "text" -> BsonString(tweetData.getOrElse("text", "").toString),
          "username" -> BsonString(tweetData.getOrElse("username", "").toString),
          "timestamp" -> BsonString(tweetData.getOrElse("timestamp", "").toString),
          "hashtags" -> (tweetData.get("hashtags") match {
            case Some(tags: Seq[_]) => BsonArray(tags.map(tag => BsonString(tag.toString)))
            case Some(tags: String) => BsonArray(tags.split(",").map(tag => BsonString(tag.trim)))
            case _ => BsonArray()
          }),
          "sentiment" -> BsonDocument(
            "label" -> BsonString(tweetData.getOrElse("sentimentLabel", "neutral").toString)
          ),
          "created_at" -> BsonString(System.currentTimeMillis().toString)
        )
      }

      if (validDocuments.nonEmpty) {
        collection.insertMany(validDocuments).toFuture().map { _ =>
          logger.info(s"Stored ${validDocuments.size} tweets in batch.")
        }.recover {
          case e: Exception =>
            logger.error(s"Failed to store batch of tweets: ${e.getMessage}")
            throw e
        }
      } else {
        Future.successful(logger.info("No valid tweets to store in this batch."))
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error building or storing tweets in batch: ${e.getMessage}")
        Future.failed(e)
    }
  }


}
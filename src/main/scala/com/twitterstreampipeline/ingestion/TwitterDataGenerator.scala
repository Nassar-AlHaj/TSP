package com.twitterstreampipeline.ingestion
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.io.Source
import scala.util.matching.Regex

case class UserEntity(
                       id: Long,
                       id_str: String,
                       name: String,
                       screen_name: String,
                       location: Option[String],
                       description: Option[String]
                     )

case class HashtagEntity(text: String, indices: List[Int])
case class UrlEntity(
                      url: String,
                      expanded_url: String,
                      display_url: String,
                      indices: List[Int]
                    )
case class Entities(
                     hashtags: List[HashtagEntity],
                     urls: List[UrlEntity],
                     user_mentions: List[JsValue],
                     symbols: List[JsValue]
                   )

case class Tweet(
                  created_at: String,
                  id: Long,
                  id_str: String,
                  text: String,
                  truncated: Boolean,
                  entities: Entities,
                  source: String,
                  user: UserEntity,
                  geo_enabled: Option[Boolean] = None,
                  coordinates: Option[JsValue] = None
                )

object TwitterJsonProtocol extends DefaultJsonProtocol {
  implicit val hashtagFormat: RootJsonFormat[HashtagEntity] = jsonFormat2(HashtagEntity)
  implicit val urlEntityFormat: RootJsonFormat[UrlEntity] = jsonFormat4(UrlEntity)
  implicit val entitiesFormat: RootJsonFormat[Entities] = jsonFormat4(Entities)
  implicit val userEntityFormat: RootJsonFormat[UserEntity] = jsonFormat6(UserEntity)
  implicit val tweetFormat: RootJsonFormat[Tweet] = jsonFormat10(Tweet)
}

class TwitterDataGenerator(filePath: String) {
  import TwitterJsonProtocol._

  private var tweets: List[Tweet] = loadTweets()
  private var currentIndex = 0


  private def removeUrlsFromText(text: String): String = {

    val urlPattern: Regex = """http[s]?://\S+""".r
    urlPattern.replaceAllIn(text, "")
  }


  private def removeUrlsFromEntities(entities: Entities): Entities = {
    entities.copy(urls = List())
  }


  private def removeUrlsFromSource(source: String): String = {

    val urlPattern: Regex = """<a href=["'](http[^"']+)["'][^>]*>[^<]+</a>""".r
    urlPattern.replaceAllIn(source, "")
  }

  private def loadTweets(): List[Tweet] = {
    try {
      val source = Source.fromFile(s"src/main/resources/data/$filePath")
      val content = source.getLines().map { line =>
        try {

          val tweet = line.parseJson.convertTo[Tweet]


          val cleanedText = removeUrlsFromText(tweet.text)


          val cleanedEntities = removeUrlsFromEntities(tweet.entities)


          val cleanedSource = removeUrlsFromSource(tweet.source)


          val cleanedTweet = tweet.copy(text = cleanedText, entities = cleanedEntities, source = cleanedSource)
          Some(cleanedTweet)
        } catch {
          case e: Exception =>
            println(s"Error parsing tweet: ${e.getMessage}")
            None
        }
      }.flatten.toList

      source.close()
      content
    } catch {
      case ex: Exception =>
        println(s"Error loading tweets file: ${ex.getMessage}")
        List.empty[Tweet]
    }
  }

  def nextTweet(): Option[Tweet] = {
    if (tweets.isEmpty) {
      None
    } else {
      val tweet = tweets(currentIndex)
      currentIndex = (currentIndex + 1) % tweets.length
      Some(tweet)
    }
  }

  def reset(): Unit = {
    currentIndex = 0
  }

  def getTotalTweets: Int = tweets.length
}

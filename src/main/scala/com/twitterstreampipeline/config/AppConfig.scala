package com.twitterstreampipeline.config

import com.typesafe.config.ConfigFactory

import scala.util.Try

case class TwitterConfig(
                          dataFilePath: String,
                          batchSize: Int,
                          streamingDelayMs: Int
                        )

case class ProcessingConfig(
                             enableSentimentAnalysis: Boolean,
                             maxTweetsPerBatch: Int,
                             processingIntervalMs: Int
                           )

case class MongoConfig(
                        uri: String,
                        database: String,
                        collection: String,
                        username: Option[String],
                        password: Option[String]
                      )

case class AppConfig(
                      kafka: KafkaConfig,
                      twitter: TwitterConfig,
                      processing: ProcessingConfig,
                      mongo: MongoConfig
                    )

object AppConfig {
  /**
   * Loads application configuration from application.conf
   * with fallback to reference.conf
   */
  def load(): AppConfig = {
    val config = ConfigFactory.load()

    def getOptionalString(path: String): Option[String] = {
      Try(config.getString(path)).toOption
    }

    AppConfig(
      kafka = KafkaConfig(
        bootstrapServers = config.getString("app.kafka.bootstrap-servers"),
        topic = config.getString("app.kafka.topic"),
        clientId = config.getString("app.kafka.client-id"),
        batchSize = config.getString("app.kafka.batch-size"),
        bufferMemory = config.getString("app.kafka.buffer-memory")
      ),

      twitter = TwitterConfig(
        dataFilePath = config.getString("app.twitter.data-file-path"),
        batchSize = config.getInt("app.twitter.batch-size"),
        streamingDelayMs = config.getInt("app.twitter.streaming-delay-ms")
      ),

      processing = ProcessingConfig(
        enableSentimentAnalysis = config.getBoolean("app.processing.enable-sentiment-analysis"),
        maxTweetsPerBatch = config.getInt("app.processing.max-tweets-per-batch"),
        processingIntervalMs = config.getInt("app.processing.interval-ms")
      ),

      mongo = MongoConfig(
        uri = config.getString("app.mongo.uri"),
        database = config.getString("app.mongo.database"),
        collection = config.getString("app.mongo.collection"),
        username = getOptionalString("app.mongo.username"),
        password = getOptionalString("app.mongo.password")
      )
    )
  }}
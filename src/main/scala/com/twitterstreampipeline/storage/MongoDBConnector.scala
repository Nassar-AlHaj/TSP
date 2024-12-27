package com.twitterstreampipeline.storage

import org.mongodb.scala._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.mongodb.scala.bson.BsonDocument

object MongoDBConnector {
  private val logger = LoggerFactory.getLogger(getClass)

  // Load configuration from application.conf
  private val config: Config = ConfigFactory.load()

  // Configurable connection parameters
  private val connectionString: String = config.getString("mongodb.connection.url")  // Corrected
  private val databaseName: String = config.getString("mongodb.database.name")        // Corrected

  // Create MongoDB client with robust error handling
  private val mongoClient: MongoClient = try {
    MongoClient(connectionString)
  } catch {
    case e: Exception =>
      logger.error(s"Failed to establish MongoDB connection: ${e.getMessage}")
      throw e
  }

  // Get database instance
  def getDatabase: MongoDatabase = {
    try {
      mongoClient.getDatabase(databaseName)
    } catch {
      case e: Exception =>
        logger.error(s"Error accessing database $databaseName: ${e.getMessage}")
        throw e
    }
  }

  // Get collection with appropriate type parameter
  def getCollection(collectionName: String): MongoCollection[BsonDocument] = {
    try {
      getDatabase.getCollection[BsonDocument](collectionName)
    } catch {
      case e: Exception =>
        logger.error(s"Error accessing collection $collectionName: ${e.getMessage}")
        throw e
    }
  }

  // Close MongoDB connection
  def close(): Unit = {
    try {
      mongoClient.close()
      logger.info("MongoDB connection closed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Error closing MongoDB connection: ${e.getMessage}")
    }
  }
}

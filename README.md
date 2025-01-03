# Twitter Stream Processing Pipeline

A scalable system that ingests real-time tweets, processes them to extract insights such as sentiment and hashtags, and visualizes the results through an interactive web application.

## Table of Contents
- [Project Overview](#project-overview)
- [System Architecture](#system-architecture)
- [Installation](#installation)
- [Dependencies](#dependencies)
- [Configuration](#configuration)
- [Running the Services](#running-the-services)
- [Components](#components)

## Project Overview

The **Twitter Stream Processing Pipeline** is a scalable 
system that ingests real-time tweets, processes them to 
extract insights such as sentiment and hashtags, and visualizes 
the results through an interactive web application. Leveraging 
big data technologies, the system combines real-time processing,
efficient data storage, and advanced analytics to ensure robust 
performance and user-friendly interaction.

### 1. Stream Ingestion
- Simulates tweet streams by reading from boulder_flood_geolocated_tweets.json
- Continuously cycles through tweets to maintain constant data flow
- Implements Kafka Producer to buffer tweets and ensure scalable processing
- Configurable streaming rate with built-in error handling and retry mechanisms

### 2. Processing Components
- Apache Spark Streaming for real-time tweet processing
- Field Extraction and Processing:
    - Text: Cleaned, removing URLs and extra whitespace
    - Timestamp: Converted from Twitter format to standardized timestamp
    - Geo-coordinates: Extracted longitude/latitude from coordinates field
    - Hashtags: Parsed from entities.hashtags array
    - Username: Extracted from user.screen_name
    - User Location: Captured from user.location
    - Sentiment analysis using John Snow Labs' pretrained pipeline

### 3. Storage

MongoDB Implementation:
- Database: TweetStorage
- Primary collection: "tweets"

Document Schema:
```json
{
  "id": "String",
  "text": "String",
  "username": "String",
  "timestamp": "String",
  "hashtags": ["String"],
  "sentiment": {
    "label": "String"
  },
  "created_at": "String"
}
```

Indexes:
- Text index on tweet content for keyword search
- Geospatial index on location for map rendering
- Timestamp index for temporal queries
- Compound index on sentiment and timestamp for trend analysis

### 4. Web Interface Implementation

Features:
- Keyword Search: Input field for searching tweets containing specific keywords
- Map Visualization: Interactive map rendering of tweets based on geo-coordinates
- Trend Analysis: Temporal distribution of tweets with hourly and daily aggregations
- Sentiment Gauge: Interactive sentiment analysis gauge for average sentiment scores

Data Flow:
```
Tweet Source → Kafka → Spark Streaming → MongoDB
     ↓            ↓           ↓              ↓
Simulation    Buffering   Processing      Storage
                                            ↓
                                         Web app
```

## Installation

### Prerequisites
- Download and install:
    - Zookeeper
    - MongoDB
    - Apache Kafka
    - Apache Spark
    - JDK: Java 11

### Build Configuration

build.sbt:
```scala
ThisBuild / version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "TSP"
  )

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.apache.kafka" % "kafka-clients" % "3.6.1",
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.apache.spark" %% "spark-streaming" % "3.5.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.1",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.9.0",
  "io.spray" %% "spray-json" % "1.3.6",
  "com.google.cloud" % "google-cloud-language" % "2.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.spark" %% "spark-mllib" % "3.5.3"
)
```

### Application Configuration
```hocon
mongodb {
  connection {
    url = "mongodb://localhost:27017/"
  }
  database {
    name = "TweetStorage"
  }
}
```

### Web Application Setup

Install the following npm packages:
```bash
npm install axios@^1.7.9
npm install chart.js@^4.4.7
npm install chartjs-plugin-zoom@^2.2.0
npm install cra-template@1.2.0
npm install leaflet@^1.9.4
npm install react@^18.3.1
npm install react-chartjs-2@^5.2.0
npm install react-dom@^18.3.1
npm install react-leaflet@^4.2.1
npm install react-scripts@5.0.1
npm install web-vitals@^4.2.4
npm install body-parser@^1.20.3
npm install chartjs-chart-financial@^0.2.1
npm install cors@^2.8.5
npm install express@^4.21.2
npm install mongodb@^6.12.0
npm install mongoose@^8.9.3
```

## Running the Services

### 1. Start Kafka Services
```bash
# Start Zookeeper
zookeeper-server-start.sh /path/to/zookeeper.properties

# Start Kafka broker
kafka-server-start.sh /path/to/server.properties
```

### 2. Spark Application Components

- SparkSession Setup for Spark Streaming
- Data Processing using DataFrame and UDF for sentiment extraction
- Integration with Kafka Producer/Consumer
- MongoDB storage implementation

### 3. Web Application

Frontend Components:
- Search bar for tweet queries
- Interactive map visualization
- Temporal distribution graph
- Real-time sentiment analysis gauge

Backend Features:
- API endpoints for database queries
- Real-time data streaming
- Integration with MongoDB




## Figure showing the different parts of the pipeline along with details for each part :

![image](https://github.com/user-attachments/assets/c9624393-2376-4543-b71e-69da66759ffe)




## Components Details

### Producer
- Subscribe messaging system and queue
- Handles high-volume data transmission
- Ensures data persistence and message replication
- Integrates with Apache Spark

### Apache Kafka's Dependency: Apache Zookeeper
- Distributed configuration and synchronization service
- Coordinates between Kafka brokers and consumers
- Stores essential metadata
- Maintains fault-tolerant state

### Consumer
- Integrates with Kafka for data streaming
- Processes data using Spark DataFrame
- Performs sentiment analysis and data cleaning
- Stores processed data in MongoDB

### Storage
MongoDB serves as the distributed storage system for processed data, with optimized indexes for tweet retrieval.

### Web Application
Combines frontend visualization tools with backend API services for real-time data access and display.

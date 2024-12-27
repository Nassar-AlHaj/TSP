ThisBuild / version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "TSP"
  )

libraryDependencies ++= Seq(
  // Apache Kafka
  "org.apache.kafka" % "kafka-clients" % "3.6.1",
  // Spark Libraries
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.apache.spark" %% "spark-streaming" % "3.5.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
  // NLP Libraries
  "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.1",
  // MongoDB Driver
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.9.0",
  // JSON Parsing
  "io.spray" %% "spray-json" % "1.3.6",
  // Google Cloud NLP
  "com.google.cloud" % "google-cloud-language" % "2.3.0",
  // Logging
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  // Miscellaneous
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.typesafe" % "config" % "1.4.2",
"org.apache.spark" %% "spark-mllib" % "3.5.3"
)


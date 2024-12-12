
ThisBuild / version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "TSP"
  )
libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.apache.spark" %% "spark-streaming" % "3.5.3",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.1",

)
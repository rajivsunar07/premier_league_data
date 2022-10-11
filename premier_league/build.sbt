ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "com.datastax.spark" % "spark-cassandra-connector_2.12" % "3.2.0"

)

lazy val root = (project in file("."))
  .settings(
    name := "premier_league"



  )

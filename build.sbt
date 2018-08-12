name := "spark-playground"

version := "0.1"

scalaVersion := "2.12.6"

val sparkVersion = "2.2.1"

scalacOptions += "-Yresolve-term-conflict:strategyrm"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

val devDependencies = Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.6",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.1",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

lazy val common = project 
  .settings(libraryDependencies ++= sparkDependencies ++ devDependencies)

lazy val stackoverflow_survey = project
  .settings(libraryDependencies ++= sparkDependencies)
  .dependsOn(common)



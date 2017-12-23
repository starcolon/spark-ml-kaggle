name := "spark-playground"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

lazy val common = project 
  .settings(libraryDependencies ++= sparkDependencies)

lazy val stackoverflow_survey = project
  .settings(libraryDependencies ++= sparkDependencies)
  .dependsOn(common)



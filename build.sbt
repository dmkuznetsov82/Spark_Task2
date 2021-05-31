name := "SparkTask2"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.1.1",
  "org.apache.spark" % "spark-sql_2.12" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1" % Test,
  "org.apache.spark" %% "spark-avro" % "3.1.1"
)

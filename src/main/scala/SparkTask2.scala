package com.epam.sample.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions._

import java.io.File
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.avro._
import org.apache.avro.Schema
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger

import java.sql.Timestamp
import java.text



case class Record(key: Int, value: String)

/** Computes an approximation to pi */
object SparkTask2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkTask2")
    val sc = new SparkContext(conf)


    val spark = SparkSession
      .builder
      .appName("SparkTask2")
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

    val schema = new StructType().add("id", "string")
      .add("name", "string")
      .add("country", "string")
      .add("city", "string")
      .add("address", "string")
      .add("latitude", "string")
      .add("longitude", "string")
      .add("geohash", "string")
      .add("avg_tmpr_f", "string")
      .add("avg_tmpr_c", "string")
      .add("wthr_date", "string")


    val expedia = spark.read
      .format("avro")
      .load("/user/dkuzniatsou/Homework/Spark_Homework/")

    val expediaSchema = expedia.printSchema()
    val expedia2016 = expedia.filter("year = 2016").toDF()

    val hotelsWeather = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host.docker.internal:9094")
      .option("subscribe", "streams-wordcount-output_5")
      .option("startingOffsets","earliest")
      .load()

    val hotelsParsed = hotelsWeather.withColumn("hotels", // nested structure with our json
      from_json($"value".cast(StringType), schema))
      .selectExpr("hotels.*", "partition", "offset").toDF()

    val expediaHotels = expedia2016.join( hotelsParsed, (expedia2016("hotel_id") === hotelsParsed("id") &&
      expedia2016("srch_ci") === hotelsParsed("wthr_date")) )
      .filter(hotelsParsed("avg_tmpr_c") > 0 )

    val expediaHotelsDuration = expediaHotels.withColumn("Duration", datediff($"srch_co",$"srch_ci"))

    val intialStateDurationWithTimestamp = expediaHotelsDuration.withColumn("batch_timestamp",current_timestamp())

   // val states = intialStateDurationWithTimestamp.withWatermark("batch_timestamp", "30 seconds")
   //   .groupBy("hotel_id","batch_timestamp")
   val states = intialStateDurationWithTimestamp
     .groupBy("hotel_id")
      .agg(
        first($"batch_timestamp").alias("batch_timestamp"),
        sum($"srch_children_cnt").alias("children_cnt"),
        count(when($"Duration" <= 0 || $"Duration" > 30 || $"Duration" === null, 1)).alias("erroneous_data_cnt"),
        count(when($"Duration" === 1, 1)).alias("short_stay_cnt"),
        count(when($"Duration" > 1 && $"Duration" <=7, 1)).alias("standart_stay_cnt"),
        count(when($"Duration" > 7 && $"Duration" <=14, 1)).alias("standart_extended_stay_cnt"),
        count(when($"Duration" > 14 && $"Duration" <=30, 1)).alias("long_stay_cnt")
      )
    states.show(30,false)

    val numCols = states.columns.slice(3,8)

    val maxCount = states.withColumn("max_count", greatest(numCols.head, numCols.tail: _*))
    val intialStateWithTimestamp = maxCount.withColumn("most_popular_stay_type", when( $"erroneous_data_cnt" === $"max_count","erroneous_data_cnt")
      .when( $"short_stay_cnt" === $"max_count","short_stay_cnt")
      .when( $"standart_stay_cnt" === $"max_count","standart_stay_cnt")
      .when( $"standart_extended_stay_cnt" === $"max_count","standart_extended_stay_cnt")
      .when( $"long_stay_cnt" === $"max_count","long_stay_cnt")
    )

    intialStateWithTimestamp.withColumn("Stream2016",lit("Stream2016")).show(30,false)

    val userSchema = expedia.schema

    val expediaStreaming2017 = spark.readStream
      .schema(userSchema)
      .format("avro")
      .load("/user/dkuzniatsou/Homework/Spark_Homework/")
      .filter("year = 2017")

    val expediaHotels2017 = expediaStreaming2017.join( hotelsParsed, (expediaStreaming2017("hotel_id") === hotelsParsed("id") &&
      expediaStreaming2017("srch_ci") === hotelsParsed("wthr_date")) )
      .filter(hotelsParsed("avg_tmpr_c") > 0 )

    val expediaHotelsDuration2017 = expediaHotels2017.withColumn("Duration", datediff($"srch_co",$"srch_ci"))
    val states2017WithTimestamp = expediaHotelsDuration2017.withColumn("batch_timestamp",$"date_time".cast(TimestampType).as("timestamp"))

    val states2017 = states2017WithTimestamp.withWatermark("batch_timestamp", "3 seconds")
      .groupBy("hotel_id","batch_timestamp")
      .agg(
        sum($"srch_children_cnt").alias("children_cnt"),
        count(when($"Duration" <= 0 || $"Duration" > 30 || $"Duration" === null, 1)).alias("erroneous_data_cnt"),
        count(when($"Duration" === 1, 1)).alias("short_stay_cnt"),
        count(when($"Duration" > 1 && $"Duration" <=7, 1)).alias("standart_stay_cnt"),
        count(when($"Duration" > 7 && $"Duration" <=14, 1)).alias("standart_extended_stay_cnt"),
        count(when($"Duration" > 14 && $"Duration" <=30, 1)).alias("long_stay_cnt")
      )

   // val joinedStates = states2017.withWatermark("batch_timestamp","30 seconds")
    //  .join(broadcast(intialStateWithTimestamp).withWatermark("batch_timestamp","30 seconds"),
    val joinedStates = states2017
      .join(broadcast(intialStateWithTimestamp),
        states2017("hotel_id") === intialStateWithTimestamp("hotel_id"),"left")
      .select(
        states2017("hotel_id") as "hotel",
        states2017("batch_timestamp") as "batch_timestamp",
        coalesce(states2017("children_cnt"),lit(0))+ coalesce(intialStateWithTimestamp("children_cnt"),lit(0)) as "children_cnt",
        coalesce(states2017("erroneous_data_cnt"),lit(0))+ coalesce(intialStateWithTimestamp("erroneous_data_cnt"),lit(0)) as "erroneous_data_cnt",
        coalesce(states2017("short_stay_cnt"),lit(0))+ coalesce(intialStateWithTimestamp("short_stay_cnt"),lit(0)) as "short_stay_cnt",
        coalesce(states2017("standart_stay_cnt"),lit(0))+ coalesce(intialStateWithTimestamp("standart_stay_cnt"),lit(0)) as "standart_stay_cnt",
        coalesce(states2017("standart_extended_stay_cnt"),lit(0))+ coalesce(intialStateWithTimestamp("standart_extended_stay_cnt"),lit(0))
          as "standart_extended_stay_cnt",
        coalesce(states2017("long_stay_cnt"),lit(0))+ coalesce(intialStateWithTimestamp("long_stay_cnt"),lit(0)) as "long_stay_cnt"
      )

    val maxCount2017 = joinedStates.withColumn("max_count", greatest(numCols.head, numCols.tail: _*))
    val initialState2017 = maxCount2017.withColumn("most_popular_stay_type", when( $"erroneous_data_cnt" === $"max_count","erroneous_data_cnt")
      .when( $"short_stay_cnt" === $"max_count","short_stay_cnt")
      .when( $"standart_stay_cnt" === $"max_count","standart_stay_cnt")
      .when( $"standart_extended_stay_cnt" === $"max_count","standart_extended_stay_cnt")
      .when( $"long_stay_cnt" === $"max_count","long_stay_cnt")
    )

    val intialStateWithTimestamp2017 = initialState2017
      .withColumn("with_children",when($"children_cnt" > 0, "true").otherwise("false"))


    intialStateWithTimestamp2017.writeStream
      //.outputMode("complete")
      .format("console")
      .option("numRows",1000)
      .option("truncate", false)
      //.format("avro")
      //.option("checkpointLocation", "/user/dkuzniatsou/Homework/Spark_Checkpoints/")
      //.option("path","/user/dkuzniatsou/Homework/Spark_Homework2/")
      .start()
      .awaitTermination()

    sc.stop()
  }
}


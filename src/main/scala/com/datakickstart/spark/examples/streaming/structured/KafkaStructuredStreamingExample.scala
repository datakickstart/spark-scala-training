package com.datakickstart.spark.examples.streaming.structured

import org.apache.spark.sql.SparkSession

import com.datakickstart.common.VehicleStops.{VehicleStop, VehicleStopSchema}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

/** Example that reads from topic and prints to console
  *   To run locally (from project folder):
  *   mvn install
  *   spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --class com.datakickstart.spark.examples.streaming.structured.KafkaStructuredStreamingExample target/spark-training-1.0-SNAPSHOT.jar
  */
object KafkaStructuredStreamingExample extends App {

  val topic = "vehicle-stops"
  val bootstrapServers = "localhost:9092"

  val spark = SparkSession.builder()
    .appName("Kafka Structured Streaming Example")
    .master("local[*]")
    .getOrCreate()

  val inputData = spark
    .readStream
    .format("kafka")
    .option("subscribe", topic)
    .option("kafka.bootstrap.servers", bootstrapServers)
    .load()

  import spark.implicits._

  val keyValueData = inputData.selectExpr("CAST(key as STRING)", "CAST(value as STRING)")

  val vehicleStopsJson = keyValueData.select(
    from_json(col("value"), VehicleStopSchema).as("record")
  )

  val vehicleStops = vehicleStopsJson.selectExpr("record.*").as[VehicleStop] //.as[VehicleStopRaw]

  // Suppress most logs while writing to console
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val streamQuery = vehicleStops
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate","false")
    .start()

  streamQuery.awaitTermination()
}

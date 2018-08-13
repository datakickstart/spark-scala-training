package com.datakickstart.spark.examples.streaming

import com.datakickstart.spark.examples.VehicleStops
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds


object SparkStructuredStreamingExamples extends App {
  val logFilePath = "file:///opt/data/sample_data/streaming_example/input" // Should be some file on your system
  val streamingEndpointBase = "localhost"
  val port = 3001
  val batchDuration = Seconds(4)

  val spark = SparkSession.builder()
    .appName("Spark Structured Streaming Example")
    .master("local[*]")
    .getOrCreate()

  val schema = readSchemaFromFile(logFilePath)

//  val stream = spark.readStream.format("csv").option("header","true").schema(VehicleStopRawSchema).load(logFilePath) //ssc.textFileStream(logFilePath)
  val stream = spark.readStream.format("csv").option("header","true").schema(schema).load(logFilePath)

//  stream.show()

  val streamQuery = stream.writeStream.format("console").outputMode("append").start()
  streamQuery.awaitTermination()

  def readSchemaFromFile(path: String): StructType = {
    spark.read.option("header","true").format("csv").load(logFilePath).schema
  }

  def getSchema(): StructType = return VehicleStops.VehicleStopRawSchema
}

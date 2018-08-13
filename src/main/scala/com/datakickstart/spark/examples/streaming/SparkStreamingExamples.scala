package com.datakickstart.spark.examples.streaming

import java.lang.management.ManagementFactory

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingExamples extends App {
  val logFilePath = "file:///opt/data/sample_data/streaming_example/input" // Should be some file on your system
  val streamingEndpointBase = "localhost"
  val port = 3001
  val batchDuration = Seconds(4)

  val conf = new SparkConf().setAppName("Spark Streaming Example")

  // Check if running from IDE, if so set master to local
  if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
    conf.setMaster("local[*]")
  }

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, batchDuration )

  // source from text files being added to folder
  val input = ssc.textFileStream(logFilePath)

  input.print()

  ssc.start
  ssc.awaitTermination

}

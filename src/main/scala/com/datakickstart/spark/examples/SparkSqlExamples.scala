package com.datakickstart.spark.examples

import java.lang.management.ManagementFactory

import com.datakickstart.spark.examples.SparkExamples.conf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlExamples extends App {

  val conf = new SparkConf().setAppName("Spark SQL Example")

  // Check if running from IDE, if so set master to local
  if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
    conf.setMaster("local[*]")
  }

  val spark = SparkSession
    .builder()
    .config(conf)
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val cvDF = spark.read.option("header","true").csv("src/main/resources/vehicle_stops_2016_datasd.csv").as[VehicleStopRaw]
  val r = cvDF.show()
}

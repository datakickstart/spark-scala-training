package com.datakickstart.spark.examples.batch

import java.lang.management.ManagementFactory

import com.datakickstart.spark.examples.VehicleStops
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SparkExamples extends App {
  val logFile = "src/main/resources/vehicle_stops_2016_datasd.csv" // Should be some file on your system

  val conf = new SparkConf().setAppName("SparkExample1")

  // Check if running from IDE, if so set master to local
  if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
    conf.setMaster("local[*]")
  }

  def getIfExists(a: Array[String], index: Int): String = {
    Try (a(index)).getOrElse(null)
  }

  val sc = new SparkContext(conf)
  val input = sc.textFile(logFile)

  // if header exists, run next 2 lines
  val header = input.first() //extract header
  val data = input.filter(record => record != header)

  // for testing, print out data
  //val x = data.collect()
  //println(x.toList.toString())

  val vehicleStopRDD = data.map { line =>
    val record = line.split(",")
    val MS_IN_HOUR = 1000 * 60 * 60

    VehicleStops.VehicleStop(record(0), record(1), record(2), record(3), record(4), record(5),
      record(6), record(7), record(8), getIfExists(record, 9), getIfExists(record, 10),
      getIfExists(record,11), getIfExists(record, 12), getIfExists(record, 13), getIfExists(record, 14)
    )
  }

  // Save clean csv with only columns that are always populated
  //  val vehicleStopText = vehicleStopRDD.map(v => Array(v.stopId, v.stopCause, v.serviceArea, v.subjectRace, v.subjectSex, v.subjectAge, v.timestamp, v.stopDate, v.stopTime))
  //  vehicleStopText.map(a => a.mkString(",")).saveAsTextFile("file:///data1/tmp/vehicle_stops_2016_datasd_clean")

  // Example transformations and aggregations
  val keyedByViolationType = vehicleStopRDD.keyBy(a => (a.stopDate, a.stopCause)).cache()
  val violations = keyedByViolationType.mapValues(a => a.subjectRace)
    .distinct
    .countByKey

  println(violations)

}

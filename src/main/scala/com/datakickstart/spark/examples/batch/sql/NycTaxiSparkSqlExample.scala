package com.datakickstart.spark.examples.batch.sql

import java.lang.management.ManagementFactory
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.slf4j.{Logger, LoggerFactory}

object NycTaxiSparkSqlExample extends App {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  /* RUN APP */
  run()

  def run(): Unit = {
    /* log start time */
    val start = logAndReturnStart()

    /* set configuration */
    val conf = new SparkConf().setAppName("NYC Taxi Spark SQL")

    /* Check if running from IDE, if so set master to local */
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      conf.setMaster("local[*]")
    }

    /* core job io and processing */
    processData(conf)

    /* log ending time and duration */
    val end = logAndReturnEnd(start)
    val duration = logAndReturnDuration(start, end)
  }

  def logAndReturnStart(): Date = {
    val start = Calendar.getInstance().getTime()
    val startString: String = TIME_FORMAT.format(start)
    logger.info(s"Job monitoring: startTimestamp = ${startString}")
    return start
  }

  def logAndReturnEnd(start: Date): Date = {
    val end = Calendar.getInstance().getTime()
    val endString: String = TIME_FORMAT.format(end)
    logger.info(s"Job monitoring: endTimestamp = ${endString}")
    return end
  }

  def logAndReturnDuration(start: Date, end: Date): Unit = {
    val diffInSeconds = (end.getTime - start.getTime)/1000.0
    logger.info(s"Job monitoring: jobDuration = ${diffInSeconds.toString}s")
    return
  }

  def processData(conf: SparkConf): Unit = {
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val inputData = spark.read.option("header", "true").csv("/opt/data/sample_data/nyc_taxi/fhv_tripdata_2018-01.csv")
    inputData.show()

    val selectedData = inputData.selectExpr(
      "Dispatching_base_num",
      """CAST(UNIX_TIMESTAMP("Pickup_DateTime", "yyyy-dd-MM hh:mm:ss") AS TIMESTAMP) as pickupTimestamp""",
      """CAST(UNIX_TIMESTAMP("Pickup_DateTime", "yyyy-dd-MM hh:mm:ss") AS TIMESTAMP) as dropOffTimestamp""",
      "PUlocationID",
      "DOlocationID",
      "SR_Flag"
    )

    val processedData = selectedData
      .withColumn("duration", datediff(col("dropOffTimestamp"), col("pickupTimestamp")))
      .agg(sum("duration"))

    processedData.write.mode(SaveMode.Overwrite).csv("/opt/data/sample_data/nyc_taxi/output")
  }
}

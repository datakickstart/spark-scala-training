package com.datakickstart.common

import java.io.{FileNotFoundException, IOException}
import java.util
import java.util.Properties

import scala.io.Source
import scala.util.Try

import VehicleStops.VehicleStop
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.StringSerializer

object VehicleStopsWriter {
  @transient val logger = LogFactory.getLog(VehicleStopsWriter.getClass)

  private val JSON = new ObjectMapper

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    checkUsage(args)
    val topicName: String = args(0)
    val brokerList: String = args(1)
    val props: Properties = getProducerProps(brokerList)
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    // Repeatedly send transactions with a 1000 milliseconds wait in between
    val filename: String = "src/main/resources/vehicle_stops_2016_datasd.csv"
    val eventList: util.List[VehicleStop] = readEvents(filename, 1)
    val it: util.Iterator[VehicleStop] = eventList.iterator
    while ( {
      it.hasNext
    }) {
      val transaction: VehicleStop = it.next
      sendVehicleStop(transaction, kafkaProducer, topicName)
      Thread.sleep(1000)
    }
    kafkaProducer.close()
  }

  private def checkUsage(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: VehicleStopsWriter " + " <topic> <brokerList>")
      System.exit(1)
    }
  }


  private def sendVehicleStop(stop: VehicleStop, kafkaProducer: KafkaProducer[String, String], topicName: String): Unit = {
    logger.info("Putting stop: " + stop.toString)
    val gson = new Gson
    val record = new ProducerRecord(topicName, stop.stopId, gson.toJson(stop))
    val recordMetadata = kafkaProducer.send(record)
    logger.info(record.toString)
    try
      kafkaProducer.flush()
    catch {
      case ex: InterruptException =>
        logger.warn("Error sending record to Kafka, flush interrupted.", ex)
    }
  }

  def getProducerProps(brokerList: String): Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "VehicleStopsProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, new Integer(1))
    props
  }

  def readEvents(filename: String, count: Int): util.List[VehicleStop] = {
    val records = new util.ArrayList[VehicleStop]
    try {
      val reader = Source.fromFile(filename).bufferedReader()
      try {
        var line: String = null
        line = reader.readLine() // read header and do nothing with it

        // loop file
        while ({line = reader.readLine(); line != null}) {
          records.add(parse(line))
        }
      } catch {
        case e: FileNotFoundException =>
          System.out.println(e.toString)
      } finally reader.close()
    } catch {
      case e: IOException =>
        System.out.println(e.toString)
    }
    records
  }

  def getIfExists(a: Array[String], index: Int): String = {
    Try (a(index)).getOrElse(null)
  }

  def parse(recordString: String): VehicleStop = {
    val record: Array[String] = recordString.split("\\s*,\\s*")

    VehicleStop(record(0), record(1), record(2), record(3), record(4), record(5),
      record(6), record(7), record(8), getIfExists(record, 9), getIfExists(record, 10),
      getIfExists(record,11), getIfExists(record, 12), getIfExists(record, 13),
      getIfExists(record, 14))
  }
}

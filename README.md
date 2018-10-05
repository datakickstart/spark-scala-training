# Spark Scala Training by DataKickstart

## Purpose
This project exists as a place to view and test out Scala Spark examples with corresponding pom.xml file to be used with Maven to build locally.  Assumes using IntelliJ IDEA as IDE but should work with anything.   Spark examples can be found at src/main/scala/com/datakickstart/spark/examples.

Recommended starting points:
1. Spark Core Batch: [`Spark Core Example`](https://github.com/datakickstart/spark-scala-training/blob/master/src/main/scala/com/datakickstart/spark/examples/batch/core/BasicSparkExample.scala)
2. Spark Structure Streaming: [`Kafka Structure Streaming`](https://github.com/datakickstart/spark-scala-training/blob/master/src/main/scala/com/datakickstart/spark/examples/streaming/structured/KafkaStructuredStreamingExample.scala)

## Getting started locally
Recommendation is to use Docker in which case you can follow the instructions in order (Docker setup, Building project, Running pipeline).  You will need Java 8 and Maven installed locally.  If on a Mac, using homebrew should work but may have to specify versions.

### Docker setup
To test Kafka pipeline locally, you can use Docker and Docker Compose.  Once those are installed follow these steps:

1) Start docker (if not already running)
2) In a terminal window, navigate to project home directory (likely `spark-scala-training`)
3) Run command `docker-compose up -d zookeeper`
4) Run command `docker-compose up`
5) Leave this running in the terminal window and proceed with other steps in a new terminal window/tab

### Building project
1) Navigate to project home directory
2) Run command `mvn install`

### Running pipeline with local spark
1) Kick off the [`Kafka Structure Streaming`](https://github.com/datakickstart/spark-scala-training/blob/master/src/main/scala/com/datakickstart/spark/examples/streaming/structured/KafkaStructuredStreamingExample.scala) consuming pipeline with command `spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --class com.datakickstart.spark.examples.streaming.structured.KafkaStructuredStreamingExample target/spark-training-1.0-SNAPSHOT.jar`.
2) While the pipeline is running, produce messages to Kafka by running job `VehicleStopsWriter`(https://github.com/datakickstart/spark-scala-training/blob/master/src/main/scala/com/datakickstart/common/VehicleStopsWriter.scala) either from your IDE or with command `java -cp target/spark-training-1.0-SNAPSHOT.jar com.datakickstart.common.VehicleStopsWriter vehicle-stops localhost:9092`.

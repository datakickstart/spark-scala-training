package com.datakickstart.spark.examples

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object VehicleStops {
  case class VehicleStop(stopId: String,
                         stopCause: String,
                         serviceArea: String,
                         subjectRace: String,
                         subjectSex: String,
                         subjectAge: String,
                         timestamp: String,
                         stopDate: String,
                         stopTime: String,
                         sdResident: String,
                         arrested: String,
                         searched: String,
                         obtainedConsent: String,
                         contrabandFound: String,
                         propertySeized: String
                        )

  case class VehicleStopRaw(stop_id: String, stop_cause: String, service_area: String, subject_race: String,
                            subject_sex: String, subject_age: String, timestamp: String, stop_date: String,
                            stop_time: String, sd_resident: String, arrested: String, searched: String,
                            obtained_consent: String, contraband_found: String, property_seized: String)

  val VehicleStopRawSchema = StructType(
    StructField("stop_id", StringType, nullable = false) ::
      StructField("stop_cause", StringType, nullable = false) ::
      StructField("service_area", StringType, nullable = false) ::
      StructField("subject_race", StringType, nullable = false) ::
      StructField("subject_sex", StringType, nullable = false) ::
      StructField("subject_age", StringType, nullable = false) ::
      StructField("timestamp", StringType, nullable = false) ::
      StructField("stop_date", StringType, nullable = false) ::
      StructField("stop_time", StringType, nullable = false) ::
      StructField("sd_resident", StringType, nullable = false) ::
      StructField("arrested", StringType, nullable = false) ::
      StructField("searched", StringType, nullable = false) ::
      StructField("obtained_consent", StringType, nullable = false) ::
      StructField("contraband_found", StringType, nullable = false) ::
      StructField("property_seized", StringType, nullable = false) ::
      Nil)

}

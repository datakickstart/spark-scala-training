package com.datakickstart.spark

package object examples {
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

}

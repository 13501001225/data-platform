package com.stey.index.pipeline.jobs

import com.google.gson.{JsonObject, JsonParser}
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author gaowei
  *         2019-12-20 15:20
  */
object DealAirCO2EveryMinuteJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("DealAirCO2EveryMinuteJob")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("hdfs://ns/deal_air_co2_checkpoint")
    val kafkaParams = consumerMaps("co2")

    val TOPIC_AIR_CO2 = "stey-iot-device-air-co2-data"
    val topics = Array(TOPIC_AIR_CO2)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val etlAirCO2DFSchema = StructType(
      Seq(
        StructField("space_id", LongType, false),
        StructField("zone_device_id", LongType, false),
        StructField("zone_device_control_id", LongType, false),
        StructField("value", DoubleType, false),
        StructField("is_online", BooleanType, false),
        StructField("is_scenario", BooleanType, false),
        StructField("ts", StringType, false)
      )
    )

    def updateFunc(currValues: Seq[(Double, Long, Long, Long, Boolean, Boolean, String)], prevValueState: Option[(Double, Long, Long, Long, Boolean, Boolean, String)]): Option[(Double, Long, Long, Long, Boolean, Boolean, String)] = {
      var value = 0.0
      var space_id = 0L
      var zone_device_id = 0L
      var zone_device_control_id = 0L
      var is_online = true
      var is_scenario = false
      var ts = getrealTime("yyyy-MM-dd HH:mm:ss.SSS")
      if (!prevValueState.isEmpty) {
        value = prevValueState.get._1
        space_id = prevValueState.get._2
        zone_device_id = prevValueState.get._3
        zone_device_control_id = prevValueState.get._4
        is_online = prevValueState.get._5
        is_scenario = prevValueState.get._6
        val old_ts = prevValueState.get._7
        ts = funAddMinute(old_ts, 1,"yyyy-MM-dd HH:mm:ss.SSS")
      }
      if (currValues != null && !currValues.isEmpty) {
        value = currValues(0)._1
        space_id = currValues(0)._2
        zone_device_id = currValues(0)._3
        zone_device_control_id = currValues(0)._4
        is_online = currValues(0)._5
        is_scenario = currValues(0)._6
        ts = currValues(0)._7
      }
      //val event_datetime_key = funMinute4Second0(ts)
      Some((value, space_id, zone_device_id, zone_device_control_id, is_online, is_scenario, ts))
    }
    val etlAirCO2DS = stream.map(stream => {
      val message = stream.value()
      val jsonS = new JsonParser().parse(message).asInstanceOf[JsonObject]
      val space_id = jsonS.get("space_id").getAsLong
      val zone_device_id = jsonS.get("zone_device_id").getAsLong
      val zone_device_control_id = jsonS.get("zone_device_control_id").getAsLong
      val ts = jsonS.get("ts").getAsString
      val is_online = jsonS.get("is_online").getAsBoolean
      val value = jsonS.get("value").getAsDouble
      val is_scenario = jsonS.get("is_scenario").getAsBoolean
      val key = space_id + "-" + zone_device_id + "-" + zone_device_control_id
      (key, (value, space_id, zone_device_id, zone_device_control_id, is_online, is_scenario, ts))
    }).updateStateByKey(updateFunc)
    val etlAirCO2Calculate2RowDS = etlAirCO2DS.foreachRDD(rdd => {
      val airco2RDD = rdd.map(row => {
        val value = row._2._1
        val space_id = row._2._2
        val zone_device_id = row._2._3
        val zone_device_control_id = row._2._4
        val is_online = row._2._5
        val is_scenario = row._2._6
        val ts = funCurrentMin(row._2._7)
        Row(space_id,zone_device_id,zone_device_control_id,value,is_online,is_scenario,ts)
      })
      val etlAirCo2CalculationDF = spark.createDataFrame(airco2RDD, etlAirCO2DFSchema)

      if (!etlAirCo2CalculationDF.isEmpty) {
        val keyspace = "iot_data_warehouse"
        val table_name = "air_co2_one_minute"
        etlAirCo2CalculationDF.write
          .mode("append")
          .format("org.apache.spark.sql.cassandra")
          .options(cassandraMaps(table_name,keyspace))
          .save()
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

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
  *         2020-01-08 14:46
  */
object CalculationEleWaterEveryHourJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("CalculationEleWaterEveryHourJob")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("hdfs://mycluster/deal_ele_water_reduce_checkpoint")
    val kafkaParams = consumerMaps("ele_water_hour")

    val TOPIC_ELE_WATER = "stey-iot-device-ele-water-data"
    val topics = Array(TOPIC_ELE_WATER)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val etlEleWaterDFSchema = StructType(
      Seq(
        StructField("space_id", LongType, false),
        StructField("zone_device_id", LongType, false),
        StructField("zone_device_control_id", LongType, false),
        StructField("is_online", BooleanType, false),
        StructField("is_scenario", BooleanType, false),
        StructField("value", DoubleType, false),
        StructField("date_time_key", StringType, false)
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
        ts = funAddHour(old_ts, 1,"yyyy-MM-dd HH:mm:ss.SSS")
//        ts = funAddMinute(old_ts, 1,"yyyy-MM-dd HH:mm:ss.SSS")
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
      Some((value, space_id, zone_device_id, zone_device_control_id, is_online, is_scenario, ts))
    }
    val etlEleWaterDS = stream.map(stream => {
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
    }).reduceByKeyAndWindow((x1: (Double, Long, Long, Long, Boolean, Boolean, String), x2: (Double, Long, Long, Long, Boolean, Boolean, String)) => {
      val old_value = x1._1
      val current_value = x2._1
      if(old_value > current_value){
        (x1._1, x1._2, x1._3, x1._4, x1._5, x1._6, x1._7)
      }else{
        (x2._1, x2._2, x2._3, x2._4, x2._5, x2._6, x2._7)
      }

    }, Seconds(3600), Seconds(3600)).updateStateByKey(updateFunc)

    etlEleWaterDS.foreachRDD(rdd => {
      val eleWaterRDD = rdd.map(row => {
        val value = row._2._1
        val space_id = row._2._2
        val zone_device_id = row._2._3
        val zone_device_control_id = row._2._4
        val is_online = row._2._5
        val is_scenario = row._2._6
        val ts = row._2._7
        val date_time_key = funCurrentHour(ts)
        Row(space_id,zone_device_id,zone_device_control_id,is_online,is_scenario,value,date_time_key)
      })
      val etlEleWaterCalculationDF = spark.createDataFrame(eleWaterRDD, etlEleWaterDFSchema)

      if (!etlEleWaterCalculationDF.isEmpty) {
        val keyspace = "iot_data_warehouse"
        val table_name = "ele_water_one_hour"
        etlEleWaterCalculationDF.write
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

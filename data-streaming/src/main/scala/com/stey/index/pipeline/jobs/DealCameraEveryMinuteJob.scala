package com.stey.index.pipeline.jobs

import com.google.gson.{JsonObject, JsonParser}
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author gaowei
  *         2020-01-08 18:14
  */
object DealCameraEveryMinuteJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("DealCameraEveryMinuteJob")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("hdfs://ns/deal_camera_one_min_checkpoint")
    val kafkaParams = consumerMaps("camera")

    val TOPIC_CAMERA = "stey-iot-device-camera-data"
    val topics = Array(TOPIC_CAMERA)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val etlCameraDFSchema = StructType(
      Seq(
        StructField("space_id", LongType, false),
        StructField("value", IntegerType, false),
        StructField("is_online", BooleanType, false),
        StructField("is_scenario", BooleanType, false),
        StructField("ts", StringType, false)
      )
    )
    val etlCameraDS = stream.map(stream => {
      val message = stream.value()
      val jsonS = new JsonParser().parse(message).asInstanceOf[JsonObject]
      val space_id = jsonS.get("space_id").getAsLong
      val ts = jsonS.get("ts").getAsString
      val is_online = jsonS.get("is_online").getAsBoolean
      val value = jsonS.get("value").getAsInt
      val is_scenario = jsonS.get("is_scenario").getAsBoolean
      (space_id, (value, space_id, is_online, is_scenario, ts))
    }).reduceByKeyAndWindow((x1: (Int, Long, Boolean, Boolean, String), x2: (Int, Long, Boolean, Boolean, String)) => {
      ((Decimal(x1._1) + Decimal(x2._1)).toInt, x2._2, x2._3, x2._4, x2._5)
    }, Seconds(60), Seconds(60))

    etlCameraDS.foreachRDD(rdd => {
      val cameraRDD = rdd.map(row => {
        val value = row._2._1
        val space_id = row._2._2
        val is_online = row._2._3
        val is_scenario = row._2._4
        val ts = funCurrentMin(row._2._5)
        Row(space_id,value,is_online,is_scenario,ts)
      })
      val etlCameraCalculationDF = spark.createDataFrame(cameraRDD, etlCameraDFSchema)

      if (!etlCameraCalculationDF.isEmpty) {
        val keyspace = "iot_data_warehouse"
        val table_name = "camera_one_minute"
        etlCameraCalculationDF.write
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

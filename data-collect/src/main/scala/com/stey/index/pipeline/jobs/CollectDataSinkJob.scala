package com.stey.index.pipeline.jobs

import java.sql.Timestamp

import com.google.gson.Gson
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.sinks.KafkaSink
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
  * gaowei 
  * 2020-04-09 2:01 
  */
object CollectDataSinkJob extends SparkModule {
  case class DeviceEleWaterData(space_id: Long, zone_device_id: Long, zone_device_control_id: Long, ts: String, is_online: Boolean, metadata: Double, is_scenario: Boolean)
  case class DeviceCameraData(space_id: Long, zone_device_id: Long, zone_device_control_id: Long, ts: String, is_online: Boolean, metadata: Int, is_scenario: Boolean)
  case class DeviceAirCO2Data(space_id: Long, zone_device_id: Long, zone_device_control_id: Long, ts: String, is_online: Boolean, metadata: Double, is_scenario: Boolean)

  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("CollectDataSinkJob")
    val producer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = producerConfig
      spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    val getRedisKey = udf((space_id: Long, zone_device_id: Long, zone_device_control_id: Long) => {
      val key = space_id.toString + "-" + zone_device_id.toString + "-" + zone_device_control_id.toString
      key
    })
    val kafkaStream = spark.readStream
      .format("kafka")
      .options(optionKafkaMaps)
    val streamDF = kafkaStream.load()
    val kafkaSchema = StructType(
      Seq(
        StructField("spaceId", LongType, false),
        StructField("zoneDeviceId", LongType, false),
        StructField("zoneDeviceControlId", LongType, false),
        StructField("ts", TimestampType, false),
        StructField("isOnline", BooleanType, false),
        StructField("value", StringType, true),
        StructField("isScenario", BooleanType, false)
      )
    )
    import spark.implicits._

    val usingColumns_device = Seq("space_id", "zone_device_id", "zone_device_control_id")
    //    val connector = CassandraConnector(spark.sparkContext.getConf)
    val data_warehouse_keyspace = "iot_data_warehouse"

    val cassandra_table_ele_water_all_id = "ele_water_data"
    val cassandra_table_air_co2_all_id = "air_co2_data"
    val cassandra_table_camera_all_id = "camera_data"

    val redisAndKafkaSinkDS = streamDF
      .selectExpr("CAST(value AS STRING)").as[String]
      .select(from_json($"value", kafkaSchema) as "data").select("data.*")
      .filter("isScenario == false")
      .filter("isOnline == true")
      .withColumnRenamed("spaceId", "space_id")
      .withColumnRenamed("zoneDeviceId", "zone_device_id")
      .withColumnRenamed("zoneDeviceControlId", "zone_device_control_id")
      .withColumnRenamed("isOnline", "is_online")
      .withColumnRenamed("isScenario", "is_scenario")
      .writeStream
      .queryName("RedisAndKafkaSink")
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) => {
        if (!batchDF.isEmpty) {
          val readDeviceRedisDf = spark.read
            .format("org.apache.spark.sql.redis")
            .option("infer.schema", true)
            .option("table", "DEVICE_*")
            .load()
            .filter($"_id".contains("-"))

          val TOPIC_ELE_WATER = "stey-iot-device-ele-water-data"
          val TOPIC_CAMERA = "stey-iot-device-camera-data"
          val TOPIC_AIR_CO2 = "stey-iot-device-air-co2-data"

          val device_DF = batchDF.join(readDeviceRedisDf, usingColumns_device, "left")
          val device_effective_DF = device_DF.where("device_type is not null")
          if (!device_effective_DF.isEmpty) {
            device_effective_DF.where("device_type = 'elemeter' or device_type = 'watermeter'")
              .select("space_id","ts","is_online","is_scenario","value","zone_device_id", "zone_device_control_id")
              .as[(Long,String,Boolean,Boolean,Double,Long,Long)]
              .write
              .mode("append")
              .format("org.apache.spark.sql.cassandra")
              .options(cassandraMaps(cassandra_table_ele_water_all_id,data_warehouse_keyspace))
              .save()

            device_effective_DF.where("device_type = 'airpurifiercontrol'")
              .select("space_id","ts","is_online","is_scenario","value","zone_device_id", "zone_device_control_id")
              .as[(Long,String,Boolean,Boolean,Double,Long,Long)]
              .write
              .mode("append")
              .format("org.apache.spark.sql.cassandra")
              .options(cassandraMaps(cassandra_table_air_co2_all_id,data_warehouse_keyspace))
              .save()

            device_effective_DF.where("device_type = 'camera'")
              .select("space_id","ts","is_online","is_scenario","value","zone_device_id", "zone_device_control_id")
              .as[(Long,String,Boolean,Boolean,Int,Long,Long)]
              .write
              .mode("append")
              .format("org.apache.spark.sql.cassandra")
              .options(cassandraMaps(cassandra_table_camera_all_id,data_warehouse_keyspace))
              .save()

            device_effective_DF.foreachPartition(it => {
              while (it.hasNext) {
                val row = it.next()
                val space_id = row.getAs[Long]("space_id")
                val zone_device_id = row.getAs[Long]("zone_device_id")
                val zone_device_control_id = row.getAs[Long]("zone_device_control_id")
                val is_online = row.getAs[Boolean]("is_online")
                val is_scenario = row.getAs[Boolean]("is_scenario")
                val value = row.getAs[String]("value")
                val ts = row.getAs[Timestamp]("ts")
                val device_type = row.getAs[String]("device_type")
                val str_ts = timeStampToString(ts)
                if ((device_type.equals("watermeter") || device_type.equals("elemeter"))) {
                  val deviceEleWaterStatus = DeviceEleWaterData(space_id, zone_device_id, zone_device_control_id, str_ts, is_online, value.toDouble, is_scenario)
                  val strStatusJson = new Gson().toJson(deviceEleWaterStatus)
                  producer.value.send(TOPIC_ELE_WATER, str_ts, strStatusJson)
                } else if (device_type.equals("airpurifiercontrol")) {
                  val deviceAirCO2Data = DeviceAirCO2Data(space_id, zone_device_id, zone_device_control_id, str_ts, is_online, value.toDouble, is_scenario)
                  val strStatusJson = new Gson().toJson(deviceAirCO2Data)
                  producer.value.send(TOPIC_AIR_CO2, str_ts, strStatusJson)
                } else if (device_type.equals("camera")) {
                  val deviceCameraData = DeviceCameraData(space_id, zone_device_id, zone_device_control_id, str_ts, is_online, value.toInt, is_scenario)
                  val strStatusJson = new Gson().toJson(deviceCameraData)
                  producer.value.send(TOPIC_CAMERA, str_ts, strStatusJson)
                }
              }
            })
          }
          val device_unknown_DF = device_DF
            .where("device_type is null")
            .select("space_id", "zone_device_id", "zone_device_control_id", "ts", "is_online", "value", "is_scenario")
          if (!device_unknown_DF.isEmpty) {
            val readInvalidDeviceRedisDf = spark.read
              .format("org.apache.spark.sql.redis")
              .option("infer.schema", true)
              .option("table", "INVALID_DEVICE")
              .load()
              .filter($"_id".contains("-"))
            val device_not_in_redis_unknown_DF = device_unknown_DF
              .join(readInvalidDeviceRedisDf, usingColumns_device, "left")
              .where("zone_device_control_tag is null and device_type is null")
            if (!device_not_in_redis_unknown_DF.isEmpty) {
              val sql_device_table = "iot.binding_master"
              val device_meta_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_device_table, sqlProdProperties)
                .selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control_tag as zone_device_control", "device_type_c")
                .where("((device_type_c = 'elemeter' or device_type_c = 'watermeter') and zone_device_control = 1020) or (device_type_c = 'camera' and zone_device_control = 1010) or (device_type_c = 'airpurifiercontrol' and zone_device_control = 1050)")
              val unknown_device_DF = device_not_in_redis_unknown_DF.join(device_meta_DF, usingColumns_device, "left")
              val is_device_DF = unknown_device_DF.where("zone_device_control is not null and device_type_c is not null")
              if (!is_device_DF.isEmpty) {
                device_effective_DF.where("device_type_c = 'elemeter' or device_type_c = 'watermeter'")
                  .select("space_id","ts","is_online","is_scenario","value","zone_device_id", "zone_device_control_id")
                  .as[(Long,String,Boolean,Boolean,Double,Long,Long)]
                  .write
                  .mode("append")
                  .format("org.apache.spark.sql.cassandra")
                  .options(cassandraMaps(cassandra_table_ele_water_all_id,data_warehouse_keyspace))
                  .save()
                val air_co2_DS = device_effective_DF.where("device_type_c = 'airpurifiercontrol'")
                  .select("space_id","ts","is_online","is_scenario","value","zone_device_id", "zone_device_control_id")
                  .as[(Long,String,Boolean,Boolean,Double,Long,Long)]
                  .write
                  .mode("append")
                  .format("org.apache.spark.sql.cassandra")
                  .options(cassandraMaps(cassandra_table_air_co2_all_id,data_warehouse_keyspace))
                  .save()
                device_effective_DF.where("device_type_c = 'camera'")
                  .select("space_id","ts","is_online","is_scenario","value","zone_device_id", "zone_device_control_id")
                  .as[(Long,String,Boolean,Boolean,Int,Long,Long)]
                  .write
                  .mode("append")
                  .format("org.apache.spark.sql.cassandra")
                  .options(cassandraMaps(cassandra_table_camera_all_id,data_warehouse_keyspace))
                  .save()
                is_device_DF.foreachPartition(it => {
                  while (it.hasNext) {
                    val row = it.next()
                    val space_id = row.getAs[Long]("space_id")
                    val zone_device_id = row.getAs[Long]("zone_device_id")
                    val zone_device_control_id = row.getAs[Long]("zone_device_control_id")
                    val is_online = row.getAs[Boolean]("is_online")
                    val is_scenario = row.getAs[Boolean]("is_scenario")
                    val value = row.getAs[String]("value")
                    val ts = row.getAs[Timestamp]("ts")
                    val device_type = row.getAs[String]("device_type_c")
                    val str_ts = timeStampToString(ts)
                    if (device_type.equals("watermeter") || device_type.equals("elemeter")) {
                      val deviceEleWaterStatus = DeviceEleWaterData(space_id, zone_device_id, zone_device_control_id, str_ts, is_online, value.toDouble, is_scenario)
                      val strStatusJson = new Gson().toJson(deviceEleWaterStatus)
                      producer.value.send(TOPIC_ELE_WATER, str_ts, strStatusJson)
                    } else if (device_type.equals("airpurifiercontrol")) {
                      val deviceAirCO2Data = DeviceAirCO2Data(space_id, zone_device_id, zone_device_control_id, str_ts, is_online, value.toDouble, is_scenario)
                      val strStatusJson = new Gson().toJson(deviceAirCO2Data)
                      producer.value.send(TOPIC_AIR_CO2, str_ts, strStatusJson)
                    } else if (device_type.equals("camera")) {
                      val deviceCameraData = DeviceCameraData(space_id, zone_device_id, zone_device_control_id, str_ts, is_online, value.toInt, is_scenario)
                      val strStatusJson = new Gson().toJson(deviceCameraData)
                      producer.value.send(TOPIC_CAMERA, str_ts, strStatusJson)
                    }
                  }
                })
                val is_device_ele_water_DF = is_device_DF.filter($"device_type_c".equalTo("elemeter") || $"device_type_c".equalTo("watermeter"))
                if (!is_device_ele_water_DF.isEmpty) {
                  is_device_ele_water_DF.selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control as zone_device_control_tag", "device_type_c as device_type")
                    .withColumn("key", getRedisKey(col("space_id"), col("zone_device_id"), col("zone_device_control_id")))
                    .write
                    .format("org.apache.spark.sql.redis")
                    .option("table", "DEVICE_ELE_WATER")
                    .option("key.column", "key")
                    .mode(SaveMode.Append)
                    .save()
                }
                val is_device_air_co2_DF = is_device_DF.filter($"device_type".equalTo("airpurifiercontrol"))
                if (!is_device_air_co2_DF.isEmpty) {
                  is_device_air_co2_DF.selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control as zone_device_control_tag", "device_type_c as device_type")
                    .withColumn("key", getRedisKey(col("space_id"), col("zone_device_id"), col("zone_device_control_id")))
                    .write
                    .format("org.apache.spark.sql.redis")
                    .option("table", "DEVICE_AIR_CO2")
                    .option("key.column", "key")
                    .mode(SaveMode.Append)
                    .save()
                }
                val is_device_camera_DF = is_device_DF.filter($"device_type".equalTo("camera"))
                if (!is_device_camera_DF.isEmpty) {
                  is_device_camera_DF.selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control as zone_device_control_tag", "device_type_c as device_type")
                    .withColumn("key", getRedisKey(col("space_id"), col("zone_device_id"), col("zone_device_control_id")))
                    .write
                    .format("org.apache.spark.sql.redis")
                    .option("table", "DEVICE_CAMERA")
                    .option("key.column", "key")
                    .mode(SaveMode.Append)
                    .save()
                }
              }
              val is_invalid_DF = unknown_device_DF.where("zone_device_control_tag is null and device_type_c is null")
              if (!is_invalid_DF.isEmpty) {
                is_invalid_DF
                  .selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control as zone_device_control_tag", "device_type_c as device_type")
                  .withColumn("key", getRedisKey(col("space_id"), col("zone_device_id"), col("zone_device_control_id")))
                  .write
                  .format("org.apache.spark.sql.redis")
                  .option("table", "INVALID_DEVICE")
                  .option("key.column", "key")
                  .mode(SaveMode.Append)
                  .save()
              }
            }
          }
        }
      }
      }.start()
    redisAndKafkaSinkDS.awaitTermination()
  }
}

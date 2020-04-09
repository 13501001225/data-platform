package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.utils.DateTimeUtil._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author gaowei
  *         2020-02-27 15:20
  */
object TypeSafeDemoJob {
  def main(args: Array[String]): Unit = {

    val configuration: Config = ConfigFactory.load

    val conf: SparkConf = new SparkConf()
      .setAppName("CollectDataSinkJob")
      .set("spark.cassandra.connection.host", configuration.getString("spark.cassandra.connection.host"))
      .set("spark.cassandra.connection.port", configuration.getString("spark.cassandra.connection.port"))
      .set("spark.cassandra.auth.username", configuration.getString("spark.cassandra.auth.username"))
      .set("spark.cassandra.auth.password", configuration.getString("spark.cassandra.auth.password"))
      .set("spark.redis.host", configuration.getString("spark.redis.host"))
      .set("spark.redis.port", configuration.getString("spark.redis.port"))

    val spark = SparkSession.builder()
      .master("local[2]")
      .config(conf)
      .getOrCreate()

    val readInvalidDeviceRedisDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "INVALID_DEVICE")
      .load()
    readInvalidDeviceRedisDf.show(1000)

    val keyspace = "iot_data_warehouse"

    val table_name_yesterday = s"ele_water_${funAddDay(getrealTime("yyyyMMdd"), -1, "yyyyMMdd")}"
    val readTodayEleWaterDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table_name_yesterday, "keyspace" -> keyspace))
      .load
      .select("date_key", "date_time_key", "create_at", "current_value", "difference", "space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "time_key")

    readTodayEleWaterDF.show(1000)
  }
}

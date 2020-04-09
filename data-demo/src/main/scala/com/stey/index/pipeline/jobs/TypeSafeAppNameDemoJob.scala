package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.modules.SparkModule

//import com.stey.index.pipeline.utils.DateTimeUtil._

/**
  * @author gaowei
  *         2020-02-27 15:20
  */
object TypeSafeAppNameDemoJob extends SparkModule {
  def main(args: Array[String]): Unit = {

    val spark = buildSparkSession("TypeSafeAppNameDemoJob")

//    val readInvalidDeviceRedisDf = spark.read
//      .format("org.apache.spark.sql.redis")
//      .option("infer.schema", true)
//      .option("table", "INVALID_DEVICE")
//      .load()
//    readInvalidDeviceRedisDf.show(1000)
//    readInvalidDeviceRedisDf.select("space_id").distinct().show(1000)
//
//    val keyspace = "iot_data_warehouse"
//
//    val table_name = s"ele_water_data_space_id"
//    val readTodayEleWaterDF = spark
//      .read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> table_name, "keyspace" -> keyspace))
//      .load
//
//    readTodayEleWaterDF.show(1000)

    val sql_device_table = "iot.binding_master"
    val device_meta_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_device_table, sqlProdProperties)
      .selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control_tag as zone_device_control", "device_type_c")
      .where("((device_type_c = 'elemeter' or device_type_c = 'watermeter') and zone_device_control = 1020) or (device_type_c = 'camera' and zone_device_control = 1010) or (device_type_c = 'airpurifiercontrol' and zone_device_control = 1050)")

    device_meta_DF.show(1000)
  }
}

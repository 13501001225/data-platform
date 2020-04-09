package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._

/**
  * @author gaowei
  *         2020-03-12 23:10
  */
object GetHistoryDataFilterJob extends SparkModule {
  def main(args: Array[String]): Unit = {

    val spark = buildSparkSession("GetHistoryDataFilterJob")

    import spark.implicits._
    val readEleWaterDeviceRedisDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "DEVICE_ELE_WATER")
      .load()
      .filter($"_id".contains("-"))
    val spaceDF = readEleWaterDeviceRedisDf.select("space_id", "zone_device_id", "zone_device_control_id").distinct()

    val keyspace = "iot_data_warehouse"
    val table_name = "ele_water_history_data_tmp"
    val columns_space_id = Seq("space_id", "zone_device_id", "zone_device_control_id")
    val appointed_date = "2020-03-01"
    val yesterday12 = getYesterday12(appointed_date)
    val today12 = getToday12(appointed_date)

    val readEleWaterDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table_name, "keyspace" -> keyspace))
      .load
      .select("space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "value", "ts")
      .where(s"ts>='${yesterday12}' and ts<'${today12}'")
    println("count:" + readEleWaterDF.count())

    //    val readEleWaterAllDF = spark
    //      .read
    //      .format("org.apache.spark.sql.cassandra")
    //      .options(Map("table" -> table_name, "keyspace" -> keyspace))
    //      .load
    //      .select("space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "value", "ts")
    //    println("count:" + readEleWaterAllDF.count())

    //    val df = readEleWaterAllDF.filter(s"ts>='${yesterday12}' and ts<'${today12}'")
    //    df.show()
    //    val readEleWaterAllDF = spark
    //      .read
    //      .format("org.apache.spark.sql.cassandra")
    //      .options(Map("table" -> table_name, "keyspace" -> keyspace))
    //      .load
    //      .join(spaceDF, columns_space_id, "left")
    //      .select("space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "value", "ts")
    //    println("count:" + readEleWaterAllDF.count())

  }
}

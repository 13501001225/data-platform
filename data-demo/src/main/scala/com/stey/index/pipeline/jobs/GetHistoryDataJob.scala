package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.modules.SparkModule
import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.types._

/**
  * @author gaowei
  *         2020-03-05 13:51
  */
object GetHistoryDataJob extends SparkModule{
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("GetHistoryDataJob")
    val etlEleWaterDataSchema = StructType(
      Seq(
        StructField("space_id", LongType, false),
        StructField("zone_device_id", LongType, false),
        StructField("zone_device_control_id", LongType, false),
        StructField("date_time_key", StringType, false),
        StructField("is_online", BooleanType, false),
        StructField("is_scenario", BooleanType, false),
        StructField("value", DoubleType, false)
      )
    )

    val keyspace = "iot_data_warehouse"
    val table_name = "ele_water_data_one_hour"

    val etlEleWaterDF = spark.read.schema(etlEleWaterDataSchema)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table_name, "keyspace" -> keyspace))
      .load
      .select("space_id", "zone_device_id", "zone_device_control_id", "date_time_key", "is_online", "is_scenario", "value")
      .sort(desc("date_time_key"),asc("space_id"),asc("zone_device_id"),asc("zone_device_control_id"))
    println(etlEleWaterDF.count())
    etlEleWaterDF.show(10000)
  }
}

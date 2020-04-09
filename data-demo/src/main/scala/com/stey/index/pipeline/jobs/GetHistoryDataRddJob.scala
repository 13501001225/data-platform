package com.stey.index.pipeline.jobs


import com.datastax.spark.connector.{CassandraRow, rdd, _}
import com.datastax.spark.connector.rdd.CassandraRDD
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil.{getToday12, getYesterday12}

/**
  * @author gaowei
  *         2020-03-13 1:35
  *
  */
object GetHistoryDataRddJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("GetHistoryDataRddJob")
    val keyspace = "iot_data_warehouse"
    val table_name = "ele_water_history_data_tmp"

    val sc = spark.sparkContext
    val appointed_date = "2020-03-01"
    val yesterday12 = getYesterday12(appointed_date)
    val today12 = getToday12(appointed_date)
    import spark.implicits._
    val readEleWaterDeviceRedisDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "DEVICE_ELE_WATER")
      .load()
      .filter($"_id".contains("-"))
    val spaceDF = readEleWaterDeviceRedisDf.select("space_id", "zone_device_id", "zone_device_control_id").distinct()

//      val space_id = row.getAs[String]("space_id").toLong
//      val zone_device_id = row.getAs[String]("zone_device_id").toLong
//      val zone_device_control_id = row.getAs[String]("zone_device_control_id").toLong
      val rdd : CassandraRDD[CassandraRow] = sc.cassandraTable(keyspace, table_name)
        .where(s"ts>='${yesterday12}' and ts<'${today12}'")
      println(rdd.count())




//    rdd.foreach(row => {println(row.toString())})

  }
}

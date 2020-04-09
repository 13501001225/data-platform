package com.stey.index.pipeline.jobs

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * @author gaowei
  *         2020-03-12 13:54
  */
object HistoryEleWaterFillMissingDatesJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("HistoryEleWaterFillMissingDatesJob")

    val date_transform = udf((date: String) => {
      val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val dt = LocalDateTime.parse(date, dtFormatter)
      "%4d-%2d-%2d_%2d:%2d:%2d".format(dt.getYear, dt.getMonthValue, dt.getDayOfMonth, dt.getHour, dt.getMinute, dt.getSecond)
        .replaceAll(" ", "0").replace("_"," ")
    })

    def fill_dates = udf((start: String, excludedDiff: Int) => {
      val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val fromDt = LocalDateTime.parse(start, dtFormatter)
      (1 to (excludedDiff - 1)).map(hour => {
        val dt = fromDt.plusHours(hour)
        "%4d-%2d-%2d_%2d:%2d:%2d".format(dt.getYear, dt.getMonthValue, dt.getDayOfMonth, dt.getHour, dt.getMinute, dt.getSecond)
          .replaceAll(" ", "0").replace("_"," ")
      })
    })
    val datetime_diff = udf((end_datetime: String, start_datetime: String) => {
      if(end_datetime != null && start_datetime != null){
        datetime_between_hour(start_datetime,end_datetime)
      }else{
        0
      }
    })

    import spark.implicits._
    val readEleWaterDeviceRedisDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "DEVICE_ELE_WATER")
      .load()
      .filter($"_id".contains("-"))
    val spaceDF = readEleWaterDeviceRedisDf.select("space_id","zone_device_id","zone_device_control_id")
    val using_device_date = Seq("space_id", "zone_device_id", "zone_device_control_id")
    val etlEleWaterDataSchema = StructType(
      Seq(
        StructField("space_id", LongType, false),
        StructField("zone_device_id", LongType, false),
        StructField("zone_device_control_id", LongType, false),
        StructField("ts", TimestampType, false),
        StructField("is_online", BooleanType, false),
        StructField("is_scenario", BooleanType, false),
        StructField("value", DoubleType, false)
      )
    )
    val keyspace = "iot_data_warehouse"
    val table_name = "ele_water_history_data_tmp"
    //    val table_name = "ele_water_history_data_collected"
    val etlEleWaterDF = spark.read.schema(etlEleWaterDataSchema)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table_name, "keyspace" -> keyspace))
      .load
      .join(spaceDF, using_device_date, "left")
      .filter("ts>='2020-02-15'")
      .select("space_id", "zone_device_id", "zone_device_control_id", "ts", "is_online", "is_scenario", "value")
      .withColumn("datetime", date_transform($"ts").cast(StringType))
    //    etlEleWaterDF.show(1000)
    val w = Window.orderBy($"datetime")
    val etlEleWaterFillDf = etlEleWaterDF
      .withColumn("next_datetime", lead($"datetime", 1).over(w))
      .withColumn("diff", datetime_diff(col("next_datetime"),col("datetime")))
      .filter($"diff" > 1)
      .withColumn("next_dates", fill_dates($"datetime", $"diff"))
      .withColumn("date_time_key", explode($"next_dates"))
    val etlEleWaterAllDF = etlEleWaterDF.select("date_time_key","space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "value").union(etlEleWaterFillDf.select("date_time_key", "space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "value"))
      .orderBy("date_time_key")
    etlEleWaterAllDF.show(1000)
    if (!etlEleWaterAllDF.isEmpty) {
      val keyspace = "iot_data_warehouse"
      val table_name = "ele_water_one_hour_test"
      etlEleWaterAllDF.write
        .mode("append")
        .format("org.apache.spark.sql.cassandra")
        .options(cassandraMaps(table_name,keyspace))
        .save()
    }
  }
}

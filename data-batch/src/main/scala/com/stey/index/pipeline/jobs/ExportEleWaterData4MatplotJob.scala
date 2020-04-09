package com.stey.index.pipeline.jobs

import java.sql.{Date, Timestamp}
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
  * @author gaowei
  *         2020-01-03 14:34
  */
object ExportEleWaterData4MatplotJob extends SparkModule{
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("ExportEleWaterData4MatplotJob")

    val etlEleWaterDFSchema = StructType(
      Seq(
        StructField("space_id", LongType, false),
        StructField("zone_device_id", LongType, false),
        StructField("zone_device_control_id", LongType, false),
        StructField("event_datetime", StringType, false),
        StructField("is_online", BooleanType, false),
        StructField("current_value", DoubleType, false),
        StructField("average_value", DoubleType, false),
        StructField("year", LongType, false),
        StructField("month", LongType, false),
        StructField("day", LongType, false),
        StructField("hour", LongType, false),
        StructField("minute", LongType, false),
        StructField("second", LongType, false),
        StructField("zone_id", LongType, false),
        StructField("control_id", LongType, false),
        StructField("zone_device_control_tag", IntegerType, false),
        StructField("zone_device_control_code", StringType, false),
        StructField("zone_device_code", StringType, false),
        StructField("control_value_type_c", StringType, false),
        StructField("zone_code", StringType, false),
        StructField("project_id", LongType, false),
        StructField("space_code", StringType, false),
        StructField("space_type_c", StringType, false),
        StructField("project_code", StringType, false),
        StructField("is_community", BooleanType, false),
        StructField("is_scenario", BooleanType, false),
        StructField("accumulate_value", DoubleType, false),
        StructField("last_value", DoubleType, false),
        StructField("frequency", IntegerType, false),
        StructField("date_key", StringType, false),
        StructField("date_time_key", StringType, false),
        StructField("created_at", StringType, false),
        StructField("device_id", LongType, false),
        StructField("device_type", StringType, false)
      )
    )

    val sql_ele_water = "iot.ele_water_data"
    //    val sqlServerTable_ele_water_dw = "iot.camera_data"
    import spark.implicits._
    //    val sqlServerTable_ele_water_dw = "iot.air_co2_data"
    val project_DS = spark.read.jdbc(sqlDwProperties.getProperty("url"), sql_ele_water, sqlDwProperties)
      //.filter($"device_type".equalTo("elemeter"))
      .filter($"device_type".equalTo("watermeter"))
      .filter("year = 2020 and month = 1 and day = 5")
      .orderBy("space_id","zone_device_id","zone_device_control_id")
    val project_RDD = project_DS.rdd.map(row=>{
      val space_id = row.getAs[Long]("space_id")
      val zone_device_id = row.getAs[Long]("zone_device_id")
      val zone_device_control_id = row.getAs[Long]("zone_device_control_id")
      val event_dt = row.getAs[Timestamp]("event_datetime")
      val event_datetime = timeStampToFormatString(event_dt,"yyyy-MM-dd HH:mm:ss.SSS")
      val is_online = row.getAs[Boolean]("is_online")
      val current_value = row.getAs[Double]("current_value")
      val average_value = row.getAs[Double]("average_value")
      val year = row.getAs[Long]("year")
      val month = row.getAs[Long]("month")
      val day = row.getAs[Long]("day")
      val hour = row.getAs[Long]("hour")
      val minute = row.getAs[Long]("minute")
      val second = row.getAs[Long]("second")
      val zone_id = row.getAs[Long]("zone_id")
      val control_id = row.getAs[Long]("control_id")
      val zone_device_control_tag = row.getAs[Int]("zone_device_control_tag")
      val zone_device_control_code = row.getAs[String]("zone_device_control_code")
      val zone_device_code = row.getAs[String]("zone_device_code")
      val control_value_type_c = row.getAs[String]("control_value_type_c")
      val zone_code = row.getAs[String]("zone_code")
      val project_id = row.getAs[Long]("project_id")
      val space_code = row.getAs[String]("space_code")
      val space_type_c = row.getAs[String]("space_type_c")
      val project_code = row.getAs[String]("project_code")
      val is_community = row.getAs[Boolean]("is_community")
      val is_scenario = row.getAs[Boolean]("is_scenario")
      val accumulate_value = row.getAs[Double]("accumulate_value")
      val last_value = row.getAs[Double]("last_value")
      val frequency = row.getAs[Int]("frequency")
      val d_key = row.getAs[Date]("date_key")
      val date_key = dateToFormatString(d_key,"yyyy-MM-dd")
      val date_tk = row.getAs[Timestamp]("date_time_key")
      val date_time_key = timeStampToFormatString(date_tk,"yyyy-MM-dd HH:mm:ss.SSS")
      val created = row.getAs[Timestamp]("created_at")
      val created_at = timeStampToFormatString(created,"yyyy-MM-dd HH:mm:ss.SSS")
      val device_id = row.getAs[Long]("device_id")
      val device_type = row.getAs[String]("device_type")

      Row(space_id,zone_device_id,zone_device_control_id,event_datetime,is_online,current_value,
        average_value,year,month,day,hour,minute,second,zone_id,control_id,zone_device_control_tag,zone_device_control_code,zone_device_code,
        control_value_type_c,zone_code,project_id,space_code,space_type_c,project_code,is_community,is_scenario,accumulate_value,last_value,frequency,
        date_key,date_time_key,created_at,device_id,device_type
      )
    })
    val project_DF = spark.createDataFrame(project_RDD,etlEleWaterDFSchema)
    project_DF.printSchema()
    //    project_DF.show()
    project_DF.write.mode("Append").csv("E:\\testFile\\ele_weater_data")
    //    project_DF.write.mode("Append").csv("E:\\testFile\\camera_data")
    //    project_DF.write.mode("Append").csv("E:\\testFile\\air_co2")
  }
}

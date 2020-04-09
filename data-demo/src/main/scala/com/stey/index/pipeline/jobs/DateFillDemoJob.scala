package com.stey.index.pipeline.jobs

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

import com.stey.index.pipeline.utils.DateTimeUtil._
/**
  * @author gaowei
  *         2020-03-11 16:03
  */
object DateFillDemoJob {
  def main(args: Array[String]): Unit = {

  val conf: SparkConf = new SparkConf()
        .setAppName("DateFillDemoJob")
      val spark = SparkSession
        .builder()
        .master("local[*]")
        .config(conf)
        .getOrCreate()
    val date_transform = udf((date: String) => {
      val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val dt = LocalDateTime.parse(date, dtFormatter)
//      println("%4d-%2d-%2d_%2d:%2d:%2d".format(dt.getYear, dt.getMonthValue, dt.getDayOfMonth, dt.getHour, dt.getMinute, dt.getSecond).replaceAll(" ", "0"))
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
    import spark.implicits._
    val df = Seq(
      ("2016-10-01 00:00:00", 1),
      ("2016-10-01 01:00:00", 2),
      ("2016-10-01 03:00:00", 0),
      ("2016-10-01 06:00:00", 1),
      ("2016-10-01 07:00:00", 0),
      ("2016-10-01 11:00:00", 2)).toDF("date", "quantity")
      .withColumn("datetime", date_transform($"date").cast(StringType))
      .withColumn("quantity", $"quantity".cast(LongType))
      .withColumn("date_time_key",$"datetime")


//    df.show()

    val datetime_diff = udf((end_datetime: String, start_datetime: String) => {
      if(end_datetime != null && start_datetime != null){
        datetime_between_hour(start_datetime,end_datetime)
      }else{
        0
      }
    })

    val w = Window.orderBy($"datetime")
    val tempDf = df
      .withColumn("next_datetime", lead($"datetime", 1).over(w))
      .withColumn("diff", datetime_diff(col("next_datetime"),col("datetime")))
      .filter($"diff" > 1) // Pick date diff more than one day to generate our date
      .withColumn("next_dates", fill_dates($"datetime", $"diff"))
      .withColumn("date_time_key", explode($"next_dates"))


//    tempDf.show(false)
//
    val result = df.select("date_time_key","quantity").union(tempDf.select("date_time_key", "quantity"))
      .orderBy("date_time_key")

    result.show()
  }

}

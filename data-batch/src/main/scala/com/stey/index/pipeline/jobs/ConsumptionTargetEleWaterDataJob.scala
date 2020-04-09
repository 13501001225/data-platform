package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.sql.functions.{current_date, _}

/**
  * @author gaowei
  *         2020-01-10 16:01
  */
object ConsumptionTargetEleWaterDataJob extends SparkModule {
  def main(args: Array[String]): Unit = {

    val spark = buildSparkSession("ConsumptionTargetEleWaterDataJob")

    val startDateToGMT = udf((ts: String) => {
      val mata_ts_format = funAddHour(ts, 8, "yyyy-MM-dd HH:mm:ss")
      mata_ts_format
    })
    val endDateToGMT = udf((ts: String) => {
      val mata_ts_format = funAddHour(ts, 0, "yyyy-MM-dd HH:mm:ss")
      mata_ts_format
    })
    val dateList = udf((startdate: String, enddate: String) => {
      getBetweenDates(startdate, enddate)
    })
    val dateTimeList = udf((initdate: String) => {
      getTimes(initdate)
    })
    val isRerented = udf((rerentid: String) => {
      if (rerentid != null) {
        true
      } else {
        false
      }
    })
    val actualDate = udf((date_time_key: String) => {
      getActualDate(date_time_key)
    })
    val actualSpaceId = udf((history_space_id: String, space_id: String) => {
      if (history_space_id != null && !"".equals(history_space_id)) {
        history_space_id.toLong
      } else {
        space_id.toLong
      }
    })

    import spark.implicits._


    val usingColumns_order_history = Seq("order_id")


    val usingColumns_order_rerent = Seq("order_id", "date")
    val usingColumns_order_rerent_user = Seq("user_id")
    val usingColumns_order_rerent_user_space = Seq("space_id")
    val usingColumns_order_ele_water = Seq("space_id", "date_time_key")

    val sqlServerTable_ele_water_dw = "iot.ele_water_every_hour"

    //    val number_arr = 1 to 3
    //    number_arr.foreach(num => {
    val num = 1
    val sql_order_table = "[order].[order]"
    val order_init_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_order_table, sqlProdProperties)
      .selectExpr("id as order_id", "order_number", "user_id", "space_id", "order_type_c", "status_c", "check_in_date", "check_out_date")
      .where("order_id = 558")
      .where($"order_type_c".equalTo("tenant"))
      .where($"status_c".equalTo("completed") || $"status_c".equalTo("active") || $"status_c".equalTo("rerenting"))
      .withColumn("order_check_in_date", startDateToGMT(col("check_in_date")))
      .withColumn("order_check_out_date", endDateToGMT(col("check_out_date")))
      .withColumn("diff_date1", datediff(col("check_out_date"), col("check_in_date")))
      .where("diff_date1 > 15")
      .withColumn("current_date", date_sub(current_date(), num))
      .withColumn("last_month_date", date_sub(col("current_date"), 30))
      .withColumn("diff_date2", datediff(col("check_out_date"), col("current_date")))
      .withColumn("diff_date3", datediff(col("last_month_date"), col("check_in_date")))
      .where("diff_date2 >= 0 and diff_date3 >= 0")

    val sql_order_history_table = "[order].order_history"
    val order_history_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_order_history_table, sqlProdProperties)
      .selectExpr("order_id", "space_id as history_space_id")
      .join(order_init_DF, usingColumns_order_history, "right")
    val order_base_DF = order_history_DF
      .selectExpr("order_id", "order_number", "user_id", "order_type_c", "status_c", "check_in_date", "check_out_date", "order_check_in_date", "order_check_out_date", "current_date", "last_month_date", "history_space_id", "space_id")
      .withColumn("actual_space_id", actualSpaceId(col("history_space_id"), col("space_id")))
      .select("order_id", "order_number", "user_id", "actual_space_id", "order_type_c", "status_c", "check_in_date", "check_out_date", "order_check_in_date", "order_check_out_date", "current_date", "last_month_date")
      .withColumn("dateList", dateList(col("last_month_date"), col("current_date")))
    val order_join_date_DS = order_base_DF.as[(Long, String, Long, Long, String, String, String, String, String, String, String, String, String)]
    val order_join_date_DF = order_join_date_DS.flatMap(tup => {
      val order_id = tup._1
      val order_number = tup._2
      val user_id = tup._3
      val space_id = tup._4
      val order_type_c = tup._5
      val status_c = tup._6
      val check_in_date = tup._7
      val check_out_date = tup._8
      val order_check_in_date = tup._9
      val order_check_out_date = tup._10
      val current_date = tup._11
      val last_month_date = tup._12
      val dateList = tup._13
      dateList.split("\\|").map((order_id, order_number, user_id, space_id, order_type_c, status_c, check_in_date, check_out_date, order_check_in_date, order_check_out_date, current_date, last_month_date, _))
    }).toDF("order_id", "order_number", "user_id", "space_id", "order_type_c", "status_c", "check_in_date", "check_out_date", "order_check_in_date", "order_check_out_date", "current_date", "last_month_date", "date")

    val sql_rerent_table = "[order].rerent_period"
    val rerent_base_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_rerent_table, sqlProdProperties)
      .select("id", "order_id", "start_date", "end_date", "created_at")
      .where("status_c != 'canceled'")
      .withColumn("start_date_rerent", startDateToGMT(col("start_date")))
      .withColumn("end_date_rerent", endDateToGMT(col("end_date")))
      .withColumn("current_date", date_sub(current_date(), num))
      .withColumn("last_month_date", date_sub(col("current_date"), 30))
      .withColumn("diff_date", datediff(col("created_at"), col("last_month_date")))
      .where("diff_date >= 0")
      .select("id", "order_id", "start_date_rerent", "end_date_rerent")
      .withColumn("dateList", dateList(col("start_date_rerent"), col("end_date_rerent")))
    val rerent_base_DS = rerent_base_DF.as[(Long, Long, String, String, String)]
    val rerent_DF = rerent_base_DS.flatMap(tup => {
      val id = tup._1
      val order_id = tup._2
      val start_date_rerent = tup._3
      val end_date_rerent = tup._4
      val dateList = tup._5
      dateList.split("\\|").map((id, order_id, start_date_rerent, end_date_rerent, _))
    }).toDF("id", "order_id", "start_date_rerent", "end_date_rerent", "date")

    val order_join_rerent_DF = order_join_date_DF.join(rerent_DF, usingColumns_order_rerent, "left")
      .withColumn("on_rerent", isRerented(col("id")))
      .select("order_id", "order_number", "user_id", "space_id", "date", "on_rerent", "status_c")

    val sql_user_table = "[user].[user]"
    val order_join_rerent_user_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_user_table, sqlProdProperties)
      .selectExpr("id as user_id", "display_name as user_name")
      .join(order_join_rerent_DF, usingColumns_order_rerent_user, "right")

    val sql_space_table = "project.space"
    val order_join_rerent_user_space_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_space_table, sqlProdProperties)
      .selectExpr("id as space_id", "code as room")
      .join(order_join_rerent_user_DF, usingColumns_order_rerent_user_space, "right")
    val order_join_rerent_status_DF = order_join_rerent_user_space_DF
      .select("order_id", "order_number", "date", "user_name", "room", "on_rerent", "space_id", "status_c")

    val order_on_rerent_DF = order_join_rerent_status_DF.filter($"on_rerent".equalTo("true"))
    val order_on_rerent_count_DF = order_on_rerent_DF
      .groupBy("order_id")
      .count()
      .selectExpr("count as rerent_count", "order_id")
    val usingColumns_order_rerent_count = Seq("order_id")
    val order_on_rerent_join_count_DF = order_join_rerent_status_DF
      .join(order_on_rerent_count_DF, usingColumns_order_rerent_count, "left")
      .where("rerent_count is null or rerent_count < 15")
      .select("order_id", "order_number", "date", "user_name", "room", "on_rerent", "space_id", "status_c")
      .withColumn("datetimeList", dateTimeList(col("date")))
    val order_on_rerent_join_count_DS = order_on_rerent_join_count_DF.as[(Long, String, String, String, String, Boolean, Long, String, String)]
    val order_on_rerent_date_time_DF = order_on_rerent_join_count_DS.flatMap(tup => {
      val order_id = tup._1
      val order_number = tup._2
      val date = tup._3
      val user_name = tup._4
      val room = tup._5
      val on_rerent = tup._6
      val space_id = tup._7
      val status_c = tup._8
      val dateList = tup._9
      dateList.split("\\|").map((order_id, order_number, date, user_name, room, on_rerent, space_id, status_c, _))
    }).toDF("order_id", "order_number", "date", "user_name", "room", "on_rerent", "space_id", "status_c", "dateTime")
    val order_DF = order_on_rerent_date_time_DF
      .selectExpr("order_id", "order_number", "date", "dateTime as date_time_key", "user_name", "room", "on_rerent", "space_id", "status_c")
      .distinct()
      .sort(desc("date_time_key"))
    //    val current_day_on_rerent = order_DF.first().getAs[Boolean]("on_rerent")
    //    val current_day_order_id = order_DF.first().getAs[Long]("order_id")
    //    val current_day_date = order_DF.first().getAs[String]("date")

    //    val sql_ele_water_table = "iot.ele_water_every_hour"
    //    val ele_water_base_DF = spark.read.jdbc(sqlDwProperties.getProperty("url"), sql_ele_water_table, sqlDwProperties)
    //      .select("space_id", "zone_device_id", "zone_device_control_id", "current_value", "average_value", "accumulate_value", "last_value", "date_key", "date_time_key", "device_type", "zone_device_code")
    //      .withColumn("current_date", date_sub(current_date(), num - 3))
    //      .withColumn("last_month_date", date_sub(col("current_date"), 32))
    //      .withColumn("diff_date", datediff(col("current_date"), col("last_month_date")))
    //      .where("diff_date >= 0")
    //      .join(order_DF, usingColumns_order_ele_water, "right")
    //      .withColumn("actual_date_key", actualDate(col("date_time_key")))
    //      .select("order_id", "order_number", "space_id", "actual_date_key", "date_key", "date_time_key", "user_name", "room", "on_rerent", "zone_device_id", "zone_device_control_id", "device_type", "zone_device_code", "current_value", "average_value", "accumulate_value", "last_value")
    //      .sort(asc("order_id"), asc("space_id"), asc("zone_device_id"), asc("zone_device_control_id"), asc("actual_date_key"), asc("date_time_key"))

    //    ele_water_base_DF.printSchema()
    //    ele_water_base_DF.show(1000)
    //    val ele_water_base_RDD = ele_water_base_DF.rdd
    //    val ele_water_reduce_RDD = ele_water_base_RDD.map(row => {
    //      val order_id = row.getAs[Long]("order_id")
    //      val order_number = row.getAs[String]("order_number")
    //      val space_id = row.getAs[Long]("space_id")
    //      val actual_date_key = row.getAs[String]("actual_date_key")
    //      val date_time_key = row.getAs[String]("date_time_key")
    //      val user_name = row.getAs[String]("user_name")
    //      val room = row.getAs[String]("room")
    //      val on_rerent = row.getAs[Boolean]("on_rerent")
    //      val zone_device_id = row.getAs[Long]("zone_device_id")
    //      val zone_device_control_id = row.getAs[Long]("zone_device_control_id")
    //      val device_type = row.getAs[String]("device_type")
    //      val zone_device_code = row.getAs[String]("zone_device_code")
    //      val current_value = row.getAs[Double]("current_value")
    //      val accumulate_value = row.getAs[Double]("accumulate_value")
    //      val last_value = row.getAs[Double]("last_value")
    //      val key = s"${actual_date_key}-${order_id}-${device_type}"
    //      (key, (on_rerent, current_value, last_value, accumulate_value, order_id, order_number, space_id, user_name, room, zone_device_id, zone_device_control_id, device_type, zone_device_code, actual_date_key, date_time_key))
    //    }).reduceByKey((x1, x2) => {
    //      val accumulate_value_bef = Decimal(x1._4)
    //      val accumulate_value_curr = Decimal(x2._4)
    //      val accumulate_value = (accumulate_value_bef + accumulate_value_curr).toDouble
    //      (x2._1, x2._2, x2._3, accumulate_value, x2._5, x2._6, x2._7, x2._8, x2._9, x2._10, x2._11, x2._12, x2._13, x2._14, x2._15)
    //    })
    //    val ele_water_reduce_day_RDD = ele_water_reduce_RDD.map(tup => {
    //      val on_rerent = tup._2._1
    //      val current_value = tup._2._2
    //      val last_value = tup._2._3
    //      val accumulate_value = tup._2._4
    //      val order_id = tup._2._5
    //      val order_number = tup._2._6
    //      val space_id = tup._2._7
    //      val user_name = tup._2._8
    //      val room = tup._2._9
    //      val zone_device_id = tup._2._10
    //      val zone_device_control_id = tup._2._11
    //      val device_type = tup._2._12
    //      val zone_device_code = tup._2._13
    //      val actual_date_key = tup._2._14
    //      val key = s"${order_id}-${device_type}-${on_rerent}"
    //      (key, (on_rerent, current_value, last_value, accumulate_value, order_id, order_number, space_id, user_name, room, zone_device_id, zone_device_control_id, device_type, zone_device_code, actual_date_key, 1))
    //    }).reduceByKey((x1, x2) => {
    //      val accumulate_value_bef = Decimal(x1._4)
    //      val accumulate_value_curr = Decimal(x2._4)
    //      val accumulate_value = (accumulate_value_bef + accumulate_value_curr).toDouble
    //      (x2._1, x2._2, x2._3, accumulate_value, current_day_order_id, x2._6, x2._7, x2._8, x2._9, x2._10, x2._11, x2._12, x2._13, current_day_date, x1._15 + x2._15)
    //    }).filter(tup=>{
    //      !tup._2._1
    //    }).map(tup=>{
    //      val on_rerent = tup._2._1
    //      val consumption = tup._2._4
    //      val order_id = tup._2._5
    //      val order_number = tup._2._6
    //      val space_id = tup._2._7
    //      val user_name = tup._2._8
    //      val room = tup._2._9
    //      val zone_device_id = tup._2._10
    //      val zone_device_control_id = tup._2._11
    //      val device_type = tup._2._12
    //      val zone_device_code = tup._2._13
    //      val actual_date_key = tup._2._14
    //      val create_at = getrealTime("yyyy-MM-dd HH:mm:ss")
    //      Row(order_id,order_number,user_name,consumption,current_day_on_rerent,room,space_id,zone_device_id,zone_device_control_id,device_type,zone_device_code,actual_date_key,create_at)
    //    })
    //
    //    val reduceEleWaterDFSchema = StructType(
    //      Seq(
    //        StructField("order_id", LongType, false),
    //        StructField("order_number", StringType, false),
    //        StructField("user_name", StringType, false),
    //        StructField("consumption", DoubleType, false),
    //        StructField("on_rerent", BooleanType, false),
    //        StructField("room", StringType, false),
    //        StructField("space_id", LongType, false),
    //        StructField("zone_device_id", LongType, false),
    //        StructField("zone_device_control_id", LongType, false),
    //        StructField("device_type", StringType, false),
    //        StructField("zone_device_code", StringType, false),
    //        StructField("date_key", StringType, false),
    //        StructField("created_at", StringType, false)
    //      )
    //    )
    //    val sqlServerTable_consumption_target_data_dw = "iot.consumption_target_data"
    //    val consumptionTargetDataDF = spark.createDataFrame(ele_water_reduce_day_RDD, reduceEleWaterDFSchema)
    //    if (!consumptionTargetDataDF.isEmpty) {
    //      consumptionTargetDataDF
    //        .write
    //        .mode("append")
    //        .jdbc(sqlServerUrl_dw, sqlServerTable_consumption_target_data_dw, sqlServerProp_dw)
    //    }
  }
}

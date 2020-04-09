package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**
  * @author gaowei
  *         2020-02-24 0:26
  */
object ConsumptionOneDayEleWaterDataJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("ConsumptionOneDayEleWaterDataJob")

    val getCreatedAt = udf(() => {
      getrealTime("yyyy-MM-dd HH:mm:ss.SSS")
    })
    val getActualDateKey = udf((dateTimeKey : String) => {
      getActualDateTime(dateTimeKey)
    })
    import spark.implicits._
    val readEleWaterDeviceRedisDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "DEVICE_ELE_WATER")
      .load()
      .filter($"_id".contains("-"))
    val spaceDF = readEleWaterDeviceRedisDf.select("space_id","zone_device_id","zone_device_control_id").distinct()
    val keyspace = "iot_data_warehouse"
    val table_name = "ele_water_one_hour"
    val columns_space_id = Seq("space_id","zone_device_id","zone_device_control_id")
    //    val date_num = args(0).toInt
    //    val appointed = args(1)
    val appointed = getrealTime()
    val date_num = 1
    val number_arr = 0 until date_num
    number_arr.foreach(num => {
      val appointed_date = funAddDay(appointed,-num)
      val yesterday12 = getYesterday12(appointed_date)
      val today12 = getToday12(appointed_date)

      val readEleWaterDF = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> table_name, "keyspace" -> keyspace))
        .load
        .join(spaceDF,columns_space_id,"left")
        .select("space_id", "zone_device_id", "zone_device_control_id", "is_online", "is_scenario", "value", "date_time_key")
        .filter(s"date_time_key>='${yesterday12}' and date_time_key<'${today12}'")
        .withColumn("date_key",getActualDateKey(col("date_time_key")))
      if (!readEleWaterDF.isEmpty) {
        val readEleWaterRDD = readEleWaterDF.rdd
        val eleWaterRDD = readEleWaterRDD.map(row => {
          val date_time_key = row.getAs[String]("date_time_key")
          val value = row.getAs[Double]("value")
          val space_id = row.getAs[Long]("space_id")
          val zone_device_id = row.getAs[Long]("zone_device_id")
          val zone_device_control_id = row.getAs[Long]("zone_device_control_id")
          val is_online = row.getAs[Boolean]("is_online")
          val is_scenario = row.getAs[Boolean]("is_scenario")
          val date_key = row.getAs[String]("date_key")
          val key = s"${date_key}-${space_id}-${zone_device_id}-${zone_device_control_id}"
          (key, (value, 0.0, date_time_key, is_online, is_scenario))
        }).sortBy(_._2._3).reduceByKey((x1, x2) => {
          val value_bef = Decimal(x1._1)
          val value_curr = Decimal(x2._1)
          val consumption = value_curr - value_bef
          (value_bef.toDouble, consumption.toDouble , x2._3, x2._4, x2._5)
        })
        val eleWaterOneDay4CassandraRDD = eleWaterRDD.map(tup => {
          val keyArr = tup._1.split("-")
          val value = tup._2._1
          val consumption = tup._2._2
          val date_time_key = tup._2._3
          val is_online = tup._2._4
          val is_scenario = tup._2._5
          val space_id = keyArr(3).toLong
          val zone_device_id = keyArr(4).toLong
          val zone_device_control_id = keyArr(5).toLong
          val create_at = getrealTime()
          val date_key = getActualDateTime(date_time_key)
          Row(space_id, consumption, (Decimal(value)+Decimal(consumption)).toDouble, date_key, is_online, is_scenario, zone_device_id, zone_device_control_id, create_at)
        })
        val eleWaterOneDay4CassandraDFSchema = StructType(
          Seq(
            StructField("space_id", LongType, false),
            StructField("consumption", DoubleType, false),
            StructField("value", DoubleType, false),
            StructField("date_key", StringType, false),
            StructField("is_online", BooleanType, false),
            StructField("is_scenario", BooleanType, false),
            StructField("zone_device_id", LongType, false),
            StructField("zone_device_control_id", LongType, false),
            StructField("create_at", StringType, false)
          )
        )
        val eleWaterOneDay4CassandraDF = spark.createDataFrame(eleWaterOneDay4CassandraRDD, eleWaterOneDay4CassandraDFSchema)
        if (!eleWaterOneDay4CassandraDF.isEmpty) {
          val usingColumns_device = Seq("space_id", "zone_device_id", "zone_device_control_id")
          val sql_device_table = "iot.binding_master"
          val device_meta_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_device_table, sqlProdProperties)
            .selectExpr("space_id", "zone_device_id", "zone_device_control_id", "zone_device_control_tag as zone_device_control", "device_type_c", "zone_device_code as device")
            .where(" (device_type_c = 'elemeter' or device_type_c = 'watermeter') and zone_device_control = 1020")
          val eleWaterOneDay4CassandraDeviceDF = eleWaterOneDay4CassandraDF.join(device_meta_DF, usingColumns_device, "left")
          val sql_space_table = "project.space"
          val usingColumns_space = Seq("space_id")
          val device_space_DF = spark.read.jdbc(sqlProdProperties.getProperty("url"), sql_space_table, sqlProdProperties)
            .selectExpr("id as space_id", "type_c as room_type", "code as room")
            .where(" room_type = 'room' ")
          val eleWaterOneDay4CassandraDeviceRoomDF = eleWaterOneDay4CassandraDeviceDF.join(device_space_DF, usingColumns_space, "left")
            .withColumn("created_at", getCreatedAt())
            .selectExpr("consumption", "value as amount", "room", "device", "date_key", "created_at")
            .filter("device is not null")
          if (!eleWaterOneDay4CassandraDeviceRoomDF.isEmpty) {
            val sql_consumption_amount_data = "iot.ele_water_consumption_one_day"
            eleWaterOneDay4CassandraDeviceRoomDF
              .write
              .mode("append")
              .jdbc(sqlDwProperties.getProperty("url"), sql_consumption_amount_data, sqlDwProperties)
            val keyspace = "iot_data_warehouse"
            val table_name = "ele_water_one_day"
            eleWaterOneDay4CassandraDeviceRoomDF.write
              .mode("append")
              .format("org.apache.spark.sql.cassandra")
              .options(cassandraMaps(table_name,keyspace))
              .save()
          }
        }
      }
    })
  }
}

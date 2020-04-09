package com.stey.index.pipeline.jobs

import java.sql.Timestamp

import com.datastax.spark.connector.cql.CassandraConnector
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.sinks.JedisConnection
import com.stey.index.pipeline.utils.DateTimeUtil._
import com.stey.index.pipeline.utils.TypeConversionUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**
  * @author gaowei
  *         2019-12-03 19:22
  */
object HistoryEleWaterDataEtlOneHourJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    def numberCompare(number_old : Double,number_now : Double): Double ={
      if(number_old < number_now){
        number_old
      }else{
        number_now
      }
    }

    val spark = buildSparkSession("HistoryEleWaterDataEtlOneHourJob")
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
    import spark.implicits._
    val readEleWaterDeviceRedisDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "DEVICE_ELE_WATER")
      .load()
      .filter($"_id".contains("-"))
    val spaceDF = readEleWaterDeviceRedisDf.select("space_id","zone_device_id","zone_device_control_id")
    val keyspace = "iot_data_warehouse"
    val table_name = "ele_water_history_data_tmp"
    val using_device_date = Seq("space_id", "zone_device_id", "zone_device_control_id")
    val etlEleWaterDF = spark.read.schema(etlEleWaterDataSchema)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table_name, "keyspace" -> keyspace))
      .load
      .join(spaceDF, using_device_date, "left")
      .filter("ts>='2020-02-15'")
      .select("space_id", "zone_device_id", "zone_device_control_id", "ts", "is_online", "is_scenario", "value")
      .sort(asc("space_id"),asc("zone_device_id"),asc("zone_device_control_id"),asc("ts"))

    val table_name_one_hour = "ele_water_data_one_hour"
    val ele_water_one_hour = s"${keyspace}.${table_name_one_hour}"

    etlEleWaterDF.foreachPartition(it=>{
      val connector = CassandraConnector(spark.sparkContext.getConf)
      val jedis = JedisConnection.getConnections()
      while(it.hasNext){
        val row = it.next()
        val space_id = row.getAs[Long]("space_id")
        val zone_device_id = row.getAs[Long]("zone_device_id")
        val zone_device_control_id = row.getAs[Long]("zone_device_control_id")
        val ts = row.getAs[Timestamp]("ts")
        val now_ts = timeStampToString(ts)
        val is_online = row.getAs[Boolean]("is_online")
        val is_scenario = row.getAs[Boolean]("is_scenario")
        val value = row.getAs[Double]("value")
        val check_point_key = s"${space_id}-${zone_device_id}-${zone_device_control_id}"
        val check_point_date_time_key = jedis.hget(check_point_key,"date_time_key")
        val check_point_value = jedis.hget(check_point_key,"value")
        if(check_point_date_time_key != null && check_point_value != null){
          if(isToDouble(check_point_value)){
            val value_old = check_point_value.toDouble
            val value_compared = numberCompare(value_old,value)
            val time_list = getBetweenTimes(check_point_date_time_key,now_ts)
            if(!time_list.isEmpty){
              time_list.foreach(time=>{
                connector.withSessionDo(session =>
                  session.execute(
                    s"""insert into ${ele_water_one_hour} (space_id, zone_device_id, zone_device_control_id, value, is_online, is_scenario, date_time_key)
           values(${space_id}, ${zone_device_id}, ${zone_device_control_id}, ${value_compared}, ${is_online}, ${is_scenario}, '${time}')""")
                )
              })
            }
            connector.withSessionDo(session =>
              session.execute(
                s"""insert into ${ele_water_one_hour} (space_id, zone_device_id, zone_device_control_id, value, is_online, is_scenario, date_time_key)
              values(${space_id}, ${zone_device_id}, ${zone_device_control_id}, ${value}, ${is_online}, ${is_scenario},'${now_ts}')""")
            )
            jedis.hset(check_point_key, "date_time_key", now_ts)
            jedis.hset(check_point_key, "value", value.toString)
          }
        }else{
          connector.withSessionDo(session =>
            session.execute(
              s"""insert into ${ele_water_one_hour} (space_id, zone_device_id, zone_device_control_id, value, is_online, is_scenario, date_time_key)
           values(${space_id}, ${zone_device_id}, ${zone_device_control_id}, ${value}, ${is_online}, ${is_scenario}, '${now_ts}')""")
          )

          jedis.hset(check_point_key, "date_time_key", now_ts)
          jedis.hset(check_point_key, "value", value.toString)
        }
        jedis.close()
        //connector.closestLiveHost
      }
    })
  }
}

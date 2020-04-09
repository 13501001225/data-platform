package com.stey.index.pipeline.jobs

import com.datastax.spark.connector.cql.CassandraConnector
import com.google.gson.{JsonObject, JsonParser}
import com.stey.index.pipeline.modules.SparkModule
import com.stey.index.pipeline.utils.DateTimeUtil._
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

import scala.io.Source

/**
  * @author gaowei
  *         2019-12-20 15:50
  */
object HistoryEleWaterDataCollectedJob extends SparkModule {
  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession("HistoryEleWaterDataCollectedJob")

    def send_http_request(space_id: Long, zone_device_id: Long, zone_device_control_id: Long, start_time: Long, end_time: Long): String = {
      val url = s"https://gateway.stey.com/api/system/dataplatform/get-device-status-history?space_id=${space_id}&zone_device_id=${zone_device_id}&zone_device_control_id=${zone_device_control_id}&start_time=${start_time}&end_time=${end_time}"
      val httpclient = new DefaultHttpClient()
      try {
        val response = httpclient.execute(new HttpGet(url))
        val entity = response.getEntity
        val result = Source.fromInputStream(entity.getContent).getLines().mkString("\n")
        result
      } catch {
        case ex: Exception => {
          "Exception"
        }
      }
    }

    val connector = CassandraConnector(spark.sparkContext.getConf)
    val start_time = 1583380800000L
    val end_time = 1583463600000L
    //    val end_time_test = 1570071600000L
    //    val start_time = args(0).toLong
    //    val end_time = args(1).toLong
    import spark.implicits._
    val deviceEleWaterDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("infer.schema", true)
      .option("table", "DEVICE_ELE_WATER")
      .load()
      .filter($"_id".contains("-"))

    deviceEleWaterDf.foreachPartition(it => {
      while (it.hasNext) {
        val row = it.next()
        val space_id = row.getAs[String]("space_id").toLong
        val zone_device_id = row.getAs[String]("zone_device_id").toLong
        val zone_device_control_id = row.getAs[String]("zone_device_control_id").toLong

        val http_request = send_http_request(space_id, zone_device_id, zone_device_control_id, start_time, end_time)
        if (http_request != null && !"".equals(http_request)) {
          val isReads = new JsonParser().parse(http_request).asInstanceOf[JsonObject].has("data")
          if (isReads) {
            val keyspace = "iot_data_warehouse"
            val table_name = "ele_water_history_data_collected"
            //          val table_name_test = "ele_water_history_data_tmp_test"
            val ele_water_history_data_tmp = s"${keyspace}.${table_name}"
            val reads = new JsonParser().parse(http_request).asInstanceOf[JsonObject].get("data").asInstanceOf[JsonObject].get("reads").getAsJsonArray.iterator()
            while (reads.hasNext) {
              val json = reads.next()
              val space_id = json.getAsJsonObject.get("spaceId").getAsLong
              val zone_device_id = json.getAsJsonObject.get("zoneDeviceId").getAsLong
              val zone_device_control_id = json.getAsJsonObject.get("zoneDeviceControlId").getAsLong
              val is_scenario = json.getAsJsonObject.get("isScenario").getAsBoolean
              val is_online = json.getAsJsonObject.get("isOnline").getAsBoolean
              val value = json.getAsJsonObject.get("value").getAsDouble
              val ts = json.getAsJsonObject.get("ts").getAsString
              connector.withSessionDo(session =>
                session.execute(
                  s"""insert into ${ele_water_history_data_tmp}
             (space_id, zone_device_id, zone_device_control_id,ts,is_online,is_scenario,value)
             values
             (${space_id}, ${zone_device_id}, ${zone_device_control_id},'${ts}',${is_online},${is_scenario},${value})""")
              )
            }
            println("end" + space_id + "-" + zone_device_id + "-" + zone_device_control_id + ":" + getrealTime("yyyy-MM-dd HH:mm:ss"))
          } else {
            println(space_id + "-" + zone_device_id + "-" + zone_device_control_id + ":" + getrealTime("yyyy-MM-dd HH:mm:ss"))
            println(new JsonParser().parse(http_request).asInstanceOf[JsonObject])
          }
          Thread.sleep(5000)
        }
      }
    })
  }
}

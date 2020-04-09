package com.stey.index.pipeline.jobs

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.SparkConf
//import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

//import scala.collection.mutable.ArrayBuffer

/**
  * @author gaowei
  *         2020-02-18 12:45
  */
object GsonParseJsonDemoJob {
  case class DeviceData(spaceId:Long,zoneDeviceId:Long,zoneDeviceControlId:Long,ts:String,isOnline:Boolean,value:Double,isScenario:Boolean)
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("GsonParseJsonDemoJob")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val test = """{"data":{"reads":[{"spaceId":21,"zoneDeviceId":312,"zoneDeviceControlId":794,"isScenario":false,"isOnline":true,"value":"15.0","ts":"2019-06-16T08:00:00Z"},{"spaceId":21,"zoneDeviceId":312,"zoneDeviceControlId":794,"isScenario":false,"isOnline":true,"value":"15.0","ts":"2019-06-16T09:00:00Z"},{"spaceId":21,"zoneDeviceId":312,"zoneDeviceControlId":794,"isScenario":false,"isOnline":true,"value":"15.0","ts":"2019-06-16T10:00:00Z"}]}}"""
    val reads = Seq(new JsonParser().parse(test).asInstanceOf[JsonObject].get("data").asInstanceOf[JsonObject].get("reads").getAsJsonArray.iterator().toString)

    val schema_df = StructType(
      Seq(
        StructField("spaceId", LongType, false),
        StructField("zoneDeviceId", LongType, false),
        StructField("zoneDeviceControlId", LongType, false),
        StructField("ts", TimestampType, false),
        StructField("isOnline", BooleanType, false),
        StructField("value", StringType, true),
        StructField("isScenario", BooleanType, false)
      )
    )

    import spark.implicits._
    val ds = spark.createDataset(reads)
//      .select(from_json($"value", schema_df) as "data")
    ds.show(false)

//    val deviceArrayBuffer = new ArrayBuffer[Row]()
//    while(reads.hasNext){
//      val a = reads.next()
//      val space_id = a.getAsJsonObject.get("spaceId").getAsLong
//      val zone_device_id = a.getAsJsonObject.get("zoneDeviceId").getAsLong
//      val zone_device_control_id = a.getAsJsonObject.get("zoneDeviceControlId").getAsLong
//      val is_scenario = a.getAsJsonObject.get("isScenario").getAsBoolean
//      val is_online = a.getAsJsonObject.get("isOnline").getAsBoolean
//      val value = a.getAsJsonObject.get("value").getAsDouble
//      val ts = a.getAsJsonObject.get("ts").getAsString
//      val device = Row(space_id,zone_device_id,zone_device_control_id,ts,is_online,value,is_scenario)
//      deviceArrayBuffer.append(device)
//    }
//
//    val deviceRDD = spark.sparkContext.makeRDD(deviceArrayBuffer)
//
//    val cassandraSchema = StructType(
//      Seq(
//        StructField("space_id", LongType, false),
//        StructField("zone_device_id", LongType, false),
//        StructField("zone_device_control_id", LongType, false),
//        StructField("ts", StringType, false),
//        StructField("is_online", BooleanType, false),
//        StructField("value", DoubleType, true),
//        StructField("is_scenario", BooleanType, false)
//      )
//    )
//    val cassandraDF = spark.createDataFrame(deviceRDD,cassandraSchema)
//    cassandraDF.show()
  }
}

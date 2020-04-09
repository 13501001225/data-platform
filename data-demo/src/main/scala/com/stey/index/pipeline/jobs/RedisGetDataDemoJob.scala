package com.stey.index.pipeline.jobs

import com.stey.index.pipeline.sinks.JedisConnection
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
/**
  * @author gaowei
  *         2020-02-19 13:47
  */
object RedisGetDataDemoJob {
  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf()
//      .setAppName("TestRedisGetData")
//      .set("spark.redis.host", "192.168.25.241")
//      .set("spark.redis.port", "7001")
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .config(conf)
//      .getOrCreate()
//
//    val readInvalidDeviceRedisDf = spark.read
//      .format("org.apache.spark.sql.redis")
//      .option("infer.schema", true)
//      .option("table", "INVALID_DEVICE")
//      .load()
//    readInvalidDeviceRedisDf.show(1000)
    val jedis = JedisConnection.getConnections()

//    jedis.hset("9-10-778", "date_time_key", "2020-03-10 13:00:00")
//    jedis.hset("9-10-778", "value", "3.3")
//    println(jedis.hget("9-10-778","date_time_key"))

//    jedis.hdel("9-10-778","date_time_key","value")
    jedis.close()
  }
}

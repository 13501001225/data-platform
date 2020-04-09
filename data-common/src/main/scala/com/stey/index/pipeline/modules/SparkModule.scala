package com.stey.index.pipeline.modules

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author gaowei
  *         2020-03-02 13:39
  */
class SparkModule extends ConfigurationModule {
  def buildSparkSession(appName:String): SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config(
        new SparkConf()
          .set("spark.cassandra.connection.host", configuration.getString("spark.cassandra.connection.host"))
          .set("spark.cassandra.connection.port", configuration.getString("spark.cassandra.connection.port"))
          .set("spark.cassandra.auth.username", configuration.getString("spark.cassandra.auth.username"))
          .set("spark.cassandra.auth.password", configuration.getString("spark.cassandra.auth.password"))
          .set("spark.redis.host", configuration.getString("spark.redis.host"))
          .set("spark.redis.port", configuration.getString("spark.redis.port"))
      )
      .appName(appName)
      .getOrCreate()
    session
  }

  def optionKafkaMaps(): scala.collection.Map[String, String] ={
    var extraOptions = new scala.collection.mutable.HashMap[String, String]
    extraOptions += ("subscribe" -> configuration.getString("spark.kafka.prod.topic"))
    extraOptions += ("kafka.bootstrap.servers" -> configuration.getString("spark.kafka.prod.broker"))
    extraOptions += ("kafka.sasl.mechanism" -> configuration.getString("spark.kafka.prod.sasl_mechanism"))
    extraOptions += ("kafka.security.protocol" -> configuration.getString("spark.kafka.prod.protocol"))
    extraOptions += ("kafka.sasl.jaas.config" -> configuration.getString("spark.kafka.prod.sasl_eh"))
    extraOptions += ("kafka.request.timeout.ms" -> configuration.getString("spark.kafka.prod.timeout_request"))
    extraOptions += ("kafka.session.timeout.ms" -> configuration.getString("spark.kafka.prod.timeout_session"))
    extraOptions += ("failOnDataLoss" -> configuration.getString("spark.kafka.prod.data_los"))
    extraOptions
  }

  def producerConfig : Properties = {
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", configuration.getString("spark.kafka.cluster.producer.broker"))
    kafkaProperties.put("request.timeout.ms", configuration.getString("spark.kafka.cluster.producer.timeout_request"))
    kafkaProperties.put("session.timeout.ms", configuration.getString("spark.kafka.cluster.producer.timeout_session"))
    kafkaProperties.put("key.serializer", configuration.getString("spark.kafka.cluster.producer.key_serializer"))
    kafkaProperties.put("value.serializer", configuration.getString("spark.kafka.cluster.producer.value_serializer"))
    kafkaProperties.put("auto.offset.reset", configuration.getString("spark.kafka.cluster.producer.offset_reset"))
    kafkaProperties.put("enable.auto.commit", configuration.getString("spark.kafka.cluster.producer.auto_commit"))
    kafkaProperties
  }

  def consumerMaps(group: String): Map[String, String] ={
    val kafkaMaps = Map[String, String](
      "bootstrap.servers" -> configuration.getString("spark.kafka.cluster.consumer.broker"),
      "key.deserializer" -> configuration.getString("spark.kafka.cluster.consumer.key_deserializer"),
      "value.deserializer" -> configuration.getString("spark.kafka.cluster.consumer.value_deserializer"),
      "group.id" -> group,
      "auto.offset.reset" -> configuration.getString("spark.kafka.cluster.consumer.offset_reset"),
      "enable.auto.commit" -> configuration.getString("spark.kafka.cluster.consumer.auto_commit")
    )
    kafkaMaps
  }

  def sqlProdProperties : Properties = {
    val prop = new Properties()
    prop.put("driver", configuration.getString("db.prod.driver"))
    prop.put("user", configuration.getString("db.prod.user"))
    prop.put("password", configuration.getString("db.prod.password"))
    prop.put("url", configuration.getString("db.prod.url"))
    prop
  }

  def sqlDwProperties : Properties = {
    val prop = new Properties()
    prop.put("driver", configuration.getString("db.dw.driver"))
    prop.put("user", configuration.getString("db.dw.user"))
    prop.put("password", configuration.getString("db.dw.password"))
    prop.put("url", configuration.getString("db.dw.url"))
    prop
  }

  def cassandraMaps(table: String,ks:String): Map[String, String] ={
    val map = Map( "table" -> table, "keyspace" -> ks,
      "output.consistency.level" -> "ALL", "ttl" -> "10000000")
    map
  }
}
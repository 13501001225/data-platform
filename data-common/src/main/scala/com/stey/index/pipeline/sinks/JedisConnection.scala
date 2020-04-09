package com.stey.index.pipeline.sinks

import redis.clients.jedis.{HostAndPort, JedisCluster}
import java.util.HashSet

import com.stey.index.pipeline.modules.ConfigurationModule
/**
  * @author gaowei
  *         2020-03-09 16:59
  */
object JedisConnection extends ConfigurationModule{
  val jedisClusterNodes = new HashSet[HostAndPort]
  jedisClusterNodes.add(new HostAndPort(configuration.getString("spark.redis_cluster.host1"), configuration.getString("spark.redis_cluster.port1").toInt))
  jedisClusterNodes.add(new HostAndPort(configuration.getString("spark.redis_cluster.host1"), configuration.getString("spark.redis_cluster.port2").toInt))
  jedisClusterNodes.add(new HostAndPort(configuration.getString("spark.redis_cluster.host2"), configuration.getString("spark.redis_cluster.port3").toInt))
  jedisClusterNodes.add(new HostAndPort(configuration.getString("spark.redis_cluster.host2"), configuration.getString("spark.redis_cluster.port4").toInt))
  jedisClusterNodes.add(new HostAndPort(configuration.getString("spark.redis_cluster.host3"), configuration.getString("spark.redis_cluster.port5").toInt))
  jedisClusterNodes.add(new HostAndPort(configuration.getString("spark.redis_cluster.host3"), configuration.getString("spark.redis_cluster.port6").toInt))
  val jc = new JedisCluster(jedisClusterNodes)

  def getConnections() ={
    new JedisCluster(jedisClusterNodes)
  }

}

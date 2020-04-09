package com.stey.index.pipeline.modules

import com.typesafe.config.{Config, ConfigFactory}
/**
  * gaowei 
  * 2020-04-09 1:54 
  */
trait ConfigurationModule {
  lazy val configuration: Config = ConfigFactory.load
}

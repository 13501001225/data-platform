package com.stey.index.pipeline.utils

import scala.util.Success
/**
  * gaowei 
  * 2020-04-09 1:57 
  */
object TypeConversionUtil {
  def main(args: Array[String]): Unit = {
    println(isToDouble("a"))
  }
  def isToInt(str:String): Boolean ={
    val r1=scala.util.Try(str.toInt)
    val result = r1 match {
      case Success(_) => true ;
      case _ =>  false
    }
    result
  }
  def isToDouble(str:String): Boolean ={
    val r1=scala.util.Try(str.toDouble)
    val result = r1 match {
      case Success(_) => true ;
      case _ =>  false
    }
    result
  }
}

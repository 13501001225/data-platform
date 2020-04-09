package com.stey.index.pipeline.jobs

import scala.collection.mutable

/**
  * @author gaowei
  *         2020-02-19 21:52
  */
object MapFoldJob {
  def main(args: Array[String]): Unit = {
    val map1 = mutable.Map("a" -> 1,"b" -> 2,"c" -> 3)
    val map2 = mutable.Map("a" -> 3,"c" -> 2,"d" -> 1)

    val stringToInt1 = map1.foldLeft(map2)((map,t)=> {
      println(t._1)
      map
    })
    println(stringToInt1)
    val stringToInt2 = map1.foldLeft(map2)((map, t) => {
      map(t._1) = map.getOrElse(t._1, 0) + t._2
      map
    })
    println(stringToInt2)
  }
}

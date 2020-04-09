package com.stey.index.pipeline.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author gaowei
  *         2019-11-05 14:57
  */
object DateTimeUtil {
  def main(args: Array[String]): Unit = {
    //    val time1 = "2019-12-26 22:59:03.668"
    //    val time2 = "2019-12-26 00:15:00.000"
    //    println(subtractionTime(time1,time2))
    //    println(funAddMinute(funAddHourMinute0(time2,-1,"yyyy-MM-dd HH:mm:ss.SSS"),55,"yyyy-MM-dd HH:mm:ss.SSS"))
    //    println(funAddMinuteSecond0("2019-12-26 22:59:03.668",1,"yyyy-MM-dd HH:mm:ss.SSS"))

    //    val today = getrealTime("yyyy-MM-dd HH:mm:ss.SSSZ")
    //    println(getToday12("2019-12-26 22:59:03"))
    //    val last_day = funAddDay(today,-1,"yyyy-MM-dd HH:mm:ss")
    //    val last_month = funAddDay(today,-30,"yyyy-MM-dd HH:mm:ss")
    //    println(funAddDay(getrealTime("yyyy-MM-dd"),-30,"yyyy-MM-dd"))
    //    date_arr.foreach(println(_))
    //        println(getTimes("2019-10-06"))
    //    println(getActualDate("2019-10-06 11:01:00"))

    //    println(funNextMinute("2019-10-06 11:01:00.123"))
    //    println(funCurrentMin("2019-10-06 11:01:24.123"))
    //    println(getBetweenTimes("2020-02-27 11:00:00.122","2020-02-27 14:00:00.122"))
    //    val list = getBetweenTimes("2020-02-27 11:00:00.122","2020-02-27 12:00:00.122")
    //    list.foreach(t=>println(s"${t.trim}"))

    //    println(getActualDateTime("2020-03-09 13:00:00"))
    val date_num = 2
    val number_arr = 0 until date_num
    number_arr.foreach(num => {
      val appointed_date = funAddDay(getrealTime(),-num)
      val yesterday12 = getYesterday12(appointed_date)
      val today12 = getToday12(appointed_date)
      println(yesterday12 + "  "+ today12)
    })
  }

  def mend0(date: Int): String = {
    if (date < 10) {
      "0" + date
    } else {
      date.toString
    }
  }

  /**
    * 获取当前时间
    *
    * @param pattern pattern 如"yyyyMMddHHmmss"
    */
  def getrealTime(pattern: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val timeTag = System.currentTimeMillis()
    val changeTime = new Date(timeTag)
    val dataFormat = new SimpleDateFormat(pattern)
    dataFormat.format(changeTime)
  }

  /**
    * 获取当前时间戳（精确到毫秒）
    */
  def getTimestamp(): Long = {
    val time = getrealTime("yyyy-MM-dd'T'HH:mm:ss.SSS")
    funStringToTimeStamp(time, "yyyy-MM-dd'T'HH:mm:ss.SSS")
  }

  /**
    * 将时间字符串修改为时间戳
    *
    * @param time          时间
    * @param timeFormatted 时间格式 如 "yyyyMMddHHmmss"
    * @return 精确到毫秒的时间戳
    */
  def funStringToTimeStamp(time: String, timeFormatted: String): Long = {
    val fm = new SimpleDateFormat(timeFormatted)
    fm.setTimeZone(TimeZone.getTimeZone("UTC"))
    val dt = fm.parse(time)
    dt.getTime
  }

  /**
    * 将时间戳修改为时间字符串
    *
    * @param timestamp 时间
    * @param pattern   时间格式 如 "yyyyMMddHHmmss"
    */
  def funTimeStampLongToString(timestamp: Long, pattern: String): String = {
    val fm = new SimpleDateFormat(pattern)
    fm.setTimeZone(TimeZone.getTimeZone("UTC"))
    val mata_ts_format = fm.format(timestamp)
    mata_ts_format
  }

  /**
    * 将时间戳转换为时间
    *
    * @param timestamp     精确到毫秒
    * @param timeFormatted 时间格式如“HH”表示按24小时制返回时间戳对应的小时
    */
  def timestampToString(timestamp: String, timeFormatted: String): String = {
    val fm = new SimpleDateFormat(timeFormatted)
    val time = fm.format(new Date(timestamp.toLong))
    time
  }

  /**
    * 获取传入日期与当前日期的天数差
    *
    * @param day          待比较日期(仅包含月日)
    * @param dayFormatted 待比较日期格式(例如:MMdd或MM-dd)
    */
  def daysBetweenToday(day: String, dayFormatted: String): Int = {
    val calendar = Calendar.getInstance
    calendar.setTime(new Date())
    val today = calendar.get(Calendar.DAY_OF_YEAR)
    val year = calendar.get(Calendar.YEAR)
    val inputF = new SimpleDateFormat("yyyy" + dayFormatted)
    val date = inputF.parse(year + day)
    calendar.setTime(date)
    val inputDay = calendar.get(Calendar.DAY_OF_YEAR)
    var days = today - inputDay
    if (days < 0) {
      val beforeYearDate = inputF.parse((year - 1) + day)
      calendar.setTime(beforeYearDate)
      days = calendar.getActualMaximum(Calendar.DAY_OF_YEAR) - calendar.get(Calendar.DAY_OF_YEAR) + today
    }
    return days
  }

  /**
    * 将毫秒级时间戳转化成分钟级时间戳
    *
    * @param time 毫秒级时间戳
    * @return 分钟级时间戳
    */
  def getMinTimestamp(time: Long): Long = {
    val minTime = time / (1000 * 60)
    minTime
  }

  /**
    * 将时间字符串修改为格式
    *
    * @param inpuTime        输入时间
    * @param inputFormatted  输入时间格式
    * @param outputFormatted 输出时间格式
    */
  def formatTime(inpuTime: String, inputFormatted: String, outputFormatted: String): String = {
    val inputF = new SimpleDateFormat(inputFormatted)
    val outputF = new SimpleDateFormat(outputFormatted)
    val inputT = inputF.parse(inpuTime)
    outputF.format(inputT)
  }

  /**
    * 获取传入时间戳的天数差
    *
    * @param t1 较小时间戳
    * @param t2 较大时间戳
    */
  def caculate2Days(t1: Long, t2: Long): Int = {
    import java.util.Calendar
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(t2)
    val t2Day = calendar.get(Calendar.DAY_OF_YEAR)
    calendar.setTimeInMillis(t1)
    val t1Day = calendar.get(Calendar.DAY_OF_YEAR)
    var days = t2Day - t1Day
    if (days < 0) {
      days = calendar.getActualMaximum(Calendar.DAY_OF_YEAR) - t1Day + t2Day
    }
    return days;
  }

  /**
    * 判断nowTime是否在startTime与endTime之间
    *
    * @param nowTime
    * @param startTime
    * @param endTime
    * @param formater
    */
  def isBetweenDate(nowTime: String, startTime: String, endTime: String, formater: String): Boolean = {
    if (nowTime != null && startTime != null && endTime != null) {
      val df = new SimpleDateFormat(formater)
      val nowDate = df.parse(nowTime)
      val startDate = df.parse(startTime)
      val endDate = df.parse(endTime)
      if ((nowDate.getTime == startDate.getTime) || (nowDate.getTime == endDate.getTime)) return true
      if (nowDate.after(startDate) && nowDate.before(endDate)) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
    * 时间增加天数，返回时间
    *
    * @param date 入参时间
    * @param num  增加的天数
    */
  def funAddDate(date: String, num: Int): String = {
    val myformat = new SimpleDateFormat("yyyyMMdd")
    var dnow = new Date()
    dnow = myformat.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.DAY_OF_MONTH, num)
    val newday = cal.getTime
    myformat.format(newday)
  }

  /**
    * 时间增加天数，返回时间
    *
    * @param date 入参时间
    * @param num  增加的天数
    */
  def funAddDay(date: String, num: Int,pattern: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val myformat = new SimpleDateFormat(pattern)
    var dnow = new Date()
    dnow = myformat.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.DAY_OF_MONTH, num)
    val newday = cal.getTime
    myformat.format(newday)
  }

  /**
    * 时间增加小时，返回时间
    *
    * @param date 入参时间
    * @param num  增加的天数
    */
  def funAddHour(date: String, num: Int, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    var dnow = new Date()
    dnow = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.HOUR, num)
    val newday = cal.getTime
    format.format(newday)
  }

  /**
    * 时间增加小时，返回0分时间
    *
    * @param date 入参时间
    * @param num  增加的天数
    */
  def funAddHourMinute0(date: String, num: Int, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    val dnow = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.HOUR, num)
    val min = cal.get(Calendar.MINUTE)
    cal.add(Calendar.MINUTE, -min)
    val newday = cal.getTime
    format.format(newday)
  }

  /**
    * 时间增加分钟，返回时间
    *
    * @param date 入参时间
    * @param num  增加的分钟数
    */
  def funAddMinute(date: String, num: Int, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    var dnow = new Date()
    dnow = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.MINUTE, num)
    val newday = cal.getTime
    format.format(newday)
  }

  /**
    * 时间增加分钟，返回时间
    *
    * @param date 入参时间
    * @param num  增加的分钟数
    */
  def funAddMinuteSecond0(date: String, num: Int, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    var dnow = new Date()
    dnow = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.MINUTE, num)
    val second = cal.get(Calendar.SECOND)
    cal.add(Calendar.SECOND, -second)
    val mill_second = cal.get(Calendar.MILLISECOND)
    cal.add(Calendar.MILLISECOND, -mill_second)
    val newday = cal.getTime
    format.format(newday)
  }

  /**
    * 时间增加秒，返回时间
    *
    * @param date 入参时间
    * @param num  增加的秒数
    */
  def funAddSecond(date: String, num: Int, format: String): String = {
    val myformat = new SimpleDateFormat(format)
    var dnow = new Date()
    dnow = myformat.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.SECOND, num)
    val newday = cal.getTime
    myformat.format(newday)
  }

  /** 获取过去几天的时间数组
    *
    * @param date
    * @param num
    */
  def getPastDays(date: String, num: Int): Array[String] = {
    val buffer = new ArrayBuffer[String]()
    val range = 0 until num
    for (i <- range) {
      buffer.append(funAddDate(date, -i))
    }
    buffer.toArray
  }

  /** 获取过去几小时的时间数组
    *
    * @param date
    * @param num
    */
  def getPastHours(date: String, num: Int, interval: Int): Array[String] = {
    val buffer = new ArrayBuffer[String]()
    val range = 0 until num
    for (i <- range) {
      buffer.append(funAddHour(date, -i * interval, "yyyy-MM-dd HH:mm:ss"))
    }
    buffer.toArray
  }

  /** 获取年月日，时分秒
    *
    * @param str_date
    * @return String
    */
  def dateStringFormat(str_date: String, pattern: String="yyyy-MM-dd HH:mm:ss"): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val second = cal.get(Calendar.SECOND)
    //println("date:"+year+"-"+month+"-"+day+"-"+hour+"-"+min+"-"+second)
    year + "-" + mend0(month) + "-" + mend0(day) + "-" + mend0(hour) + "-" + mend0(min) + "-" + mend0(second)
  }

  /** 获取年月日，时分秒
    *
    * @param str_date
    * @return String
    */
  def dateStringHourKey(str_date: String, pattern: String="yyyy-MM-dd HH:mm:ss"): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val second = cal.get(Calendar.SECOND)
    //println("date:"+year+"-"+month+"-"+day+"-"+hour+"-"+min+"-"+second)
    year + "-" + mend0(month) + "-" + mend0(day) + "-" + mend0(hour)
  }

  /** 获取年月日，时分秒
    *
    * @param str_date
    * @return String
    */
  def dateHourKey(str_date: String, pattern: String="yyyy-MM-dd HH:mm:ss"): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val second = cal.get(Calendar.SECOND)
    //println("date:"+year+"-"+month+"-"+day+"-"+hour+"-"+min+"-"+second)
    year + mend0(month) + mend0(day) + mend0(hour)
  }

  /** 获取年月日
    *
    * @param str_date
    * @return String
    */
  def dateStringFormatDate(str_date: String, pattern: String): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)

    //    val hour = cal.get(Calendar.HOUR_OF_DAY)
    //    val min = cal.get(Calendar.MINUTE)
    //    val second = cal.get(Calendar.SECOND)
    //println("date:"+year+"-"+month+"-"+day+"-"+hour+"-"+min+"-"+second)
    year + "-" + mend0(month) + "-" + mend0(day)
  }

  def subtractionTime(startTime: String, endTime: String): Long = {
    val start = funStringToTimeStamp(startTime, "yyyy-MM-dd HH:mm:ss")
    val end = funStringToTimeStamp(endTime, "yyyy-MM-dd HH:mm:ss")
    val second = (end - start) / 1000
    second
  }

  /**
    * 下一分钟时间，返回时间格式
    *
    */
  def funNextMinute(str_date: String,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val num = 1
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    cal.add(Calendar.MINUTE, num)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val second = "00"

    //println("date:"+year+"-"+month+"-"+day+"-"+hour+"-"+min+"-"+second)
    val newday = year + "-" + mend0(month) + "-" + mend0(day) + " " + mend0(hour) + ":" + mend0(min) + ":" + second
    newday
  }

  /**
    * 时间增加天数，返回时间
    *
    */
  def funNextHour(str_date: String,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val num = 1
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    cal.add(Calendar.HOUR_OF_DAY, num)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    //val min = cal.get(Calendar.MINUTE)
    val second = "00"
    val minute = "00"
    val newday = year + "-" + mend0(month) + "-" + mend0(day) + " " + mend0(hour) + ":" + minute + ":" + second
    newday
  }

  /**
    * 时间增加天数，返回时间
    *
    */
  def funCurrentHour(str_date: String,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    //    cal.add(Calendar.HOUR_OF_DAY)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    //val min = cal.get(Calendar.MINUTE)
    val second = "00"
    val minute = "00"
    val newday = year + "-" + mend0(month) + "-" + mend0(day) + " " + mend0(hour) + ":" + minute + ":" + second
    newday
  }

  def funCurrentMin(str_date: String,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    //    cal.add(Calendar.HOUR_OF_DAY)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    //val min = cal.get(Calendar.MINUTE)
    val second = "00"
    val minute = cal.get(Calendar.MINUTE)
    val newday = year + "-" + mend0(month) + "-" + mend0(day) + " " + mend0(hour) + ":" +  mend0(minute) + ":" + second
    newday
  }

  /**
    * 时间增加天数，返回时间
    *
    */
  def funNextTenMinute(str_date: String,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val num = 10
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    cal.add(Calendar.MINUTE, num)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val second = "00"
    val newday = year + "-" + mend0(month) + "-" + mend0(day) + " " + mend0(hour) + ":" + mend0(min) + ":" + second
    newday
  }

  /**
    * 下一分钟时间，返回时间格式
    *
    */
  def funMinute4Second0(str_date: String,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val dataFormat = new SimpleDateFormat(pattern)
    val data = dataFormat.parse(str_date)
    val cal = Calendar.getInstance
    cal.setTime(data)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val second = "00"

    //println("date:"+year+"-"+month+"-"+day+"-"+hour+"-"+min+"-"+second)
    val newday = year + "-" + mend0(month) + "-" + mend0(day) + " " + mend0(hour) + ":" + mend0(min) + ":" + second
    newday
  }

  /**
    * 将时间戳修改为时间字符串
    *
    * @param timestamp 时间
    */
  def timeStampToString(timestamp: Timestamp,pattern:String = "yyyy-MM-dd HH:mm:ss"): String = {
    val fm = new SimpleDateFormat(pattern)
    val datetime = fm.format(timestamp)
    datetime
  }

  /**
    * 将时间戳修改为时间字符串
    *
    * @param timestamp 时间
    */
  def timeStampToFormatString(timestamp: Timestamp, pattern: String): String = {
    val fm = new SimpleDateFormat(pattern)
    val datetime = fm.format(timestamp)
    datetime
  }

  def dateToFormatString(date: Date, pattern: String): String = {
    val fm = new SimpleDateFormat(pattern)
    val datetime = fm.format(date)
    datetime
  }

  /**
    * 获取两个日期之间的日期
    *
    * @param start 开始日期
    * @param end   结束日期
    * @return 日期集合
    */
  def getBetweenDates(start: String, end: String) = {
    val startData = new SimpleDateFormat("yyyy-MM-dd").parse(start); //定义起始日期
    val endData = new SimpleDateFormat("yyyy-MM-dd").parse(end); //定义结束日期

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var buffer = new ListBuffer[String]
    buffer += dateFormat.format(startData.getTime())
    val tempStart = Calendar.getInstance()

    tempStart.setTime(startData)
    //tempStart.add(Calendar.DAY_OF_YEAR, 1)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)
    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer += dateFormat.format(endData.getTime())
    //    val tempNext = Calendar.getInstance()
    //    tempNext.setTime(endData)
    //    tempNext.add(Calendar.DAY_OF_YEAR, 1)
    //    buffer += dateFormat.format(tempNext.getTime())
    buffer.toList.mkString("|")
  }

  /**
    * 获取两个日期之间的日期
    *
    * @return 日期集合
    */
  def getTimes(date: String) = {
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(date); //定义起始日期
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var buffer = new ListBuffer[String]
    val tempStart = Calendar.getInstance()

    tempStart.setTime(initData)
    tempStart.add(Calendar.HOUR, 13)
    tempStart.add(Calendar.MINUTE, 0)
    tempStart.add(Calendar.SECOND, 0)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(initData)
    tempEnd.add(Calendar.DAY_OF_MONTH,1)
    tempEnd.add(Calendar.HOUR, 13)
    tempEnd.add(Calendar.MINUTE, 0)
    tempEnd.add(Calendar.SECOND, 0)

    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.HOUR, 1)
    }
    //buffer += dateFormat.format(tempEnd.getTime)
    buffer.toList.mkString("|")
  }

  def getActualDate(datetime: String) = {
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(datetime); //定义起始日期
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val tempStart = Calendar.getInstance()
    tempStart.setTime(initData)
    tempStart.add(Calendar.HOUR, 12)
    tempStart.add(Calendar.MINUTE, 0)
    tempStart.add(Calendar.SECOND, 0)

    val currentData = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datetime); //定义起始日期
    val tempCurrent = Calendar.getInstance()
    tempCurrent.setTime(currentData)

    if (tempStart.before(tempCurrent)) {
      dateFormat.format(tempCurrent.getTime)
    } else {
      tempCurrent.add(Calendar.HOUR, - 24)
      dateFormat.format(tempCurrent.getTime)
    }
  }

  def getActualDateTime(datetime: String,pattern : String = "yyyy-MM-dd HH:mm:ss"): String ={
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(datetime); //定义起始日期
    val tempStart = Calendar.getInstance()
    tempStart.setTime(initData)
    tempStart.add(Calendar.DAY_OF_MONTH,-1)
    tempStart.add(Calendar.HOUR, 11)
    tempStart.add(Calendar.MINUTE, 0)
    tempStart.add(Calendar.SECOND, 0)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(initData)
    tempEnd.add(Calendar.HOUR, 11)
    tempEnd.add(Calendar.MINUTE, 0)
    tempEnd.add(Calendar.SECOND, 0)

    val currentData = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datetime); //定义起始日期
    val tempCurrent = Calendar.getInstance()
    tempCurrent.setTime(currentData)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    if(tempCurrent.before(tempStart)){
      tempStart.add(Calendar.DAY_OF_MONTH, -1)
      dateFormat.format(tempStart.getTime)
    }else if(tempCurrent.after(tempEnd)){
      tempEnd.add(Calendar.DAY_OF_MONTH, 1)
      dateFormat.format(tempEnd.getTime)
    }else{
      dateFormat.format(tempCurrent.getTime)
    }
  }

  def TimeCompare(starTime:String,endTime:String): Boolean = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val star: Date = df.parse(starTime)
    val end: Date = df.parse(endTime)
    if(star.after(end)){
      false
    }else{
      true
    }
  }

  def DateTimeCompare(starTime:String,endTime:String): Boolean = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val star: Date = df.parse(starTime)
    val end: Date = df.parse(endTime)
    if(star.before(end)){
      true
    }else{
      false
    }
  }

  def isActualDate(datetime: String,pattern : String = "yyyy-MM-dd HH:mm:ss"): Boolean ={
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(getrealTime("yyyy-MM-dd")); //定义起始日期
    val tempStart = Calendar.getInstance()
    tempStart.setTime(initData)
    tempStart.add(Calendar.DAY_OF_MONTH,-1)
    tempStart.add(Calendar.HOUR, 12)
    tempStart.add(Calendar.MINUTE, 0)
    tempStart.add(Calendar.SECOND, 0)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(initData)
    tempEnd.add(Calendar.HOUR, 12)
    tempEnd.add(Calendar.MINUTE, 0)
    tempEnd.add(Calendar.SECOND, 0)

    val currentData = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datetime); //定义起始日期
    val tempCurrent = Calendar.getInstance()
    tempCurrent.setTime(currentData)

    if(tempCurrent.before(tempEnd) && tempCurrent.after(tempStart)){
      true
    }else{
      false
    }
  }

  def TimeParse(date:String,pattern:String): Date = {
    val df = new SimpleDateFormat(pattern)
    df.parse(date)
  }

  def getYesterday12(datetime: String,pattern : String = "yyyy-MM-dd"): String ={
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(datetime); //定义起始日期
    val tempYesterday = Calendar.getInstance()
    tempYesterday.setTime(initData)
    tempYesterday.add(Calendar.DAY_OF_MONTH,-1)
    tempYesterday.add(Calendar.HOUR, 12)
    tempYesterday.add(Calendar.MINUTE, 0)
    tempYesterday.add(Calendar.SECOND, 0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(tempYesterday.getTime)
  }

  def getYesterday11(datetime: String,pattern : String = "yyyy-MM-dd"): String ={
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(datetime); //定义起始日期
    val tempYesterday = Calendar.getInstance()
    tempYesterday.setTime(initData)
    tempYesterday.add(Calendar.DAY_OF_MONTH,-1)
    tempYesterday.add(Calendar.HOUR, 11)
    tempYesterday.add(Calendar.MINUTE, 0)
    tempYesterday.add(Calendar.SECOND, 0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(tempYesterday.getTime)
  }

  def getToday12(datetime: String,pattern : String = "yyyy-MM-dd"): String ={
    val initData = new SimpleDateFormat("yyyy-MM-dd").parse(datetime); //定义起始日期
    val tempYesterday = Calendar.getInstance()
    tempYesterday.setTime(initData)
    tempYesterday.add(Calendar.HOUR, 12)
    tempYesterday.add(Calendar.MINUTE, 0)
    tempYesterday.add(Calendar.SECOND, 0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(tempYesterday.getTime)
  }

  /**
    * 获取两个日期之间的日期
    *
    * @param startTime 开始日期
    * @param endTime   结束日期
    * @return 日期集合
    */
  def getBetweenTimes(startTime: String, endTime: String) : List[String] = {
    val startDataTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime); //定义起始时间
    val endDataTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTime); //定义结束时间

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var buffer = new ListBuffer[String]
    val tempStart = Calendar.getInstance()
    tempStart.setTime(startDataTime)
    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endDataTime)
    while (tempStart.before(tempEnd)) {
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.HOUR_OF_DAY, 1)
    }

    if(buffer.length > 1){
      buffer.tail.toList
    }else{
      Nil
    }

  }

  /**
    * 获取传入日期与当前日期的天数差
    *
    * @param start_datetime  end_datetime        待比较日期(仅包含月日)
    * @param pattern 待比较日期格式(例如:MMdd或MM-dd)
    */
  def datetime_between_hour(start_datetime: String, end_datetime: String, pattern: String="yyyy-MM-dd HH:mm:ss"): Int = {
    val startDataTime = new SimpleDateFormat(pattern).parse(start_datetime)
    val endDataTime = new SimpleDateFormat(pattern).parse(end_datetime)

    val between = endDataTime.getTime - startDataTime.getTime
    val hour = between / 1000 / 3600
    hour.toInt
  }
}

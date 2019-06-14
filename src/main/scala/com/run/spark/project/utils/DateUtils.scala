package com.run.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 时间解析工具类
  */
object DateUtils {

  val ORIG_FROMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FROMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time : String): Long ={
    ORIG_FROMAT.parse(time).getTime
  }

  def parseToMinute(time : String): String ={
    TARGET_FROMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    print(parseToMinute("2019-06-10 16:18:01"))
  }

}

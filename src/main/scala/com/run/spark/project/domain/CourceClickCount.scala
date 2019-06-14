package com.run.spark.project.domain

/**
  * 每天 每一门课程 点击数量 domain
  * @param day_course   对应hbase 中的 Rowkey
  * @param click_count  点击数量
  */
case class CourceClickCount (day_course:String,click_count:Long)

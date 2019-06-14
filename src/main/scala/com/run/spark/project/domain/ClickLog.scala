package com.run.spark.project.domain

/**
  * 日志清洗后结果domain
  * @param ip  ip
  * @param time   时间
  * @param courseId   课程表编号
  * @param statusCode   状态码
  * @param referer   引流
  */
case class ClickLog(ip:String,time:String,courseId:Int,statusCode:Int,referer:String)


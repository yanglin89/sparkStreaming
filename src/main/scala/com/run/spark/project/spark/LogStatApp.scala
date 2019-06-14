package com.run.spark.project.spark

import com.run.spark.project.dao.CourseClickCountDao
import com.run.spark.project.domain.{ClickLog, CourceClickCount}
import com.run.spark.project.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 日志分析统计
  * 使用 spark streaming 处理kafka 过来的数据
  * 统计今天开始课程的点击数量
  */
object LogStatApp {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      System.err.print(" KafkaStreamingApp 参数错误")
      System.exit(1)
    }
    val Array(brokerList,topics) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogStatApp")
    val ssc = new StreamingContext(sparkConf,Seconds(30))

    val topicInterable = topics.split(",").toIterable

    val kafkaParmas = Map[String,Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sparkStreaming"

    )

    val message = KafkaUtils.createDirectStream[String,String](ssc,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String,String](topicInterable,kafkaParmas))

    val logs = message.map(_.value())
    val cleanDate = logs.map(line =>{
      // 2.42.84.36	2019-06-10 16:23:01	"GET /learn/1044.html HTTP/1.1"	200	https://www.baidu.com/s?wd=spark sql实战
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)

      var courseId = 0
      if(url.startsWith("/class")){
        val courseIdHtml = url.split("/")(2)
        courseId = courseIdHtml.substring(0,courseIdHtml.lastIndexOf(".")).toInt
      }

      // 填充清洗结果实体类，并且过滤掉课程编号为0 的课程（不是 class）
      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

//    cleanDate.print()
    /**
      * 将清洗结果写入到hbase
      */
      //首先按照 habse 的 rowkey 格式进行 reduceByKey
    val dstream = cleanDate.map(x => {
      (x.time.substring(0,8) + "_" + x.courseId , 1)
    }).reduceByKey(_+_)

    val aa = dstream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecord => {
        val list = new ListBuffer[CourceClickCount]
        partitionRecord.foreach(pair =>{
          list.append(CourceClickCount(pair._1,pair._2))
        })

        // 将每个partition中的数据做一次插入数据库操作
        CourseClickCountDao.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}

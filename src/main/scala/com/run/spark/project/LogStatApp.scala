package com.run.spark.project

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 日志分析统计
  * 使用 spark streaming 处理kafka 过来的数据
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

    val result = message.map(_.value()).count().print()


    ssc.start()
    ssc.awaitTermination()

  }

}

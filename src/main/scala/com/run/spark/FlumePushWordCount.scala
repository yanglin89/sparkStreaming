package com.run.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 整合 flume 采用 push 方式
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("FlumePullWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**
      *  spark streaming 整合 flume
      */
//    val flumeStream = FlumeUtils.createStream(ssc,args(0),args(1).toInt)
    val flumeStream = FlumeUtils.createStream(ssc,"0.0.0.0",41414)

    /**
      * flume 拿到的内容包含 header 和 body ， 我们要用到的是 body 中的内容
      */
    val result = flumeStream.map(x => new String(x.event.getBody.array()).trim)
        .flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

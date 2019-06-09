package com.run.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 整合 flume 采用 pull 方式
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("参数错误")
      System.exit(1)
    }
    val Array(hostname,port) = args

    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("FlumePushWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**
      *  spark streaming 整合 flume
      */
      print(hostname)
      print(port)
    val flumeStream = FlumeUtils.createPollingStream(ssc,hostname,port.toInt)

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

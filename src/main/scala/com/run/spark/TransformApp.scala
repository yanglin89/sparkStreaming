package com.run.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤app
  */
object TransformApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    /**
      * 创建 StreamingContext 需要两个参数，分别是 SparkConf 和 batch interval
      */
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**
      * 构建黑名单
      * 并将黑名单转换为rdd
      * 并将该rdd的每一个记录转换为  （zs,true） 的样式
      */
    val blacks = List("zs","ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x,true))


    val lines = ssc.socketTextStream("192.168.52.138",6789)

    /**
      * 将获取到的结果 lines 中的每一个记录 (20190608,zs) 转换为我们需要的格式 （zs,(20190608,zs)）
      * 并通过 transform 算子与 黑名单的 rdd 左连接，删选出其中不为true 的记录
      * 最初只取出满足条件的原始数据 (20190608,zs) 类型
      */
    val result = lines.map(x => (x.split(",")(1),x))
      .transform(rdd =>{
        rdd.leftOuterJoin(blacksRDD)
          .filter(x => x._2._2.getOrElse(false) != true)
          .map(x => x._2._1)
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

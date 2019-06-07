package com.run.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用 spark streaming 完成 有状态的统计（从最开始到现在为止计数）
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**
      * 如果使用了stateful的算子，必须要设置checkpoint的存放路径
      * 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
      */
    ssc.checkpoint("E:/study_data/sparkstreaming/checkpoint")

    val lines = ssc.socketTextStream("192.168.52.138",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues
    * @param preValues
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] ={
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

}



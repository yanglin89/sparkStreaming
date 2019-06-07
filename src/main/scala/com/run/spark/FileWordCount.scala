package com.run.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用 spark streaming 处理文件系统（local/hdfs）的数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    // 处理文件系统的时候，不需要receiver 占用一个资源，可以使用local 或者local[1] 都可以
    val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.textFileStream("file:///E:/study_data/sparkstreaming/")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

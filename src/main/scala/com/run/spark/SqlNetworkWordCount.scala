package com.run.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * spark streaming 整合 spark sql 完成词频统计
  */
object SqlNetworkWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SqlNetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.socketTextStream("master",6789)
    val words = lines.flatMap(_.split(" "))

    /**
      * Convert RDDs of the words DStream to DataFrame and run SQL query
      * 首先通过streaming 获取到 dstream ，通过 foreachRDD 将 dstream 转换为一个 rdd
      * 然后将 rdd 利用 case class 转换为 dataframe
      * 然后可以利用得到的 dataframe 直接利用sql 的方式进行处理得到结果
      */
    words.foreachRDD((rdd:RDD[String],time:Time) => {
      /**
        * 通过单例模式创建一个 sparkSession
        */
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      // 导入隐式转换
      import spark.implicits._
      /**
        * 将 RDD[String] 转换为 RDD[case class] 转换为 DataFrame
        */
      val wordsDataFrame = rdd.map(x => Record(x)).toDF()

      wordsDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select word,count(1) from words group by word")

      println(s"========= $time =========")
      wordCountsDataFrame.show()

    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * sparkSession 单例模式
    */
  object SparkSessionSingleton{

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf) : SparkSession = {
      if(instance == null){
        instance = SparkSession.builder().config(sparkConf).getOrCreate()
      }
      instance
    }
  }

  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)

}

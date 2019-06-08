package com.run.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用 spark streaming 完成 词频2统计
  * 并将结果写入到 MYSQL 数据库中
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**
      * 如果使用了stateful的算子，必须要设置checkpoint的存放路径
      * 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
      */
//    ssc.checkpoint("E:/study_data/sparkstreaming/checkpoint")

    val lines = ssc.socketTextStream("192.168.52.138",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    // 因为有数据库支撑，不需要每次都计算全部的值
//    val state = result.updateStateByKey[Int](updateFunction _)
//    state.print()

    /**
      * 将结果写入到 mysql 数据库中，使用foreachRDD
      * 分别采用两种方式，第一种为子数据库连接池，第二种为直接创建数据库连接
      */
    // 方式一： 数据库连接池
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partition =>{
        val conn = ConnectionPool.getConnection()
        partition.foreach(record => {
          val sql = "insert into wordcount_key(word, wordcount) values('" + record._1 + "', " + record._2 + ")"
          conn.createStatement().execute(sql)
          println(record._1)
        })
        ConnectionPool.closeConnection(conn)
      })
    })

    // 方式二： 数据库直接连接
    /*result.foreachRDD(rdd => {
      rdd.foreachPartition(partition =>{
        val conn = createConnection()
        partition.foreach(record => {
          val sql = "insert into wordcount_key(word, wordcount) values('" + record._1 + "', " + record._2 + ")"
          conn.createStatement().execute(sql)
          println(record._1)
        })
        conn.close()
      })
    })*/

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 创建 mysql 数据库连接
    */
  def createConnection(): Connection ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://master:3306/sparkstreaming_project","hadoop","mdhc5891")
  }


}



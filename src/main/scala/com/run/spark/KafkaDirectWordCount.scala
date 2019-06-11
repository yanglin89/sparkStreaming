package com.run.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用 receiver方式进行 kafka 和 streaming 的整合,在 kafka 0.10版本之上，这种receiver的方式已经不支持了
  * 在 kafka 0.10 版本之后，推荐使用 direct 方式和sparkStreaming 对接
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      System.err.print(" KafkaDirectWordCount 参数错误")
      System.exit(1)
    }
    val Array(brokerList,topics) = args

    val sparkConf = new SparkConf()
      //.setMaster("local[2]").setAppName("KafkaReceiverWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaParmas = Map[String,Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test"
    )

    val topicsIterable = topics.split(",").toIterable

    /**
      * LocationStrategies: 位置策略
      * 在大多数情况下，您应该使用LocationStrategies.PreferConsistent，如上所示。
      * 这会将分区平均分配给可用的执行者。如果您的执行者（executors ）与您的Kafka经纪人（brokers）位于同一个主机上，
      *     请使用PreferBrokers，它将优先为该分区的Kafka领导安排分区。
      * 最后，如果分区间负载存在明显偏移，请使用PreferFixed。这允许您指定分区到主机的明确映射（
      *     任何未指定的分区将使用一致的位置）
      */

    /**
      * ConsumerStrategies : 消费者策略
      * 通过 Subscribe 来指定
      */
    val message = KafkaUtils.createDirectStream[String,String](ssc,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String,String](topicsIterable,kafkaParmas))

    /**
      * sparkstreaming 和 kafka 对接的过程中，目前使用kafka 版本为 2.2.1
      * 在kafka 之前的版本中，header 和 value 通过 _1  _2 来获取，当前版本中通过 .header() 和 .value() 来获取
      * sparkstreaming 从kafka 中获取的到数据有效的为 value ，所以我们要拿 value 来进行处理
      */
    val result = message.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

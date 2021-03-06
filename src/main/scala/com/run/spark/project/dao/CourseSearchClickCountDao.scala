package com.run.spark.project.dao

import com.run.spark.kafka.project.utils.HbaseUtils
import com.run.spark.project.domain.{CourceClickCount, CourseSearchClickCount}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 针对搜索引擎过来的课程，每天 每一门课程 点击数量 dao 层
  */
object CourseSearchClickCountDao {

  // hbase 表名
  val table_name = "run_course_search_clickcount"
  //hbase cf ，每一个cf中可以存放多个列
  val cf = "info"
  // hbase 列名 点击数量
  val qualifer = "click_count"

  /**
    *  批量保存 数据到 hbase
    * @param list
    */
  def save(list:ListBuffer[CourseSearchClickCount]): Unit ={

    val table = HbaseUtils.getInstance().getHtable(table_name)

    /**
      *  通过 incrementColumnValue() 可以直接将之前已经存在的rowkey 和 后续新添加的相同的 rowkey 的值相加
      *  从而不需要我们先获取之前的 rowkey 对应的值，然后再做运算进行添加
      */
    for(ele <- list){
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count
      )
    }
  }


  /**
    * 根据 rowkey 查询 点击数量
    * @param day_search_course
    * @return
    */
  def count(day_search_course:String):Long ={

    val table = HbaseUtils.getInstance().getHtable(table_name)

    val get = new Get(day_search_course.getBytes)
    val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

    /**
      * scala 中 == 和 equals 没有区别
      */
    if (value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20181111_www.baidu.com_8",8))
    list.append(CourseSearchClickCount("20181111_www.baidu.com_9",9))
    list.append(CourseSearchClickCount("20190523_cn.bing.com_6",666))

    save(list)

    print(count("20181111_www.baidu.com_8") + ":" + count("20181111_www.baidu.com_9") + ":" + count("20190523_cn.bing.com_6"))
  }

}

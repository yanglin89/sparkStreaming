package com.run.spark.kafka.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase 操作工具类
 * 建议采用 单例模式
 */
public class HbaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;


    /**
     * 单例模式，构造方法为私有
     */
    private HbaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","master:2181");
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtils instance = null;

    /**
     * 单例模式创建 HbaseUtils
     * @return
     */
    public static synchronized HbaseUtils getInstance(){
        if (null == instance){
            return new HbaseUtils();
        }
        return instance;
    }


    /**
     *  根据表名获取 htable实例（相当于hbase 的一个表）
     * @param tableName
     * @return
     */
    public HTable getHtable(String tableName){
        HTable table = null;

        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }


    /**
     *  插入单条数据到 hbase
     * @param tableName 表名
     * @param rowkey rowkey
     * @param cf  hbase表的columnfamily
     * @param column  列名
     * @param value  插入的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){

        HTable table = getHtable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
//        HTable table = HbaseUtils.getInstance().getHtable("run_course_clickcount");
//        System.out.println(table.getName().getNameAsString());

        String tableName = "run_course_clickcount";
        String rowkey = "20181111_88";
        String cf = "info";
        String column = "click_count";
        String value = "123";
        HbaseUtils.getInstance().put(tableName,rowkey,cf,column,value);
    }

}























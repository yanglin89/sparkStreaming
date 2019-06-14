package com.run.spark.kafka.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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


    /**
     * 根据表名和查询条件作为前缀条件，获取Hbase 的记录数
     * @param tableName
     * @param option 查询条件
     * @param cf
     * @param qualifier
     * @return
     */
    public Map<String,Long> query(String tableName,String option,String cf,String qualifier){

        Map<String,Long> map = new HashMap<String, Long>();

        HTable table = getHtable(tableName);

        Scan scan = new Scan();
        // 将查询条件作为前缀
        Filter filter = new PrefixFilter(Bytes.toBytes(option));
        scan.setFilter(filter);
        try {
            System.out.println(" 开始查询数据 from hbase");
            ResultScanner rs = table.getScanner(scan); // 查询到结果rs
            System.out.println(" 查询数据结束 from hbase");
            for(Result result : rs){
                // 获取到 rowkey
                String rowkey = Bytes.toString(result.getRow());
                // 获取到 点击数量 clickCount
                long clickCount = Bytes.toLong(result.getValue(cf.getBytes(),qualifier.getBytes()));

                map.put(rowkey,clickCount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return map;
    }


    public static void main(String[] args) {
        /*HTable table = HbaseUtils.getInstance().getHtable("run_course_clickcount");
        System.out.println(table.getName().getNameAsString());*/

        /*String tableName = "run_course_clickcount";
        String rowkey = "20181111_88";
        String cf = "info";
        String column = "click_count";
        String value = "123";
        HbaseUtils.getInstance().put(tableName,rowkey,cf,column,value);*/

        String tableName = "run_course_clickcount";
        String option = "20190610";
        String cf = "info";
        String column = "click_count";
        Map<String,Long> result = HbaseUtils.getInstance().query(tableName,option,cf,column);
        for(Map.Entry<String,Long> entry : result.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

    }

}























package com.run.spark

import java.sql.Connection
import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * 数据库连接池
  */
object ConnectionPool {

  val dataSource = new ComboPooledDataSource()

  dataSource.setJdbcUrl("jdbc:mysql://master:3306/sparkstreaming_project");//设置url
  dataSource.setDriverClass("com.mysql.jdbc.Driver");//设置驱动
  dataSource.setUser("hadoop");//mysql的账号
  dataSource.setPassword("mdhc5891");//mysql的密码
  dataSource.setInitialPoolSize(6);//初始连接数，即初始化6个连接
  dataSource.setMaxPoolSize(50);//最大连接数，即最大的连接数是50
  dataSource.setMaxIdleTime(60);//最大空闲时间

  // 获取数据库连接
  def getConnection(): Connection ={
    dataSource.getConnection();
  }

  // 释放数据库连接
  def closeConnection(connection:Connection): Unit = {
    if(!connection.isClosed) {
      connection.close()
    }
  }

}

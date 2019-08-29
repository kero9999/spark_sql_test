package com.kero99.wp

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import java.util.Date
/**
  *
  * @author wp
  * @date 2019-08-29 15:01
  *
  */
object HiveDemo extends WpSession {

  import session.implicits._
  import session.sql
//  HiveUtil.init(session)
  HiveUtil.ETLData(session)
}



object HiveUtil{
  def init(session:SparkSession)={
    import session.implicits._
    import session.sql
    //初始化数据库
    sql("drop database if exists spark")
    sql("create database if not exists spark")
    sql("use spark")
    //创建表
    sql("drop table if exists tb_emp")
    sql("create external table if not exists tb_emp(empno int,ename string,job string,sal double,comm double,deptno int) " +
      "partitioned by (hdate int,hday int) " +
      "row format delimited fields terminated by ',' " +
      "location 'hdfs://apache01:9000/user/hive/warehouse/spark.db/'")

  }

  def loadData(session:SparkSession)={
    import session.implicits._
    import session.sql
    var date = new Date()
    var dateFormat = new SimpleDateFormat("yyyyMM,dd")
    var dateStr=dateFormat.format(date).split(",")
    sql(s"load data inpath 'hdfs://apache01:9000/spark_data/tb_emp.txt' into table tb_emp partition(hdate=${dateStr(0)},hday=${dateStr(1)})")
  }

  def ETLData(session:SparkSession)={
    import session.implicits._
    import session.sql
    val tb_emp_df=sql(s"select max(sal) maxsal,min(sal) as minsal,job,${System.currentTimeMillis()} htime from spark.tb_emp group by job")
    tb_emp_df.write.saveAsTable("spark.tb_emp_info")
  }
}

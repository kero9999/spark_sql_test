package com.kero99.wp

import java.util.Properties

/**
  *
  * @author wp
  * @date 2019-08-27 14:07
  *
  */
object MySQLTest extends WpSession {
//  val url = "jdbc:mysql://192.168.52.42:3306/d1903"
//  val table = "ob_emp"
//  val pro = new Properties()
//  pro.setProperty("user","root")
//  pro.setProperty("password","root")
//
//  val df = session.read.jdbc(url,table,pro)
//  df.show


//  val df = session.read.format("jdbc").options(
//    Map(
//      "url"->"jdbc:mysql://192.168.52.42:3306/d1903",
//      "user"->"root",
//      "password"->"root",
//      "dbtable"->"ob_emp"
//    )).load()
  val df = session.read.format("jdbc").options(
    Map(
      "driver"->"org.apache.hive.jdbc.HiveDriver",
      "url"->"jdbc:hive2://192.168.1.132:10000/yzx",
      "user"->"hdfs",
      "password"->"hdfs",
      "dbtable"->"ob_emp"
    )).load()
  df.select("*").show


}

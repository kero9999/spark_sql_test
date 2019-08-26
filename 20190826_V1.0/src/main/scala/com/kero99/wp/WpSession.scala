package com.kero99.wp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  *
  * @author wp
  * @date 2019-08-26 15:42
  *
  */
class WpSession extends App {
  val session = SparkSession.builder().appName(getClass.getSimpleName)
    .master("local[*]")
    .getOrCreate()
  val sc = session.sparkContext
  sc.setLogLevel("WARN")
}

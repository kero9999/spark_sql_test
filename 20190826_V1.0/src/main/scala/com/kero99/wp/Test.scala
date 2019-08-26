package com.kero99.wp

import org.apache.spark.sql.SparkSession

/**
  *
  * @author wp
  * @date 2019-08-26 15:33
  *
  */
object Test extends App {
  val session = SparkSession.builder().
    master("local").appName("test").getOrCreate()
  session.sparkContext.setLogLevel("WARN")

  val r1 = session.sparkContext.parallelize(1 to 10 )
  r1.foreach(println)


  session.stop()
}

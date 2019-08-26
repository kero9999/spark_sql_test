package com.kero99.wp

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author wp
  * @date 2019-08-26 16:11
  *
  */
object DataFrameTest02 extends WpSession {
  val r1=sc.parallelize(List("zhangsan"->18,"wangwu"->22,"lisi"->33))
  val df =session.createDataFrame(r1).toDF("name","age")
  df.show
}

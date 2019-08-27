package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-27 14:02
  *
  */
object DataFrameTest06 extends WpSession {
  val r1 = sc.parallelize(List("zhangsan"->18,"lisi"->22))
  import session.implicits._
  val df =r1.toDF("name","age")

  df.show
}

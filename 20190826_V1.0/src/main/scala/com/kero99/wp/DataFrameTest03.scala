package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-26 16:17
  *
  */
object DataFrameTest03 extends WpSession {

  val r1 = sc.textFile(System.getProperty("user.dir")+"/input").map(f=>{
    var strs = f.split(",")
    (strs(0),strs(1).trim.toInt)
  })
  val df= session.createDataFrame(r1).toDF("name","age")

  df.show
}

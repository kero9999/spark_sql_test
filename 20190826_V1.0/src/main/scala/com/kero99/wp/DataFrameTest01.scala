package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-26 15:41
  * RDD --> DataFrame
  * 
  * 
  * 表
  * 
  * List<类>
  */

case class TbUsers (id:Long,account:String,age:Int)


object DataFrameTest01 extends WpSession {
  
  val r1 = sc.parallelize(List(TbUsers(1001L,"zhangsan",18),TbUsers(1002L,"lisi",22),TbUsers(1003L,"wangwu",44)))

  val df = session.createDataFrame(r1)
  df.createTempView("tb_users")
  session.sql("select * from tb_users where age>20").show()
}

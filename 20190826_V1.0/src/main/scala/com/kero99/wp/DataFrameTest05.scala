package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-26 16:46
  *
  */
object DataFrameTest05 extends WpSession {
  session.read.json(System.getProperty("user.dir")+"/jsoninput").createTempView("tb_users")



  session.sql("select id,name,age from tb_users").show

}

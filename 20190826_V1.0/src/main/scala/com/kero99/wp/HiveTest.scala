package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-27 14:54
  *
  */
object HiveTest extends WpSession {
  session.sql("select * from yzx.ob_emp").show
}

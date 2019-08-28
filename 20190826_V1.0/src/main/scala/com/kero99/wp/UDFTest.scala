package com.kero99.wp

import com.kero99.wp.DataFrameTest07.sc

/**
  *
  * @author wp
  * @date 2019-08-28 16:29
  *
  */
object UDFTest extends WpSession {
  import session.implicits._
  val df1=sc.parallelize(List(
    (1001,"zhangsan","普通员工",1200,10),
    (1002,"lisi","普通员工",2200,20),
    (1003,"zhanghan","普通员工",1300,20),
    (1004,"xuan","部门经理",4400,30),
    (1005,"wangyan","普通员工",1500,30),
    (1006,"tianqi","部门经理",6600,20),
    (1007,"maba","部门经理",8800,10),
    (1008,"dazhezi","CEO",9900,10)
  )).toDF("empno","ename","job","sal","dno")

  df1.createTempView("tb_emp")

  session.udf.register("wplen",(f:String)=>{f+":"+f.length})
  session.sql("select ename,wplen(ename) from tb_emp").show













  val df2=sc.parallelize(List(
    (10,"总裁办","北京"),
    (20,"市场部","上海"),
    (30,"研发部","广州"),
    (40,"销售部","深圳")
  )).toDF("deptno","ename","location")

  val df3=sc.parallelize(List(
    (0,2000,"C"),
    (2001,4000,"B"),
    (4001,6000,"A"),
    (6001,Integer.MAX_VALUE,"SSR")
  )).toDF("sallow","salhigh","sallevel")
}

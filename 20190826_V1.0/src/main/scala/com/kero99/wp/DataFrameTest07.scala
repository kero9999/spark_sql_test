package com.kero99.wp
import org.apache.spark.sql.Column
/**
  *
  * @author wp
1  * @date 2019-08-28 14:03
  *
  */
object DataFrameTest07 extends WpSession {
  import session.implicits._
//  val df = sc.parallelize(List("zhangsan"->18,"lisi"->19,"wangwu"->18,"zhangsan"->22,"wangwu"->18)).toDF("name","age")
//分组求总数
//  df.groupBy("age").count().show
//数据魔方
//  df.cube("name","age").count().show
  //df.rollup("name","age").count().show
//整条数据去重
//  df.distinct().show
  //按指定字段分组
//  df.dropDuplicates("name").show
  //分组聚合
  //select count(name),sum(age) from xxx group by name
//  df.groupBy("name").agg("name"->"count","age"->"sum").show


//  val df1 = sc.parallelize(List("张三"->18,"李四"->13)).toDF("name","age")
//  val col = new Column("tmp").isNull
//
//  val df2 = sc.parallelize(List(
//    ("zhaoliu","男",18),
//    ("tianqi","女",22)
//  )).toDF("account","sex","age")
//
//  df2.union(df1.withColumn("tmp",new Column("tmp"))).show




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

//  两表同字段JOIN
//  df1.join(df2,"deptno").show()
//  右连接
//  df1.join(df2,Seq("deptno"),"right").show()




//多表JOIN,多类型条件。
  df1.join(df2,df1("dno")===df2("deptno")).join(df3,df1("sal")>df3("sallow")&&df1("sal")<df3("salhigh")).select(df1("ename"),df1("sal"),df2("ename"),df3("sallevel")).show


//  df1.createTempView("tb_emp")
//  df2.createTempView("tb_dept")
//  df3.createTempView("tb_salgrade")
//
//  session.sql("select e.ename,e.sal,d.ename,g.sallevel from tb_emp e join tb_dept d on e.dno=d.deptno join tb_salgrade g on e.sal between g.sallow and g.salhigh ").show





//  df1.stat.freqItems(Array("dno"),0.30).show



  //df1.intersect(df1.limit(3)).show


//  val df4=sc.parallelize(List(
//    "admin,aaa","root,rttt,rrr","zhangsan,lisi,wangwu"
//  )).toDF("word")
//
//
//
//  df4.explode("word","newword") { f:String =>f.split(",")}.show




















}

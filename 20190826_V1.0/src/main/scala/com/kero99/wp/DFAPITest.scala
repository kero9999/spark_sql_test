package com.kero99.wp

import java.util.Properties

/**
  *
  * @author wp
  * @date 2019-08-27 15:21
  *
  */
object DFAPITest extends WpSession {
  val pro = new Properties
  pro.setProperty("user","root")
  pro.setProperty("password","root")
  val df = session.read.jdbc("jdbc:mysql://192.168.52.42/d1903","ob_emp",pro)

  //df.show(5,false)
//  val col = df.collect()
//  var isHead = true
//  col.foreach(f=>{
//    if(isHead){
//      //获取表头
//      f.schema.foreach(st=>{
//        print(st.name+":"+st.dataType.typeName+"\t")
//      })
//      println
//      isHead=false
//    }
//    //获取表中数据
//    for(i <- 0 until f.size){
//      print(f.get(i)+"\t")
//    }
//    println
//  })
    //df.describe("ename","sal","comm").show()
//  val f = df.first()
//  println(f.mkString(","))
//  df.where("sal>3000 and sal<5000").show()
//  df.where("sal between 3000 and 5000").show()
//  df.filter(f=>{
//    val value = f.getDouble(f.fieldIndex("sal"))
//    println(value)
//    if(value>3000&&value<5000){
//      true
//    }else{
//      false
//    }
//  }).select("ename","sal","job").show


//  val tmp = df("ename")
//  println(tmp)
  //select ename,sal,sal+100 from
  //df.select(df("ename"),df("sal"),df("sal")+100).show

//  df.selectExpr("empno","ename as uname","substr(job,1,-1)").show
  //df.drop("comm","deptno","hdate").show
//  df.limit(5).show()


  //df.orderBy("sal").show()
  //df.orderBy(df("sal").desc).show()
//  df.orderBy(df("deptno"),-df("sal")).show()
}

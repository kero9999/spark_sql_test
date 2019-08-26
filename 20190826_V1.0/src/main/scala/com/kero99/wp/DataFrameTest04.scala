package com.kero99.wp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
/**
  *
  * @author wp
  * @date 2019-08-26 16:17
  *
  */
object DataFrameTest04 extends WpSession {

  val r1 = sc.textFile(System.getProperty("user.dir")+"/input").map(f=>{
    var strs = f.split(",")
    var age = strs(1).trim.toInt
    if(strs.length>2){
      Row(strs(0),age,strs(2))
    }else{
      Row(strs(0),age,null)

    }
  })
  val rowStruct = new StructType(Array(
    StructField("name",StringType,true),
    StructField("age",IntegerType,true),
    StructField("sex",StringType,true)
  ))

  val df = session.createDataFrame(r1,rowStruct)

  df.show
}

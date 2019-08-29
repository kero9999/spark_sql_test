package com.kero99.wp

import com.kero99.wp.StringTest.{is, str}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  *
  * @author wp
  * @date 2019-08-28 16:36
  *
  */
object UDAFTest extends WpSession {







  import session.implicits._
  sc.parallelize(List("zhangsan"->18,"lisi"->22,"wangwuwu"->33)).toDF("name","age").createTempView("tb_users")

  session.udf.register("wu",new WpUDAF)

  session.sql("select wu(name) from tb_users").show


}
class WpUDAF extends UserDefinedAggregateFunction{
  //定义输入类型
  override def inputSchema: StructType = StructType(Array(
    StructField("name",StringType)
  ))
  //定义中间缓存类型
  override def bufferSchema: StructType = StructType(Array(
    StructField("ch",StringType),StructField("num",IntegerType)
  ))
  //定义输出类型
  override def dataType: DataType = StringType
  //输入和输出是否同类型
  override def deterministic: Boolean = true
  //初始化中间缓存对象
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=""
    buffer(1)=0
  }
  //分区内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var name = input.getString(0)
    var is=new Array[Int](26)
    name.foreach(f=>{
      var index = f.toInt-97
      is(index)+=1
    })
    var maxC="a"
    var maxCount=is(0)
    for(i <- 1 until is.length){
      if(is(i)>maxCount){
        maxCount=is(i)
        maxC=(i+97).toChar.toString
      }
    }
    if(buffer.getInt(1)<maxCount){
      buffer(0)=maxC
      buffer(1)=maxCount
    }
  }
  //分区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val c1 = buffer1.getInt(1)
    val c2 = buffer2.getInt(1)
    if(c2>c1){
      buffer1(0)=buffer2(0)
      buffer1(1)=buffer2(1)
    }
  }
  //结果
  override def evaluate(buffer: Row): Any =
    buffer.getString(0)+":"+buffer.getInt(1)
}

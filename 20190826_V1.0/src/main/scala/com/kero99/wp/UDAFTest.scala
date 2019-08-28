package com.kero99.wp

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







//  import session.implicits._
//  sc.parallelize(List("zhangsan"->18,"lisi"->22,"wangwuwu"->33)).toDF("name","age").createTempView("tb_users")
//  session.sql("select wpudaf(name) from tb_users").show


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


  }
  //分区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???
  //结果
  override def evaluate(buffer: Row): Any = ???
}

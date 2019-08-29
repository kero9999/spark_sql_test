package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-29 14:05
  *
  */
object StringTest extends App {
  val str = "wangwuwu"
  val is = new Array[Int](26)
  str.foreach(f=>{
    var index = f.toInt-97
    is(index)+=1
  })
  var maxC='a'
  var maxCount=is(0)
  for(i <- 1 until is.length){
    if(is(i)>maxCount){
      maxCount=is(i)
      maxC=(i+97).toChar
    }
  }
  println(maxC+":"+maxCount)

}

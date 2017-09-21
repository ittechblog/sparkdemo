package com.yang

/**
  * Created by yangyang on 2017/8/27.
  */
object  MyEnum extends Enumeration{

  type myType =Value
  val E1,E2 = Value

  def main(args: Array[String]): Unit = {
    val myEnum = MyEnum(1)
    println(myEnum,myEnum.id)
  }
}

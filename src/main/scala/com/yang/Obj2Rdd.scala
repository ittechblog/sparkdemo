package com.yang

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

case class Person(val name:String,val age:Int)
/**
  * Created by yangyang on 2017/8/18.
  */
object Obj2Rdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    val person = Person("yang",25)
    val rdd = sparkContext.makeRDD(Array(person))
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd.filter(p=>{
      p.name.equals("yan")
    }).collect().foreach(p=>{
      println(p.name+"----"+p.age)
    })
    println("rdd.count()----"+rdd.count())
    rdd.take(1).foreach(p=>{
      println(p.name+"----"+p.age)
    })
    sparkContext.stop()
  }
}

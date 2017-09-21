package com.yang

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangyang on 2017/2/16.
  */
object WorldCount {

  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkConf = new SparkConf().setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
//    val lines = sparkContext.textFile("C:\\Users\\yangyang\\Desktop\\Client.java")
    val lines = sparkContext.textFile("/tmp/eday.csv")
    val counts = lines.flatMap(_.split(",")).map(s => (s,1)).reduceByKey((a,b) => a+b)
    counts.collect().foreach(println(_))
    sparkContext.stop()
  }
}

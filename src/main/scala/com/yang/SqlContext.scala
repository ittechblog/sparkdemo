package com.yang

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import scala.collection.JavaConversions._

/**
  * Created by yangyang on 2017/8/17.
  */
object SqlContext {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val properties = new Properties()
    properties.put("user", "netty_user")
    properties.put("password", "yingzhen123")
    val dataFrame = sqlContext.read.jdbc("jdbc:mysql://192.168.1.44:3306/test?useUnicode=true&amp;characterEncoding=UTF-8", "partition_test", properties)

    dataFrame.collect().foreach(r => {
      println(r.getInt(0) + "------" + r.getInt(1))
    })

    val dataList = dataFrame.collectAsList()
    for (data <- dataList) {
      println(data.getInt(0) + "------" + data.getInt(1))
    }
  }
}

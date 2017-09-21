package com.yang

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangyang on 2017/2/16.
  */
object SocketStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("SocketStream")
    val sparkContext = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = ssc.socketTextStream("192.168.1.45",8802)
    // 简单地打印每一批的几个元素
    // 批量运行
    val events = stream.map{
      record =>
        val event = record.split(",")
        (event(0),event(1),event(2))
    }
    events.foreachRDD{
      rdd =>
        val numPurchases = rdd.count()
        val uniqueUsers = rdd.map{
          case(user, _, _) => user
        }.distinct.count
        val totalRevenue = rdd.map{
          case(_, _, price) => price.toDouble
        }.sum()
        val productsByPopularity = rdd.map{
          case(user, product, price) => (product,1)
        }.reduceByKey(_ + _).collect().sortBy(-_._2)

        val popular = productsByPopularity(0)
        println(s"Total purchases : ${numPurchases}")
        println(s"uniqueUsers     : ${uniqueUsers}")
        println(s"Total Revenue   : ${totalRevenue}")
        println(s"mostPopluar     : ${popular._1}")
    }

    // stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

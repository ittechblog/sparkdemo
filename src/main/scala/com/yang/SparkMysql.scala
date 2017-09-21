package com.yang

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangyang on 2017/8/17.
  */
object SparkMysql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.1.44:3306/test?useUnicode=true&amp;characterEncoding=UTF-8", "netty_user", "yingzhen123")
    }

    val jdbcRdd = new JdbcRDD(
      sparkContext,
      connection,
      "select * from partition_test where id >=? and id <=?",
      1,
      100,
      1,
      r => {
        val id = r.getInt(1)
        val num = r.getInt(2)
        (id, num)
      }
    )
    jdbcRdd.collect().map(record => {
      println(record._1+"---------"+record._2)
    }
    )
//    sparkContext.stop()
  }
}

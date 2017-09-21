package com.yang

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangyang on 2017/8/15.
  */
object Mysql extends Serializable{

  def main(args: Array[String]): Unit = {
    val a = "title"
    println(s"$a")
//    val sparkConf = new SparkConf().setMaster("local").setAppName("Mysql")
    val sparkConf = new SparkConf().setAppName("Mysql")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1 to 30)

    rdd.repartition(3).foreachPartition(it => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      val connection = DriverManager.getConnection("jdbc:mysql://192.168.1.44:3306/test?useUnicode=true&amp;characterEncoding=UTF-8", "netty_user", "yingzhen123")
      it.foreach(t => {
        val ps = connection.prepareStatement("insert into partition_test(num) values(?)")
        println(t.toString())
        ps.setString(1,t.toString())
        ps.executeUpdate()
        ps.close()
      }
      )
//      it.foreach(t => {
//        val ps = connection.prepareStatement("insert into partition_test(num) values(?)")
//        println(t.toString())
//        ps.setString(1,t.toString())
//        ps.executeUpdate()
//        ps.close()
//      }
//      )
      connection.close()
    }
    )


  }
}

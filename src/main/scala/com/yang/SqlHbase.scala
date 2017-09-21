package com.yang

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by yangyang on 2017/8/17.
  */
object SqlHbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "test40:2181,test41:2181,test42:2181")
    val hbaseContext = new HBaseContext(sparkContext, conf)

  }
}

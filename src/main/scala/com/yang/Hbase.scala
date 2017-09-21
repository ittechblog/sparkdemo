package com.yang

import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangyang on 2017/8/15.
  */
object Hbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "test40:2181,test41:2181,test42:2181")
    val hbaseContext = new HBaseContext(sparkContext, conf)
    val rdd = sparkContext.parallelize(Array((Bytes.toBytes("100000596_656005413_02"))))
    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      TableName.valueOf("common_data_history"),
      100,
      rdd,
      record => {
        new Get(record)
      },
      (result: Result) => {
        val it = result.list().iterator()
        val b = new StringBuilder
        while (it.hasNext()) {
          val kv = it.next()
          val q = Bytes.toString(kv.getQualifier())
          if (q.equals("counter")) {
            b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toLong(kv.getValue()) + ")")
          } else {
            b.append(Bytes.toString(kv.getValue()))
          }
        }
        b.toString
      })
    getRdd.collect.foreach(v => println(v))

    val insertRdd = sparkContext.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("1")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("2")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("3")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("4")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("5"))))
    )
    )
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      insertRdd,
      TableName.valueOf("common_data_history"),
      (putRecord)=>{
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
        put
      }
    )

    sparkContext.stop()
  }

}

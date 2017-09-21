package com.yang

import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.rdd.{RedisKVRDD, RedisKeysRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by yangyang on 2017/8/15.
  */
object Redis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
      // initial redis host - can be any node in cluster mode
      .set("redis.host", "192.168.1.44")
      // initial redis port
      .set("redis.port", "6381")
      // optional redis AUTH password
      .set("redis.auth", "1234")
    val sparkContext = new SparkContext(sparkConf)

//    val keyRdd = sparkContext.parallelize(Array("test"))
    val rdd = sparkContext.parallelize(1 to 10).map(i => (i.toString, i.toString))
    sparkContext.toRedisKV(rdd)

    sparkContext.toRedisHASH(rdd,"my")

    val connectVal = sparkContext.fromRedisKV("connect_test")
    connectVal.collect().foreach(v => println(v._2))

    val hashVal = sparkContext.fromRedisHash(Array("dl:1234","_et"))
    hashVal.collect().foreach(v=>println(v._2))

    sparkContext.stop()
  }

}

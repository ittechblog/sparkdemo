package com.yang

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangyang on 2017/8/15.
  */
object KafkaComsumer extends Serializable{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.1.60:9092,192.168.1.61:9092,192.168.1.62:9092", "group.id" -> "111my-consumer-group2",
      "auto.offset.reset" -> "largest", "zookeeper.connect" -> "192.168.1.60:2181,192.168.1.61:2181,192.168.1.62:2181")

    val topic = "deviceAlarm1"
    val topicSet = topic.split(",").toSet
    @transient val kafkaManager = new KafkaManager(kafkaParams)
    val directStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicSet)

    directStream.foreachRDD(rdd=>{
      if(rdd.isEmpty()){
        println("------------")
      }else{
        rdd.collect().foreach(record=>{
          println("-------------"+record._2)
        })
        /*rdd.foreachPartition(it=>{
          it.foreach(record=>{
            println("**********************")
            println("-------------"+record._2)
          })
        })*/
        // update zk offsets
        val zkOffsetsUpdateExecutor = kafkaManager.updateZKOffsets(rdd)
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }

}

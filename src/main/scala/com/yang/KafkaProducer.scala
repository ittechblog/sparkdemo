package com.yang

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by yangyang on 2017/8/16.
  */
object KafkaProducer extends Serializable {

  def main(args: Array[String]): Unit = {


    val kafkaProducer = new KafkaProducer[String, String](producerProperties())
    kafkaProducer.send(new ProducerRecord[String, String]("deviceAlarm1", "1111111111111"))
//    kafkaProducer.flush()

    /*val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.parallelize(Array(1, 2, 3))

    rdd.repartition(3).foreachPartition(it => {
      val kafkaProducer = new KafkaProducer[String, String](producerProperties())
      it.foreach(t => {
        val value = t
        println(t)
        kafkaProducer.send(new ProducerRecord[String, String]("deviceAlarm1", value + ""))
        kafkaProducer.flush()
      }
      )
    }
    )*/

  }

  def producerProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.1.60:9092,192.168.1.61:9092,192.168.1.62:9092")
    //    properties.put("kafka.group", "my-consumer-group1")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    /*    properties.put("acks","")
        properties.put("retries","")
        properties.put("batch.size","")
        properties.put("auto.commit.interval.ms","")
        properties.put("linger.ms","")*/

    properties
  }
}

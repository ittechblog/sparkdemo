package com.yang

import com.yang.KafkaCluster.{Err, LeaderOffset}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

import scala.reflect.ClassTag

class KafkaManager(kafkaParams: Map[String, String]) extends Serializable {
  private val cluster = new KafkaCluster(kafkaParams)

  /**
    * create DStream
    *
    * @param streamingContext spark streaming context
    * @param kafkaParams      kafka parameters
    * @param topics           topics to consumer for the consumers
    * @tparam K  message key class
    * @tparam V  message value class
    * @tparam KD message key decoder class
    * @tparam VD message value decoder class
    * @return InputDStream
    */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](streamingContext: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams("group.id")

    setOrUpdateOffsets(topics, groupId)

    val messages = {
      val partitionE = cluster.getPartitions(topics)
      if (partitionE.isLeft) {
        throw new SparkException("Could not get kafka partitions")
      }
      val partitions = partitionE.right.get
      val consumerOffsetsE = cluster.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) {
        throw new SparkException("Failed to get kafka consumer offsets")
      }
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](streamingContext, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message()))
    }
    messages
  }

  /**
    * update or set zookeeper offset according to real consume scenario
    *
    * @param topics  kafka topic
    * @param groupId consumer group id
    */
  def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionE = cluster.getPartitions(Set(topic))
      if (partitionE.isLeft) {
        throw new SparkException(s"Could not get kafka partition for topic: $topic")
      }

      val partitions = partitionE.right.get
      val consumerOffsetsE = cluster.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) { // already consumed
        val earliestLeaderOffsets = cluster.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsetsE.right.get

        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case (tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (offsets.nonEmpty) cluster.setConsumerOffsets(groupId, offsets)
      } else { // not consumed
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          leaderOffsets = cluster.getEarliestLeaderOffsets(partitions).right.get
        } else {
          leaderOffsets = cluster.getLatestLeaderOffsets(partitions).right.get
        }

        val offsets = leaderOffsets.map { case (tp, offset) => (tp, offset.offset) }
        cluster.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  /**
    * update consumer offset in zookeeper
    *
    * @param rdd rdd in DStreams
    */
  def updateZKOffsets(rdd: RDD[_]): Unit = {
    val groupId = kafkaParams("group.id")
    val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = cluster.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
    }
  }

  /**
    * get latest offsets of all partitions in this topic
    *
    * @param partitions TopicAndPartition set
    * @return partition latest offset map or error
    */
  def getTopicLatestLeaderOffsets(partitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    cluster.getLatestLeaderOffsets(partitions)

  /**
    * get earliest offsets of all partitions in this topic
    *
    * @param partitions TopicAndPartition set
    * @return partition earliest offset map or error
    */
  def getTopicEarliestLeaderOffsets(partitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    cluster.getEarliestLeaderOffsets(partitions)

  /**
    * get topic partitions
    *
    * @param topics kafka topics set
    * @return TopicAndPartition set
    */
  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)

  /**
    * set topic offsets for a specific group
    *
    * @param offsets target offsets
    * @param groupId consumer group
    * @return offset set result
    */
  def setConsumerOffsets(offsets: Map[TopicAndPartition, Long],
                         groupId: String): Either[Err, Map[TopicAndPartition, Short]] = cluster.setConsumerOffsets(groupId, offsets)
}

package kafka.javaapi.consumer

import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{MessageStreamsExistException, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, KafkaStream, TopicFilter}
import kafka.serializer.{Decoder, DefaultDecoder}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
class ZkKafkaConsumerConnector(val config: ConsumerConfig,
                               val enableFetcher: Boolean)
  extends ConsumerConnector {

  private val underlying = new kafka.consumer.ZookeeperConsumerConnector(config, enableFetcher)
  private val messageStreamCreated = new AtomicBoolean(false)

  def this(config: ConsumerConfig) = this(config, true)

  // for java client
  def createMessageStreams[K, V](topicCountMap: java.util.Map[String, java.lang.Integer],
                                 keyDecoder: Decoder[K],
                                 valueDecoder: Decoder[V]): java.util.Map[String, java.util.List[KafkaStream[K, V]]] = {

    if (messageStreamCreated.getAndSet(true))
      throw new MessageStreamsExistException(this.getClass.getSimpleName +
        " can create message streams at most once", null)
    val scalaTopicCountMap: Map[String, Int] = {
      Map.empty[String, Int] ++ (topicCountMap.asInstanceOf[java.util.Map[String, Int]].asScala: mutable.Map[String, Int])
    }
    val scalaReturn = underlying.consume(scalaTopicCountMap, keyDecoder, valueDecoder)
    val ret = new java.util.HashMap[String, java.util.List[KafkaStream[K, V]]]
    for ((topic, streams) ← scalaReturn) {
      val javaStreamList = new java.util.ArrayList[KafkaStream[K, V]]
      for (stream ← streams)
        javaStreamList.add(stream)
      ret.put(topic, javaStreamList)
    }
    ret
  }

  def createMessageStreams(topicCountMap: java.util.Map[String, java.lang.Integer]): java.util.Map[String, java.util.List[KafkaStream[Array[Byte], Array[Byte]]]] =
    createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder())

  def createMessageStreamsByFilter[K, V](topicFilter: TopicFilter, numStreams: Int, keyDecoder: Decoder[K], valueDecoder: Decoder[V]) = {
    underlying.createMessageStreamsByFilter(topicFilter, numStreams, keyDecoder, valueDecoder).asJava
  }

  def createMessageStreamsByFilter(topicFilter: TopicFilter, numStreams: Int) =
    createMessageStreamsByFilter(topicFilter, numStreams, new DefaultDecoder(), new DefaultDecoder())

  def createMessageStreamsByFilter(topicFilter: TopicFilter) =
    createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder())

  def commitOffsets() {
    underlying.commitOffsets(true)
  }

  def commitOffsets(retryOnFailure: Boolean) {
    underlying.commitOffsets(retryOnFailure)
  }

  def commitOffsets(topicPartition: TopicAndPartition, offset: Long) {
    underlying.commitOffsetToZooKeeper(topicPartition, offset)
  }

  def shutdown() {
    underlying.shutdown
  }
}

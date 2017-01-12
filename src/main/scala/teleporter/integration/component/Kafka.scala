package teleporter.integration.component

import java.util.Properties

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.kafka.clients.producer._
import teleporter.integration.core._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
object Kafka {

  case class TopicPartition(topic: String, partition: Int)

  case class KafkaLocation(topicPartition: TopicPartition, offset: Long)

  object Control {

    case class Subscribe(topics: String*)

    case class UnSubscribe(topics: String*)

    case class Commit(topicPartition: TopicPartition, offset: Long)

  }

  def sourceAck(sourceKey: String, addressBind: Option[String] = None)
               (implicit center: TeleporterCenter): Source[AckMessage[KafkaLocation, KafkaMessage], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val kafkaSourceConfig = sourceContext.config.mapTo[KafkaSourceMetaBean]
    val bind = addressBind.getOrElse(sourceKey)
    val addressKey = sourceContext.address().key
    val subscribeTopics = kafkaSourceConfig.topics.split(",").map(_.split(":"))
      .map { case Array(topic, threads) ⇒ (topic, Int.box(threads.toInt)) }.toMap
    val client = center.context.register(addressKey, bind, () ⇒ consumer(addressKey)).client
    val checkPoint = KafkaCheckPoint(client.zkKafkaConnector)
    client.subscribe(subscribeTopics).map(m ⇒ SourceMessage(KafkaLocation(TopicPartition(m.topic, m.partition), m.offset), m))
      .via(SourceAck.flow(
        id = sourceContext.id,
        config = SourceAckConfig(sourceContext.config),
        commit = coordinate ⇒ checkPoint.save(sourceKey, coordinate),
        finish = () ⇒ checkPoint.complete(sourceKey)
      ))
  }

  def source(sourceKey: String, addressBind: Option[String] = None)
            (implicit center: TeleporterCenter): Source[KafkaMessage, NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val kafkaSourceConfig = sourceContext.config.mapTo[KafkaSourceMetaBean]
    val bind = addressBind.getOrElse(sourceKey)
    val addressKey = sourceContext.address().key
    val subscribeTopics = kafkaSourceConfig.topics.split(",").map(_.split(":"))
      .map { case Array(topic, threads) ⇒ (topic, Int.box(threads.toInt)) }.toMap
    center.context.register(addressKey, bind, () ⇒ consumer(addressKey)).client.subscribe(subscribeTopics)
  }

  def sink(sinkKey: String, addressBind: Option[String] = None)(implicit center: TeleporterCenter): Sink[Message[KafkaRecord], Future[Done]] = {
    flow(sinkKey, addressBind).toMat(Sink.ignore)(Keep.right)
  }

  def flow(sinkKey: String, addressBind: Option[String] = None)(implicit center: TeleporterCenter): Flow[Message[KafkaRecord], Message[KafkaRecord], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val bind = addressBind.getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new KafkaSinkAsync(parallelism = 1,
      _create = (ec) ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ producer(addressKey)).client
      }(ec), _close = {
        (_, _) ⇒
          center.context.unRegister(sinkKey, bind)
          Future.successful(Done)
      }))
  }

  def producer(key: String)(implicit center: TeleporterCenter): AutoCloseClientRef[KafkaProducer[Array[Byte], Array[Byte]]] = {
    implicit val config = center.context.getContext[AddressContext](key).config
    val props = new Properties()
    config.client.toMap.foreach {
      case (k, v) ⇒
        if (v != null && v.toString.nonEmpty) {
          props.put(k, v.toString)
        }
    }
    AutoCloseClientRef(key, new KafkaProducer[Array[Byte], Array[Byte]](props))
  }

  def consumer(key: String)(implicit center: TeleporterCenter): CloseClientRef[KafkaConsumer] = {
    implicit val config = center.context.getContext[AddressContext](key).config
    val props = new Properties()
    config.client.toMap.foreach {
      case (k, v) ⇒
        if (v != null && v.toString.nonEmpty) {
          props.put(k, v.toString)
        }
    }
    val consumerConfig = new ConsumerConfig(props)
    val consumer = new KafkaConsumer(new ZkKafkaConsumerConnector(consumerConfig))
    new AutoCloseClientRef[KafkaConsumer](key, consumer)
  }
}

class KafkaConsumer(val zkKafkaConnector: ZkKafkaConsumerConnector) extends AutoCloseable {
  def subscribe(topics: Map[String, Integer]): Source[KafkaMessage, NotUsed] = {
    val streams = zkKafkaConnector.createMessageStreams(topics.asJava).asScala
      .values.flatMap(_.asScala)
    Source(streams.toIndexedSeq)
      .flatMapConcat(stream ⇒ Source.fromIterator(() ⇒ stream.iterator()))
  }

  def commit(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    zkKafkaConnector.commitOffsets(topicAndPartition, offset)
  }

  override def close(): Unit = zkKafkaConnector.shutdown()
}

object KafkaSourceMetaBean {
  val FTopics = "topics"
}

class KafkaSourceMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {

  import KafkaSourceMetaBean._

  def topics: String = client[String](FTopics)
}

class KafkaSinkAsync(parallelism: Int = 1,
                     _create: (ExecutionContext) ⇒ Future[Producer[Array[Byte], Array[Byte]]],
                     _close: (Producer[Array[Byte], Array[Byte]], ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[Producer[Array[Byte], Array[Byte]], Message[KafkaRecord], Message[KafkaRecord]]("kafka.sink", parallelism) {
  override def create(executionContext: ExecutionContext): Future[Producer[Array[Byte], Array[Byte]]] = _create(executionContext)

  override def write(client: Producer[Array[Byte], Array[Byte]], elem: Message[KafkaRecord], executionContext: ExecutionContext): Future[Message[KafkaRecord]] = {
    val promise = Promise[Message[KafkaRecord]]()
    client.send(elem.data, new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          promise.failure(exception)
        } else {
          promise.success(elem)
        }
      }
    })
    promise.future
  }

  override def close(client: Producer[Array[Byte], Array[Byte]], executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}
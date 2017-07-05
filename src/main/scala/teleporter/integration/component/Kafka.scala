package teleporter.integration.component

import java.util.Properties
import java.util.concurrent.Executors

import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Sink, Source}
import akka.{Done, NotUsed}
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, ConsumerIterator, KafkaStream}
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kudu.client.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics

import scala.collection.JavaConverters._
import scala.concurrent._

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

  def sourceAck(sourceKey: String)
               (implicit center: TeleporterCenter): Source[AckMessage[KafkaLocation, KafkaMessage], NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val kafkaSourceConfig = sourceContext.config.mapTo[KafkaSourceMetaBean]
    val bind = Option(sourceContext.config.addressBind).getOrElse(sourceContext.stream().key)
    val addressKey = sourceContext.address().key
    val subscribeTopics = kafkaSourceConfig.topics.split(",").map(_.split(":"))
      .map { case Array(topic, threads) ⇒ (topic, Int.box(threads.toInt)) }.toMap
    val client = center.context.register(addressKey, bind, () ⇒ consumer(addressKey, subscribeTopics)).client
    val savePoint = KafkaSavePoint(client.zkKafkaConnector)
    client.getInstance().map(m ⇒ SourceMessage(KafkaLocation(TopicPartition(m.topic, m.partition), m.offset), m))
      .via(SourceAck.flow(
        id = sourceContext.id,
        config = SourceAckConfig(sourceContext.config),
        commit = coordinate ⇒ savePoint.save(sourceKey, coordinate),
        finish = coordinate ⇒ savePoint.complete(sourceKey, coordinate)
      ))
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
      .via(Metrics.count[AckMessage[KafkaLocation, KafkaMessage]](sourceKey)(center.metricsRegistry))
  }

  def source(sourceKey: String)(implicit center: TeleporterCenter): Source[KafkaMessage, NotUsed] = {
    val sourceContext = center.context.getContext[SourceContext](sourceKey)
    val kafkaSourceConfig = sourceContext.config.mapTo[KafkaSourceMetaBean]
    val bind = Option(sourceContext.config.addressBind).getOrElse(sourceContext.stream().key)
    val addressKey = sourceContext.address().key
    val subscribeTopics = kafkaSourceConfig.topics.split(",").map(_.split(":"))
      .map { case Array(topic, threads) ⇒ (topic, Int.box(threads.toInt)) }.toMap
    center.context.register(addressKey, bind, () ⇒ consumer(addressKey, subscribeTopics)).client.getInstance()
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sourceKey, sourceContext.config)))
      .via(Metrics.count[KafkaMessage](sourceKey)(center.metricsRegistry))
  }

  def sink(sinkKey: String)(implicit center: TeleporterCenter): Sink[Message[KafkaRecord], Future[Done]] = {
    flow(sinkKey).toMat(Sink.ignore)(Keep.right)
  }

  def flow(sinkKey: String)(implicit center: TeleporterCenter): Flow[Message[KafkaRecord], Message[KafkaRecord], NotUsed] = {
    val sinkContext = center.context.getContext[SinkContext](sinkKey)
    val bind = Option(sinkContext.config.addressBind).getOrElse(sinkKey)
    val addressKey = sinkContext.address().key
    Flow.fromGraph(new KafkaSinkAsync(parallelism = 1,
      _create = (ec) ⇒ Future {
        center.context.register(addressKey, bind, () ⇒ producer(addressKey)).client
      }(ec), _close = {
        (_, _) ⇒
          center.context.unRegister(addressKey, bind)
          Future.successful(Done)
      }))
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(sinkKey, sinkContext.config)))
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

  def consumer(key: String, topics: Map[String, Integer])(implicit center: TeleporterCenter): CloseClientRef[KafkaConsumer] = {
    implicit val config = center.context.getContext[AddressContext](key).config
    val props = new Properties()
    config.client.toMap.foreach {
      case (k, v) ⇒
        if (v != null && v.toString.nonEmpty) {
          props.put(k, v.toString)
        }
    }
    val consumerConfig = new ConsumerConfig(props)
    val consumer = new KafkaConsumer(new ZkKafkaConsumerConnector(consumerConfig), topics)
    new AutoCloseClientRef[KafkaConsumer](key, consumer)
  }
}

class KafkaSource(threadName: String, kafkaStream: KafkaStream[Array[Byte], Array[Byte]])
  extends CommonSourceAsync[KafkaMessage, ConsumerIterator[Array[Byte], Array[Byte]]]("KafkaSource") {
  val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors
    .newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat(s"kafka-$threadName-%d").build()))

  override def create(ec: ExecutionContext): Future[ConsumerIterator[Array[Byte], Array[Byte]]] = {
    Future {
      kafkaStream.iterator()
    }(executionContext)
  }

  override def readData(client: ConsumerIterator[Array[Byte], Array[Byte]], ec: ExecutionContext): Future[Option[KafkaMessage]] = {
    Future {
      Option(client.next())
    }(executionContext)
  }

  override def close(client: ConsumerIterator[Array[Byte], Array[Byte]], ec: ExecutionContext): Future[Done] = {
    executionContext.shutdownNow()
    Future.successful(Done)
  }
}

class KafkaConsumer(val zkKafkaConnector: ZkKafkaConsumerConnector, topics: Map[String, Integer]) extends AutoCloseable {
  val streamSource: Source[MessageAndMetadata[Array[Byte], Array[Byte]], NotUsed] = {
    Source.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val streamSources = zkKafkaConnector.createMessageStreams(topics.asJava).asScala
        .flatMap { case (topic, streams) ⇒
          streams.asScala.map { stream ⇒
            Source.fromGraph(new KafkaSource(topic, stream))
          }
        }.toIndexedSeq
      val merge = b.add(Merge[MessageAndMetadata[Array[Byte], Array[Byte]]](streamSources.size))
      for (i ← streamSources.indices) {
        streamSources(i) ~> merge.in(i)
      }
      SourceShape(merge.out)
    })
  }

  def getInstance(): Source[KafkaMessage, NotUsed] = streamSource

  def commit(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    zkKafkaConnector.commitOffsets(topicAndPartition, offset)
  }

  override def close(): Unit = zkKafkaConnector.shutdown()
}

class KafkaSourceMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {
  val FTopics = "topics"

  def topics: String = client[String](FTopics)
}

class KafkaSinkAsync(parallelism: Int = 1,
                     _create: (ExecutionContext) ⇒ Future[Producer[Array[Byte], Array[Byte]]],
                     _close: (Producer[Array[Byte], Array[Byte]], ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[Producer[Array[Byte], Array[Byte]], Message[KafkaRecord], Message[KafkaRecord]]("kafka.sink", parallelism, identity) {
  override def create(executionContext: ExecutionContext): Future[Producer[Array[Byte], Array[Byte]]] = _create(executionContext)

  override def write(client: Producer[Array[Byte], Array[Byte]], elem: Message[KafkaRecord], executionContext: ExecutionContext): Future[Message[KafkaRecord]] = {
    val promise = Promise[Message[KafkaRecord]]()
    client.send(elem.data, new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          promise.success(elem)
        } else {
          promise.failure(exception)
        }
      }
    })
    promise.future
  }

  override def close(client: Producer[Array[Byte], Array[Byte]], executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}
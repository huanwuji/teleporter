package teleporter.integration.component

import java.util.Properties

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import com.google.protobuf.{ByteString ⇒ GByteString}
import com.typesafe.scalalogging.LazyLogging
import kafka.consumer.{ConsumerConfig, ConsumerIterator, KafkaStream}
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.kafka.clients.producer._
import teleporter.integration._
import teleporter.integration.component.KafkaComponent.{KafkaLocation, TopicPartition}
import teleporter.integration.component.KafkaExchanger.InnerAddress
import teleporter.integration.component.ShadowPublisher.Register
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics.{Measurement, Tag, Tags}
import teleporter.integration.metrics.MetricsCounter
import teleporter.integration.protocol.proto.KafkaBuf.{KafkaProto, KafkaProtos}
import teleporter.integration.transaction._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.concurrent.duration._

/**
  * date 2015/8/3.
  *
  * @author daikui
  */
object KafkaComponent {

  case class TopicPartition(topic: String, partition: Int)

  case class KafkaLocation(topicPartition: TopicPartition, offset: Long)

  def kafkaProducerApply: ClientApply = (key, center) ⇒ {
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

  def kafkaConsumerApply: ClientApply = (key, center) ⇒ {
    implicit val config = center.context.getContext[AddressContext](key).config
    val props = new Properties()
    config.client.toMap.foreach {
      case (k, v) ⇒
        if (v != null && v.toString.nonEmpty) {
          props.put(k, v.toString)
        }
    }
    val consumerConfig = new ConsumerConfig(props)
    new CloseClientRef[ZkKafkaConsumerConnector](key, new ZkKafkaConsumerConnector(consumerConfig), _.client.shutdown())
  }
}

object KafkaPublisherMetaBean {
  val FTopics = "topics"
}

class KafkaPublisherMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {

  import KafkaPublisherMetaBean._

  def topics: String = client[String](FTopics)
}

trait PublisherRouter {
  self: Component ⇒
  var routingSeqNr = 0L
  val tIdMapping: mutable.LongMap[InnerAddress] = mutable.LongMap[InnerAddress]()

  def routingOut[A](message: TeleporterMessage[A], innerRef: ActorRef)(implicit outerRef: ActorRef): (ActorRef, TeleporterMessage[A]) = {
    val innerAddress = InnerAddress(message.id, innerRef)
    val outerTId = TId(id(), routingSeqNr, 1)
    routingSeqNr += 1
    tIdMapping += (routeId(outerTId) → innerAddress)
    (innerRef, message.copy[A](id = outerTId, sourceRef = outerRef))
  }

  def routingIn(outerTId: TId): Unit = tIdMapping.remove(routeId(outerTId)).foreach { innerAddress ⇒
    if (outerTId.channelId != 1) {
      innerAddress.innerRef ! innerAddress.tId.copy(channelId = outerTId.channelId)
    } else {
      innerAddress.innerRef ! innerAddress.tId
    }
  }

  def routerClear(): Unit = tIdMapping.clear()

  private def routeId(tId: TId): Long = tId.persistenceId << 32 | tId.seqNr.toInt
}

object KafkaExchanger {

  case class InnerAddress(tId: TId, innerRef: ActorRef)

}

class KafkaPublisher(override val key: String)(implicit val center: TeleporterCenter)
  extends ActorPublisher[TeleporterKafkaMessage]
    with PublisherRouter
    with Component
    with LazyLogging {
  val sourceContext: SourceContext = center.context.getContext[SourceContext](key)
  val counter: MetricsCounter = center.metricsRegistry.counter(Measurement(key, Seq(Tags.success)))
  val zkConnector: ZkKafkaConsumerConnector = center.components.address[ZkKafkaConsumerConnector](sourceContext.config.address)
  val kafkaStreamWorkers: IndexedSeq[ActorRef] = {
    var totalThreads = 0
    val kafkaPublisherMetaBean = sourceContext.config.mapTo[KafkaPublisherMetaBean]
    val topicMaps: Map[String, Integer] = kafkaPublisherMetaBean.topics.split(",").map {
      topicInfo ⇒
        topicInfo.split(":") match {
          case Array(name, threads) ⇒
            val topicThreads = Int.box(threads.toInt)
            totalThreads += topicThreads
            (name, topicThreads)
          case _ ⇒ throw new IllegalArgumentException(s"$topicInfo config not match, eg:trade:2,item:3....")
        }
    }.toMap
    if (totalThreads > 32) {
      logger.warn("Kafka worker is use independent thread pool, Only 32 of threads, If totalThreads more than this, must modify kafka-workers-dispatcher config")
    }
    zkConnector.createMessageStreams(topicMaps.asJava).asScala.flatMap {
      case (_, streams) ⇒
        streams.asScala.map(stream ⇒ context.actorOf(Props(classOf[KafkaStreamWorker], key, zkConnector, stream, center)
          .withDispatcher("akka.teleporter.kafka-workers-dispatcher")))
    }.toIndexedSeq
  }
  var shadowRef: ActorRef = Actor.noSender
  var selfDemand = 0L
  val workersDemand = mutable.Map(kafkaStreamWorkers.map(ref ⇒ (ref, 0L)): _*)
  val buffer: mutable.Queue[(ActorRef, TeleporterKafkaMessage)] = mutable.Queue[(ActorRef, TeleporterKafkaMessage)]()

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    logger.info(s"$key will start")
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    logger.error(reason.getLocalizedMessage, reason)
  }

  override def receive: Receive = {
    case Register(registerShadowRef) ⇒ reset(); this.shadowRef = registerShadowRef
    case Request(n) ⇒
      worksRequest(n)
      delivery(n, onNext)
    case ShadowRequest(n) ⇒
      worksRequest(n)
      delivery(n, shadowRef ! _)
    case message: TeleporterKafkaMessage ⇒
      buffer.enqueue(routingOut(message, sender()))
      shadowRef match {
        case Actor.noSender ⇒ delivery(selfDemand, onNext)
        case _ ⇒ delivery(selfDemand, shadowRef ! _)
      }
    case tId: TId ⇒ routingIn(tId)
    case ActorPublisherMessage.Cancel ⇒
      sourceContext.address().clientRefs.close(key)
      context.stop(self)
    case x ⇒ logger.warn(s"kafka consumer can't arrived, $x")
  }

  def worksRequest(request: Long): Unit = {
    selfDemand += request
    workersDemand.foreach {
      case (workerRef, demand) ⇒
        if (demand < selfDemand) {
          workerRef ! Request(selfDemand)
          workersDemand.update(workerRef, demand + selfDemand)
        }
    }
  }

  @tailrec
  final def delivery(n: Long, handler: TeleporterKafkaMessage ⇒ Unit): Unit =
    if (n > 0 && buffer.nonEmpty) {
      val (innerRef, message) = buffer.dequeue()
      handler(message)
      selfDemand -= 1
      workersDemand.update(innerRef, workersDemand(innerRef) - 1)
      counter.inc()
      delivery(n - 1, handler)
    }

  def reset(): Unit = {
    routerClear()
    routingSeqNr = 0
    selfDemand = 0
    workersDemand.keys.foreach(workersDemand.update(_, 0))
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info(s"kafka component $key will stop")
  }
}

class KafkaStreamWorker(val key: String,
                        zkConnector: ZkKafkaConsumerConnector,
                        kafkaStream: KafkaStream[Array[Byte], Array[Byte]])(implicit center: TeleporterCenter) extends Actor with LazyLogging {
  val streamIt: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
  val transaction: Transaction[KafkaMessage, Seq[KafkaLocation]] = Transaction[KafkaMessage, Seq[KafkaLocation]](key, new KafkaRecoveryPoint(zkConnector))
  val metrics: mutable.HashMap[TopicPartition, MetricsCounter] = mutable.HashMap[TopicPartition, MetricsCounter]()

  import context.dispatcher
  import transaction._

  override def receive: Actor.Receive = {
    case Request(n) ⇒ delivery(n)
    case tId: TId ⇒ end(tId)
    case x ⇒ logger.warn(s"$x not arrived")
  }

  val locations: mutable.Map[TopicPartition, KafkaLocation] = mutable.Map[TopicPartition, KafkaLocation]()

  @tailrec
  final def delivery(n: Long): Unit = {
    if (n > 0) {
      tryBegin(locations.values.toSeq, {
        val kafkaMessageOption = Component.getIfPresent(streamIt)
        kafkaMessageOption.foreach {
          message ⇒
            val topicPartition = TopicPartition(message.topic, message.partition)
            metrics.getOrElseUpdate(topicPartition, {
              center.metricsRegistry.counter(Measurement(key, Seq(Tags.success, Tag("topic", topicPartition.topic), Tag("partition", topicPartition.partition.toString))))
            }).inc()
            locations.put(topicPartition, KafkaLocation(topicPartition, message.offset))
        }
        kafkaMessageOption
      }, sender() ! _) match {
        case Transaction.Normal | Transaction.Retry ⇒
          delivery(n - 1)
        case Transaction.NoData ⇒ throw new IllegalStateException("No data can't happened")
        case Transaction.OverLimit ⇒
          context.system.scheduler.scheduleOnce(1.seconds, self, Request(n))
      }
    }
  }
}

class KafkaSubscriberWork(val client: Producer[Array[Byte], Array[Byte]]) extends SubscriberWorker[Producer[Array[Byte], Array[Byte]]] {

  override protected def handle(onNext: OnNext, nrOfRetries: Int): Unit = {
    val record = onNext.element.asInstanceOf[TeleporterKafkaRecord]
    client.send(record.data, new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          failure(onNext, exception, nrOfRetries)
        } else {
          success(onNext)
        }
      }
    })
  }
}

class KafkaSubscriber(override val key: String)(implicit val center: TeleporterCenter) extends SubscriberSupport[Producer[Array[Byte], Array[Byte]]] {
  override def workProps: Props = Props(classOf[KafkaSubscriberWork], client)
}

object KafkaProtoData {
  def apply(message: TeleporterKafkaMessage): KafkaProto = {
    val messageAndMetadata = message.data
    KafkaProto.newBuilder()
      .setTId(GByteString.copyFrom(message.id.toBytes))
      .setTopic(messageAndMetadata.topic)
      .setKey(GByteString.copyFrom(messageAndMetadata.key()))
      .setPartition(messageAndMetadata.partition)
      .setMessage(GByteString.copyFrom(messageAndMetadata.message()))
      .build()
  }
}

object KafkaProtoDataList {
  def apply(messages: Seq[TeleporterMessage[KafkaMessage]]): KafkaProtos = {
    KafkaProtos.newBuilder().addAllProtos(messages.map(KafkaProtoData(_)).asJava).build()
  }
}
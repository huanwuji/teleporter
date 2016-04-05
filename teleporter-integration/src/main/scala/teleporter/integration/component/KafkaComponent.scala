package teleporter.integration.component

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor._
import com.google.protobuf.{ByteString ⇒ GByteString}
import com.typesafe.scalalogging.LazyLogging
import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.kafka.clients.producer._
import teleporter.integration.SourceControl.{CompleteThenStop, ErrorThenStop, Register}
import teleporter.integration.component.KafkaComponent.{KafkaLocation, TopicPartition}
import teleporter.integration.component.KafkaExchanger.InnerAddress
import teleporter.integration.conf._
import teleporter.integration.core._
import teleporter.integration.metrics.MetricsCounter
import teleporter.integration.proto.KafkaBuf.{KafkaProto, KafkaProtos}
import teleporter.integration.transaction._

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class KafkaProducerAddressBuilder(override val conf: Conf.Address)(implicit override val center: TeleporterCenter)
  extends AddressBuilder[Producer[Array[Byte], Array[Byte]]] with PropsSupport with LazyLogging {
  override def build: Address[Producer[Array[Byte], Array[Byte]]] = {
    val props = new Properties()
    val _cmpProps = camel2Point(cmpProps(conf.props)).mapValues(_.toString)
    props.putAll(_cmpProps)
    new AutoCloseAddress[Producer[Array[Byte], Array[Byte]]](conf, new KafkaProducer[Array[Byte], Array[Byte]](props))
  }
}

class KafkaConsumerAddress(val conf: Conf.Address, val client: ZkKafkaConsumerConnector) extends Address[ZkKafkaConsumerConnector] {
  override def close(): Unit = client.shutdown()
}

class KafkaConsumerAddressBuilder(override val conf: Conf.Address)(implicit override val center: TeleporterCenter) extends AddressBuilder[ZkKafkaConsumerConnector] with PropsSupport {
  override def build: Address[ZkKafkaConsumerConnector] = {
    val props = new Properties()
    val _cmpProps = camel2Point(cmpProps(conf.props)).mapValues(_.toString)
    props.putAll(_cmpProps)
    val consumerConfig = new ConsumerConfig(props)
    new KafkaConsumerAddress(conf, new ZkKafkaConsumerConnector(consumerConfig))
  }
}

trait PublisherRouter {
  self: Component ⇒
  var routingSeqNr = 0L
  val tIdMapping = mutable.Map[TId, InnerAddress]()

  def routingOut[A](message: TeleporterMessage[A], innerRef: ActorRef)(implicit outerRef: ActorRef): (ActorRef, TeleporterMessage[A]) = {
    val innerAddress = InnerAddress(message.id, innerRef)
    val outerTId = TId(id, routingSeqNr, 1)
    routingSeqNr += 1
    tIdMapping += (outerTId → innerAddress)
    (innerRef, message.copy(id = outerTId, sourceRef = outerRef))
  }

  def routingIn(outerTId: TId): Unit = tIdMapping.remove(outerTId).foreach(innerAddress ⇒ {
    innerAddress.innerRef ! innerAddress.tId
  })

  def routerClear() = tIdMapping.clear()
}

object KafkaExchanger {

  case class InnerAddress(tId: TId, innerRef: ActorRef)

}

/**
 * source://addressId?topic=trade
 */
class KafkaPublisher(override val id: Int)(implicit center: TeleporterCenter)
  extends ActorPublisher[TeleporterKafkaMessage]
  with PublisherRouter
  with Component
  with ActorLogging {

  import KafkaPublisherPropsConversions._

  val conf = center.sourceFactory.loadConf(id)
  val counter = center.metricsRegistry.counter(conf.name)
  val zkConnector = center.addressing[ZkKafkaConsumerConnector](conf)
  val kafkaStreamWorkers = {
    var totalThreads = 0
    val topics = conf.props.topics
    val topicMaps: Map[String, Integer] = topics.get.split(",").map {
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
      log.warning("Kafka worker is use independent thread pool, Only 32 of threads, If totalThreads more than this, must modify kafka-workers-dispatcher config")
    }
    val consumerMap = zkConnector.createMessageStreams(topicMaps)
    var index = 0
    consumerMap.flatMap {
      case entry@(topic, streams) ⇒
        index += 1
        val innerId = id - id % 1000 + index
        log.info(s"$topic, innerId:$innerId")
        streams.map(stream ⇒ context.actorOf(Props(classOf[KafkaStreamWorker], id - id % 100 + index, conf.name, TransactionConf(conf.props), zkConnector, stream, center)
          .withDispatcher("akka.teleporter.kafka-workers-dispatcher")))
    }.toIndexedSeq
  }
  var shadowRef = Actor.noSender
  var selfDemand = 0L
  val workersDemand = mutable.Map(kafkaStreamWorkers.map(ref ⇒ (ref, 0L)): _*)
  val buffer = mutable.Queue[(ActorRef,TeleporterKafkaMessage)]()
  var isClosed = false

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info(s"$id will start")
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(reason.getLocalizedMessage, reason)
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
    case ErrorThenStop(reason: Throwable) ⇒
      log.info(s"$id error then stop")
      cmpClose()
      onErrorThenStop(reason)
    case CompleteThenStop ⇒
      log.info(s"$id will complete then stop")
      cmpClose()
      onCompleteThenStop()
    case x ⇒ log.warning("kafka consumer can't arrived, {}", x)
  }

  def worksRequest(request: Long) = {
    selfDemand += request
    workersDemand.foreach {
      case entry@(workerRef, demand) ⇒
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

  private def cmpClose() = {
    if (!isClosed) {
      center.removeAddress(conf)
      isClosed = true
    }
  }

  def reset() = {
    routerClear()
    routingSeqNr = 0
    selfDemand = 0
    workersDemand.keys.foreach(workersDemand.update(_, 0))
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    cmpClose()
    log.info(s"kafka component $id will stop")
  }
}

class KafkaStreamWorker(val id: Int, name: String,
                        val transactionConf: TransactionConf,
                        zkConnector: ZkKafkaConsumerConnector,
                        kafkaStream: KafkaStream[Array[Byte], Array[Byte]])(implicit center: TeleporterCenter) extends Actor with BatchCommitTransaction[KafkaMessage, KafkaLocation] with ActorLogging {
  val streamIt = kafkaStream.iterator()
  override implicit val recoveryPoint: RecoveryPoint[KafkaLocation] = new KafkaRecoveryPoint(zkConnector)
  val metrics = mutable.HashMap[TopicPartition,MetricsCounter]()

  override def receive: Actor.Receive = {
    case Request(n) ⇒ delivery(n)
    case tId: TId ⇒ end(tId)
    case x ⇒ log.warning(s"$x not arrived")
  }

  @tailrec
  final def delivery(n: Long): Unit = {
    if (n > 0) {
      if (recovery.hasNext) {
        sender() ! recovery.next()
        delivery(n - 1)
      } else if (streamIt.hasNext()) {
        val kafkaMessage = streamIt.next()
        val topicPartition = TopicPartition(kafkaMessage.topic, kafkaMessage.partition)
        metrics.getOrElseUpdate(topicPartition, {
          center.metricsRegistry.counter(s"$name:${topicPartition.topic}:${topicPartition.partition}")
        }).inc()
        begin(KafkaLocation(topicPartition, kafkaMessage.offset), kafkaMessage)(sender() ! _)
        delivery(n - 1)
      } else {
        log.info(s"Does this branch can arrived")
      }
    }
  }

  val locations = mutable.Map[TopicPartition, KafkaLocation]()

  override protected def batchCollect(seqNr: Long, batchCoordinate: KafkaLocation): Unit = {
    if (seqNr % transactionConf.batchSize == 0) {
      batchGrouped += ((seqNr / transactionConf.batchSize) → BatchInfo(Seq(batchCoordinate), 0))
      locations.clear()
    }
    locations += (batchCoordinate.topicPartition → batchCoordinate)
    if (seqNr % transactionConf.batchSize == transactionConf.batchSize - 1) {
      val batch = batchGrouped(seqNr / transactionConf.batchSize)
      batch.batchCoordinates = locations.values.toSeq
    }
  }
}

class KafkaSubscriber(override val id: Int)(implicit center: TeleporterCenter)
  extends ActorSubscriber
  with Component with PropsSupport with ActorLogging {
  val conf = center.sinkFactory.loadConf(id)
  override protected val requestStrategy: RequestStrategy = RequestStrategyManager(conf.props)
  val producer = center.addressing[Producer[Array[Byte], Array[Byte]]](conf.addressId.get, conf)
  val counter = center.metricsRegistry.counter(conf.name)

  override def receive: Actor.Receive = {
    case OnNext(element: TeleporterKafkaRecord) ⇒
      producer.send(element.data, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          counter.inc()
          if (exception != null) {
            log.error(exception.getLocalizedMessage, exception)
          } else {
            element.toNext(element)
          }
        }
      })
    case OnComplete ⇒ context.stop(self)
    case OnError(e) ⇒ log.error(s"$id error", e); context.stop(self)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    center.removeAddress(conf.addressId.get, conf)
    log.info(s"kafka subscriber $id will stop")
  }
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
    KafkaProtos.newBuilder().addAllProtos(messages.map(KafkaProtoData(_))).build()
  }
}

object KafkaComponent {

  case class TopicPartition(topic: String, partition: Int)

  case class KafkaLocation(topicPartition: TopicPartition, offset: Long)

}
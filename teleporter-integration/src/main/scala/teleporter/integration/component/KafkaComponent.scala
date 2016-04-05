package teleporter.integration.component

import java.util.{Collections, Properties}

import akka.actor.{Actor, ActorLogging, Props}
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import com.codahale.metrics.MetricRegistry
import com.google.protobuf.{ByteString ⇒ GByteString}
import com.typesafe.scalalogging.LazyLogging
import kafka.consumer.{ConsumerConfig, ConsumerIterator}
import kafka.javaapi.consumer.ZkKafkaConsumerConnector
import org.apache.kafka.clients.producer._
import teleporter.integration.component.Control.{CompleteThenStop, ErrorThenStop, StopWhenHaveData}
import teleporter.integration.component.KafkaComponent.KafkaLocation
import teleporter.integration.conf._
import teleporter.integration.core._
import teleporter.integration.metrics.HdrInstrumentedBuilder
import teleporter.integration.proto.KafkaBuf.{KafkaProto, KafkaProtos}
import teleporter.integration.transaction._

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class KafkaProducerAddressBuilder(override val conf: Conf.Address)(implicit override val teleporterCenter: TeleporterCenter) extends AddressBuilder[Producer[Array[Byte], Array[Byte]]]
with PropsSupport with LazyLogging {
  override def build: Address[Producer[Array[Byte], Array[Byte]]] = {
    val props = new Properties()
    val _cmpProps = camel2Point(cmpProps(conf.props)).mapValues(_.toString)
    props.putAll(_cmpProps)
    new Address[Producer[Array[Byte], Array[Byte]]] {
      override val _conf: Conf.Address = conf
      override val client: Producer[Array[Byte], Array[Byte]] = new KafkaProducer[Array[Byte], Array[Byte]](props)

      override def close(): Unit = client.close()
    }
  }
}

/**
 * @param conf address://kafka.consumer/id=etl_kafka&zookeeper.connect=&group.id=&zookeeper.session.timeout.ms=400&zookeeper.sync.time.ms=200&auto.commit.interval.ms=60000
 */
class KafkaConsumerAddressBuilder(override val conf: Conf.Address)(implicit override val teleporterCenter: TeleporterCenter) extends AddressBuilder[ZkKafkaConsumerConnector] with PropsSupport {
  override def build: Address[ZkKafkaConsumerConnector] = {
    val props = new Properties()
    val _cmpProps = camel2Point(cmpProps(conf.props)).mapValues(_.toString)
    props.putAll(_cmpProps)
    val consumerConfig = new ConsumerConfig(props)
    new Address[ZkKafkaConsumerConnector] {
      override val _conf: Conf.Address = conf
      override val client: ZkKafkaConsumerConnector = new ZkKafkaConsumerConnector(consumerConfig)

      override def close(): Unit = client.shutdown()
    }
  }
}

/**
 * source://addressId?topic=trade
 */
class KafkaPublisher(override val id: Int)
                    (implicit teleporterCenter: TeleporterCenter)
  extends ActorPublisher[TeleporterKafkaMessage] with Component with BatchCommitTransaction[KafkaMessage, KafkaLocation]
  with ActorLogging {

  import KafkaPublisherPropsConversions._

  val conf = teleporterCenter.sourceFactory.loadConf(id)
  override val transactionConf: TransactionConf = TransactionConf(conf)
  val zkConnector = teleporterCenter.addressing[ZkKafkaConsumerConnector](conf).get
  val topic = conf.props.topic
  val streamIt = {
    val consumerMap = zkConnector.createMessageStreams(Collections.singletonMap(topic, 1))
    val stream = consumerMap.get(topic).get(0)
    stream.iterator()
  }
  val kafkaWorker = context.actorOf(KafkaWorker.props(streamIt), s"$id-worker")
  val cache = mutable.Queue[TeleporterKafkaMessage]()
  var kafkaActive = true
  var isStopWhenHaveData = false
  override implicit val recoveryPoint: RecoveryPoint[KafkaLocation] = new KafkaRecoveryPoint(zkConnector)


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info(s"$id $topic will start")
  }

  @throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.warning(s"Because of error, consumer was restart, $totalDemand")
    Thread.sleep(5000)
    kafkaWorker ! totalDemand
    log.error(reason.getLocalizedMessage, reason)
  }

  override def receive: Receive = {
    case Request(n) ⇒
      var _n = n
      while (cache.nonEmpty && _n > 0) {
        onNext(cache.dequeue())
        _n -= 1
      }
      val recoveryIt = recovery()
      while (_n > 0 && recoveryIt.nonEmpty) {
        onNext(recoveryIt.next())
        _n -= 1
      }
      if (_n > 0) kafkaWorker ! _n
    case kafkaMessage: KafkaMessage ⇒
      if (isStopWhenHaveData) {
        self ! CompleteThenStop
      } else {
        begin(KafkaLocation(kafkaMessage.topic, kafkaMessage.partition, kafkaMessage.offset), kafkaMessage) {
          msg ⇒
            if (totalDemand == 0) {
              cache += msg
            } else {
              onNext(msg)
            }
        }
      }
    case tId: TId ⇒ end(tId)
    case StopWhenHaveData ⇒ isStopWhenHaveData = true; kafkaWorker ! 1L
    case ErrorThenStop(reason: Throwable) ⇒
      logger.info(s"$topic error then stop")
      postStop()
      context.stop(kafkaWorker)
      onErrorThenStop(reason)
    case CompleteThenStop ⇒ postStop(); context.stop(kafkaWorker); onCompleteThenStop()
    case x ⇒ log.warning("kafka consumer can't arrived, {}", x)
  }

  val locations = mutable.Map[Int, KafkaLocation]()

  override protected def batchCollect(seqNr: Long, batchCoordinate: KafkaLocation): Unit = {
    if (seqNr % transactionConf.batchSize == 0) {
      batchGrouped +=(seqNr / transactionConf.batchSize, BatchInfo(Seq(batchCoordinate), 0))
      locations.clear()
    }
    locations += (batchCoordinate.partition → batchCoordinate)
    if (seqNr % transactionConf.batchSize == transactionConf.batchSize - 1) {
      val batch = batchGrouped(seqNr / transactionConf.batchSize)
      batch.batchCoordinates = locations.values.toSeq
    }
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    if (kafkaActive) {
      log.info(s"kafka component $id $topic will stop")
      teleporterCenter.removeAddress(conf)
      kafkaActive = false
    }
  }
}

class KafkaWorker(streamIt: ConsumerIterator[Array[Byte], Array[Byte]]) extends Actor with ActorLogging {
  override def receive: Actor.Receive = {
    case num: Long ⇒ deliver(num)
    case x ⇒ log.warning("kafkaWorker can't arrived, {}", x)
  }

  @tailrec
  final def deliver(num: Long): Unit = {
    if (num > 0 && streamIt.hasNext()) {
      sender() ! streamIt.next()
      deliver(num - 1)
    }
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    log.info(s"${self.path} was stop")
  }
}

object KafkaWorker {
  def props(streamIt: ConsumerIterator[Array[Byte], Array[Byte]]) = Props(classOf[KafkaWorker], streamIt)
}

class KafkaSubscriber(override val id: Int)(implicit teleporterCenter: TeleporterCenter)
  extends HdrInstrumentedBuilder with ActorSubscriber
  with Component with PropsSupport with ActorLogging {
  override val metricRegistry: MetricRegistry = teleporterCenter.metricRegistry
  val (errorCount, successCount) = (metrics.counter(s"$id:success"), metrics.histogram(s"$id:error"))
  val conf = teleporterCenter.sinkFactory.loadConf(id)
  override protected val requestStrategy: RequestStrategy = RequestStrategy(conf.props)
  val producer = teleporterCenter.addressing[Producer[Array[Byte], Array[Byte]]](conf.addressId.get).get

  override def receive: Actor.Receive = {
    case OnNext(element: TeleporterKafkaRecord) ⇒
      producer.send(element.data, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            log.error(exception.getLocalizedMessage, exception)
            errorCount += 1
          } else {
            element.toNext(element)
            successCount += 1
          }
        }
      })
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = teleporterCenter.removeAddress(conf.addressId.get)
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

  case class KafkaLocation(topic: String, partition: Int, offset: Long)

}
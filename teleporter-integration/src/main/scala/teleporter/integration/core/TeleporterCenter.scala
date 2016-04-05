package teleporter.integration.core

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.markatta.akron.CronTab
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.reactivestreams.{Publisher, Subscriber}
import teleporter.integration.CmpType
import teleporter.integration.conf.Conf.GlobalProps
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.conf.{MongoTeleporterConfigFactory, TeleporterConfigFactory}
import teleporter.integration.metrics.{InfluxdbReporter, MetricRegistry}
import teleporter.integration.transaction._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * date 2015/8/3.
 * @author daikui
 */
trait TeleporterCenter extends LazyLogging {
  val configFactory: TeleporterConfigFactory
  val shareObjects: TrieMap[String, Any] = TrieMap[String,Any]()
  val addressFactory: AddressFactory
  val sourceFactory: ActorSourceFactory
  val sinkFactory: ActorSinkFactory
  val defaultRecoveryPoint: RecoveryPoint[Conf.Source]
  val actorAddresses: ActorAddresses
  val metricsRegistry: MetricRegistry
  val streamManagers: mutable.Map[Int, ActorRef] = mutable.Map[Int,ActorRef]()
  val streamContexts = mutable.HashMap[Int,StreamContext]()
  val crontab: ActorRef

  implicit def system: ActorSystem

  implicit def materializer: Materializer

  implicit final val self = this

  def recoveryPoint(transactionConf: TransactionConf): RecoveryPoint[Conf.Source] = if (transactionConf.recoveryPointEnabled) defaultRecoveryPoint else RecoveryPoint.empty

  def addressing[T](conf: Conf.Source): T = addressFactory.addressing(conf)

  def addressing[T](id: Int, ref: Any): T = {
    addressFactory.addressing(id, ref)
  }


  def addressing[T](name: String, ref: Any): T = addressFactory.addressing(configFactory.nameMapper.getId(name, CmpType.Address), ref)

  def removeAddress[T](id: Int, ref: Any): Unit = addressFactory.close(id, ref)

  def removeAddress[T](conf: Conf.Source): Unit = addressFactory.close(conf)

  def publisher[T](id: Int): Publisher[T] = sourceFactory.build(id)

  def subscriber[T](id: Int): Subscriber[T] = sinkFactory.build(id)

  def task(id: Int): Conf.Task = configFactory.task(id)

  def streamContext(streamId: Int): StreamContext = streamContexts.getOrElseUpdate(streamId, new StreamContext())

  def streamContext(streamId: Int, context: StreamContext): Unit = streamContexts += (streamId → context)

  /**
   * Register actorRef to listen all stream message state, to decide stream will stop,restart,recover or other
   * @param taskId taskId
   * @param actorRef actorRef
   * @return
   */
  def taskCenter(taskId: Int, actorRef: ActorRef): Unit = actorAddresses.register(taskId, actorRef, CmpType.Task)

  /**
   * This will accept all message from it's sub streams for mange it, like child stream restart,stop and other
   * @param taskId taskId
   * @return
   */
  def taskCenter(taskId: Int): ActorRef = if (actorAddresses.exists(taskId, CmpType.Task)) actorAddresses(taskId, CmpType.Task) else ActorRef.noSender

  def streamManager(taskId: Int, actorRef: ActorRef) = streamManagers += taskId → actorRef

  def streamManager(taskId: Int) = streamManagers(taskId)

  def task(name: String): Conf.Task = configFactory.task(configFactory.nameMapper.getId(name, CmpType.Task))

  def source[T](id: Int): Source[T, NotUsed] = Source.fromPublisher(publisher(id))

  def source[T](name: String): Source[T, NotUsed] = Source.fromPublisher(publisher(configFactory.nameMapper.getId(name, CmpType.Source)))

  def sink[T](id: Int): Sink[T, NotUsed] = Sink.fromSubscriber(subscriber(id))

  def sink[T](name: String): Sink[T, NotUsed] = Sink.fromSubscriber(subscriber(configFactory.nameMapper.getId(name, CmpType.Sink)))

  def actor(id: Int, cmpType: CmpType = CmpType.Source): ActorRef = {
    actorAddresses(id, cmpType)
  }

  def shadowActor(id: Int): ActorRef = {
    actorAddresses(id, CmpType.Shadow)
  }

  def shadowActor(name: String): ActorRef = {
    actorAddresses(configFactory.nameMapper.getId(name, CmpType.Source), CmpType.Shadow)
  }

  def globalProps(id: Int): GlobalProps = configFactory.globalProps(id)

  def globalProps(name: String): GlobalProps = configFactory.globalProps(name)

  def openMetrics(period: FiniteDuration): Unit = {
    system.actorOf(Props(classOf[InfluxdbReporter], period, this))
  }
}

class TeleporterCenterImpl(
                            val configFactory: TeleporterConfigFactory,
                            val system: ActorSystem,
                            val materializer: Materializer,
                            val addressFactory: AddressFactory,
                            val sourceFactory: ActorSourceFactory,
                            val sinkFactory: ActorSinkFactory,
                            val defaultRecoveryPoint: RecoveryPoint[Conf.Source],
                            val actorAddresses: ActorAddresses = ActorAddresses(),
                            val crontab: ActorRef,
                            val metricsRegistry: MetricRegistry = new MetricRegistry) extends TeleporterCenter

object TeleporterCenter extends PropsSupport {
  def apply()(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): TeleporterCenter = {
    val config = ConfigFactory.load()
    apply(config)
  }

  def apply(config: Config)(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): TeleporterCenter = {
    val mongoConfig = config.getConfig("teleporter.mongo")
    val client: MongoClient = MongoClient(mongoConfig.getString("url"))
    implicit val db: MongoDatabase = client.getDatabase(mongoConfig.getString("database"))
    val configFactory = new MongoTeleporterConfigFactory()
    val center = apply(configFactory, new MongoStoreRecoveryPoint())
    val metrics = config.getConfig("teleporter.metrics")
    if (metrics.getBoolean("open")) {
      center.openMetrics(Duration(metrics.getString("duration")).asInstanceOf[FiniteDuration])
    }
    center
  }

  def apply[T](configFactory: TeleporterConfigFactory, recoveryPoint: RecoveryPoint[Conf.Source])(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): TeleporterCenter = {
    new TeleporterCenterImpl(
      configFactory = configFactory,
      system = system,
      materializer = materializer,
      addressFactory = AddressFactory(configFactory),
      sourceFactory = ActorSourceFactory(configFactory),
      sinkFactory = ActorSinkFactory(configFactory),
      defaultRecoveryPoint = recoveryPoint,
      crontab = system.actorOf(CronTab.props, "stream_crontab")
    )
  }
}
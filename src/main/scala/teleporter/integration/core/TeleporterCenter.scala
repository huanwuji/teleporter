package teleporter.integration.core

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import com.markatta.akron.CronTab
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.ActorTestMessages.Ping
import teleporter.integration.cluster.instance.Brokers
import teleporter.integration.cluster.rpc.proto.Rpc.TeleporterEvent
import teleporter.integration.component.GitClient
import teleporter.integration.core.TeleporterConfig._
import teleporter.integration.core.TeleporterConfigActor.LoadAddress
import teleporter.integration.metrics.{InfluxdbReporter, MetricRegistry}
import teleporter.integration.transaction.ChunkTransaction.ChunkTxnConfig
import teleporter.integration.transaction.RecoveryPoint
import teleporter.integration.utils.EventListener

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration, _}


/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait TeleporterCenter extends LazyLogging {
  implicit final val self = this

  implicit val system: ActorSystem

  implicit val materializer: Materializer

  val instanceKey: String

  def context: TeleporterContext

  def brokers: ActorRef

  def streams: ActorRef

  def components: Components

  def configRef: ActorRef

  def defaultRecoveryPoint: RecoveryPoint[SourceConfig]

  def eventListener: EventListener[TeleporterEvent]

  def metricsRegistry: MetricRegistry

  def gitClient: GitClient

  def crontab: ActorRef

  def recoveryPoint(transactionConf: ChunkTxnConfig): RecoveryPoint[SourceConfig] = if (transactionConf.recoveryPointEnabled) defaultRecoveryPoint else RecoveryPoint.empty

  def openMetrics(key: String, period: FiniteDuration): Unit = system.actorOf(Props(classOf[InfluxdbReporter], key, period, this))

  def source[T](id: Long): Source[T, ActorRef] = components.source[T](id)

  def source[T](key: String): Source[T, ActorRef] = components.source[T](key)

  def sink[T](id: Long): Sink[T, ActorRef] = components.sink[T](id)

  def sink[T](key: String): Sink[T, ActorRef] = components.sink[T](key)

  def start(): Future[Done]
}

class TeleporterCenterImpl(val instanceKey: String, seedBrokers: String, config: Config)
                          (implicit val system: ActorSystem, val materializer: Materializer) extends TeleporterCenter {
  implicit val timeout: Timeout = 1.minute
  var _context: TeleporterContext = _
  var _brokers: ActorRef = _
  var _streams: ActorRef = _
  var _components: Components = _
  var _configRef: ActorRef = _
  var _defaultRecoveryPoint: RecoveryPoint[SourceConfig] = _
  var _metricRegistry: MetricRegistry = _
  val _eventListener = EventListener[TeleporterEvent]()

  override def start(): Future[Done] = {
    import system.dispatcher
    _context = TeleporterContext()
    _streams = Streams()
    _components = Components()
    _configRef = TeleporterConfigActor(_eventListener)
    _defaultRecoveryPoint = RecoveryPoint()
    _metricRegistry = MetricRegistry()
    val (brokerRef, connected) = Brokers(seedBrokers)
    _brokers = brokerRef
    connected.map {
      done ⇒
        val metricsConfig = config.getConfig("metrics")
        val (open, key, duration) = (metricsConfig.getBoolean("open"), metricsConfig.getString("key"), metricsConfig.getString("duration"))
        if (open) {
          //load address
          (this.configRef ? LoadAddress(key))
            .flatMap(_ ⇒ this.context.ref ? Ping /*ensure context is add to center.context*/)
            .foreach {
              _ ⇒
                logger.info(s"Metrics will open, key: $key, refresh: $duration")
                this.openMetrics(key, Duration(duration).asInstanceOf[FiniteDuration])
            }
        }
        done
    }
  }

  override def context: TeleporterContext = _context

  override def brokers: ActorRef = _brokers

  override def streams: ActorRef = _streams

  override def components: Components = _components

  override def configRef: ActorRef = _configRef

  override def defaultRecoveryPoint: RecoveryPoint[SourceConfig] = _defaultRecoveryPoint

  override def eventListener: EventListener[TeleporterEvent] = _eventListener

  override def metricsRegistry: MetricRegistry = _metricRegistry

  override def gitClient: GitClient = null

  override def crontab: ActorRef = system.actorOf(CronTab.props, "teleporter_crontab")
}

object TeleporterCenter extends LazyLogging {
  def apply(config: Config = ConfigFactory.load("instance")): TeleporterCenter = {
    implicit val system = ActorSystem("instance", config)
    implicit val mater = ActorMaterializer()

    val instanceConfig = config.getConfig("teleporter")
    val (instanceKey, seedBrokers) = (instanceConfig.getString("key"), instanceConfig.getString("brokers"))
    new TeleporterCenterImpl(instanceKey, seedBrokers, config)
  }
}
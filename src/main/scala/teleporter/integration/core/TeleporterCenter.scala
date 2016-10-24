package teleporter.integration.core

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.markatta.akron.CronTab
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.ActorTestMessages.Ping
import teleporter.integration.cluster.instance.Brokers
import teleporter.integration.cluster.instance.Brokers.OnConnected
import teleporter.integration.cluster.rpc.proto.Rpc.TeleporterEvent
import teleporter.integration.component.GitClient
import teleporter.integration.core.TeleporterConfig._
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

  val instance: String

  def context: TeleporterContext

  def brokers: ActorRef

  def streams: ActorRef

  def components: Components

  def config: TeleporterConfigService

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
}

class TeleporterCenterImpl(val instance: String)
                          (implicit val system: ActorSystem, val materializer: Materializer) extends TeleporterCenter {

  import system.dispatcher

  var _context: TeleporterContext = _
  var _brokers: ActorRef = _
  var _streams: ActorRef = _
  var _components: Components = _
  var _config: TeleporterConfigService = _
  var _defaultRecoveryPoint: RecoveryPoint[SourceConfig] = _
  var _metricRegistry: MetricRegistry = _
  val _eventListener = EventListener[TeleporterEvent]()

  def apply(): TeleporterCenter = {
    _context = TeleporterContext()
    _brokers = Brokers()
    _streams = Streams()
    _components = Components()
    _config = TeleporterConfigService(_eventListener)
    _defaultRecoveryPoint = RecoveryPoint()
    _metricRegistry = MetricRegistry()
    self
  }

  override def context: TeleporterContext = _context

  override def brokers: ActorRef = _brokers

  override def streams: ActorRef = _streams

  override def components: Components = _components

  override def config: TeleporterConfigService = _config

  override def defaultRecoveryPoint: RecoveryPoint[SourceConfig] = _defaultRecoveryPoint

  override def eventListener: EventListener[TeleporterEvent] = _eventListener

  override def metricsRegistry: MetricRegistry = _metricRegistry

  override def gitClient: GitClient = null

  override def crontab: ActorRef = system.actorOf(CronTab.props, "teleporter_crontab")
}

object TeleporterCenter extends LazyLogging {
  def apply(instance: String, config: Config)(implicit system: ActorSystem, mater: Materializer): TeleporterCenter = {
    import system.dispatcher
    implicit val timeout: Timeout = 1.minute
    val center = new TeleporterCenterImpl(instance).apply()
    val metricsConfig = config.getConfig("metrics")
    val (open, key, duration) = (metricsConfig.getBoolean("open"), metricsConfig.getString("key"), metricsConfig.getString("duration"))
    if (open) {
      // ensure connected to broker
      (center.brokers ? OnConnected).foreach {
        case fu: Future[Done] ⇒
          // success connected to broker
          fu.foreach {
            _ ⇒
              //load address
              center.config.loadAddress(key).flatMap(_ ⇒ center.context.ref ? Ping /*ensure context is add to center.context*/).foreach {
                _ ⇒
                  logger.info(s"Metrics will open, key: $key, refresh: $duration")
                  center.openMetrics(key, Duration(duration).asInstanceOf[FiniteDuration])
              }
          }
      }
    }
    center
  }
}
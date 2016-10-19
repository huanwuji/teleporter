package teleporter.integration.core

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.markatta.akron.CronTab
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.cluster.instance.Brokers
import teleporter.integration.cluster.rpc.proto.Rpc.TeleporterEvent
import teleporter.integration.component.GitClient
import teleporter.integration.core.TeleporterConfig._
import teleporter.integration.metrics.{InfluxdbReporter, MetricRegistry}
import teleporter.integration.transaction.ChunkTransaction.ChunkTxnConfig
import teleporter.integration.transaction.RecoveryPoint
import teleporter.integration.utils.EventListener

import scala.concurrent.duration.{Duration, FiniteDuration}


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
  var _context: TeleporterContext = _
  var _brokers: ActorRef = _
  var _streams: ActorRef = _
  var _components: Components = _
  var _defaultRecoveryPoint: RecoveryPoint[SourceConfig] = _
  val _eventListener = EventListener[TeleporterEvent]()

  def apply(): TeleporterCenter = {
    _context = TeleporterContext()
    _brokers = Brokers()
    _streams = Streams()
    _components = Components()
    _defaultRecoveryPoint = RecoveryPoint()
    self
  }

  override def context: TeleporterContext = _context

  override def brokers: ActorRef = _brokers

  override def streams: ActorRef = _streams

  override def components: Components = _components

  override def defaultRecoveryPoint: RecoveryPoint[SourceConfig] = _defaultRecoveryPoint

  override def eventListener: EventListener[TeleporterEvent] = _eventListener

  override def metricsRegistry: MetricRegistry = MetricRegistry()

  override def gitClient: GitClient = null

  override def crontab: ActorRef = system.actorOf(CronTab.props, "teleporter_crontab")
}

object TeleporterCenter {
  def apply(instance: String, config: Config)(implicit system: ActorSystem, mater: Materializer): TeleporterCenter = {
    val center = new TeleporterCenterImpl(instance).apply()
    val metricsConfig = config.getConfig("metrics")
    val (open, duration, key) = (metricsConfig.getBoolean("open"), metricsConfig.getString("key"), metricsConfig.getString("duration"))
    if (open) {
      center.openMetrics(key, Duration(duration).asInstanceOf[FiniteDuration])
    }
    center
  }
}
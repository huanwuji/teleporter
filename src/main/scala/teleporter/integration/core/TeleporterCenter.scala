package teleporter.integration.core

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.ExecutionContexts
import akka.pattern._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, Supervision}
import akka.util.Timeout
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.ActorTestMessages.Ping
import teleporter.integration.cluster.instance.Brokers
import teleporter.integration.cluster.rpc.{EventBody, TeleporterEvent}
import teleporter.integration.component.kv.KVOperator
import teleporter.integration.component.kv.leveldb.{LevelDBs, LevelTable}
import teleporter.integration.component.kv.rocksdb.{RocksDBs, RocksTable}
import teleporter.integration.component.{Cron, GitClient}
import teleporter.integration.concurrent.SizeScaleThreadPoolExecutor
import teleporter.integration.core.TeleporterConfigActor.LoadAddress
import teleporter.integration.metrics.{InfluxdbReporter, MetricRegistry}
import teleporter.integration.utils.{EventListener, MapBean}

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}


/**
  * date 2015/8/3.
  *
  * @author daikui
  */
trait TeleporterCenter extends Logging {
  implicit final val self: TeleporterCenter = this

  implicit val system: ActorSystem

  implicit val materializer: Materializer
  implicit val defaultExecutionContext: ExecutionContext = system.dispatcher
  val blockExecutionContext: ExecutionContext = ExecutionContexts.fromExecutor(new SizeScaleThreadPoolExecutor(5, 500, 20, 60L, TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable], new ThreadFactoryBuilder().setNameFormat("teleporter-bio-thread-%d").build()))

  val instanceKey: String

  def context: TeleporterContext

  def brokers: ActorRef

  def streams: ActorRef

  def configRef: ActorRef

  def defaultSourceSavePoint: SavePoint[MapBean]

  def eventListener: EventListener[TeleporterEvent[_ <: EventBody]]

  def client: TeleporterConfigClient

  def metricsRegistry: MetricRegistry

  def gitClient: GitClient

  def crontab: ActorRef

  def openMetrics(key: String, period: FiniteDuration): Unit = system.actorOf(Props(classOf[InfluxdbReporter], key, period, this))

  def localStatusRef: ActorRef

  def start(): Future[Done]
}

class TeleporterCenterImpl(val instanceKey: String, seedBrokers: String, config: Config, kVOperator: KVOperator)
                          (implicit val system: ActorSystem, val materializer: Materializer) extends TeleporterCenter {
  implicit val timeout: Timeout = 10.seconds
  var _context: TeleporterContext = _
  var _brokers: ActorRef = _
  var _streams: ActorRef = _
  var _configRef: ActorRef = _
  var _defaultRecoveryPoint: SavePoint[MapBean] = _
  var _metricRegistry: MetricRegistry = _
  val _eventListener: EventListener[TeleporterEvent[_ <: EventBody]] = EventListener[TeleporterEvent[_ <: EventBody]]()
  val _teleporterConfigClient = TeleporterConfigClient()
  var _localStatusRef: ActorRef = _
  var _crontab: ActorRef = _

  override def start(): Future[Done] = {
    _context = TeleporterContext()
    _streams = Streams()
    _configRef = TeleporterConfigActor(_eventListener)
    _defaultRecoveryPoint = SavePoint.sourceSavePoint()
    _metricRegistry = MetricRegistry()
    _localStatusRef = system.actorOf(Props(classOf[LocalStatusActor], config.getString("status-file"), this))
    val (brokerRef, connected) = Brokers(seedBrokers)
    _brokers = brokerRef
    _crontab = Cron.cronRef("teleporter_crontab")
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

  override def localStatusRef: ActorRef = _localStatusRef

  override def context: TeleporterContext = _context

  override def brokers: ActorRef = _brokers

  override def streams: ActorRef = _streams

  override def configRef: ActorRef = _configRef

  override def defaultSourceSavePoint: SavePoint[MapBean] = _defaultRecoveryPoint

  override def eventListener: EventListener[TeleporterEvent[_ <: EventBody]] = _eventListener

  override def client: TeleporterConfigClient = _teleporterConfigClient

  override def metricsRegistry: MetricRegistry = _metricRegistry

  override def gitClient: GitClient = null

  override def crontab: ActorRef = _crontab
}

object TeleporterCenter extends Logging {
  def apply(config: Config = ConfigFactory.load("instance")): TeleporterCenter = {
    implicit val system = ActorSystem("instance", config)
    val decider: Supervision.Decider = {
      case e: Exception ⇒ logger.warn(e.getMessage, e); Supervision.Stop
    }
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val instanceConfig = config.getConfig("teleporter")
    val (instanceKey, seedBrokers) = (instanceConfig.getString("key"), instanceConfig.getString("brokers"))
    val localStorageConfig = instanceConfig.getConfig("localStorage")
    val localStorage = localStorageConfig.getString("type") match {
      case "leveldb" ⇒ LevelTable(LevelDBs("teleporter", localStorageConfig.getString("path")), "localStorage")
      case "rocksdb" ⇒ RocksTable(RocksDBs("teleporter", localStorageConfig.getString("path")), "localStorage")
    }
    new TeleporterCenterImpl(instanceKey, seedBrokers, instanceConfig, localStorage)
  }
}
package teleporter.integration.core

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.{MetricRegistry, Slf4jReporter}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.reactivestreams.{Publisher, Subscriber}
import teleporter.integration.conf.Conf
import teleporter.integration.core.conf.{MongoTeleporterConfigFactory, TeleporterConfigFactory}
import teleporter.integration.transaction._

import scala.concurrent.ExecutionContext

/**
 * date 2015/8/3.
 * @author daikui
 */
trait TeleporterCenter extends LazyLogging {
  val configFactory: TeleporterConfigFactory

  implicit var actorSystem: ActorSystem = null
  implicit var materializer: Materializer = null

  var addressFactory: AddressFactory = null

  var sourceFactory: ActorSourceFactory = null

  var sinkFactory: ActorSinkFactory = null

  var defaultRecoveryPoint: RecoveryPoint[Conf.Source] = null

  var metricRegistry: MetricRegistry = null

  val actorAddresses: ActorAddresses = ActorAddresses()

  def addMaterializer(materializer: Materializer): TeleporterCenter = {
    this.materializer = materializer
    this
  }

  def addActorSystem(actorSystem: ActorSystem): TeleporterCenter = {
    this.actorSystem = actorSystem
    this
  }

  def addAddressFactory(addressFactory: AddressFactory): TeleporterCenter = {
    this.addressFactory = addressFactory
    this
  }

  def addSourceFactory(sourceFactory: ActorSourceFactory): TeleporterCenter = {
    this.sourceFactory = sourceFactory
    this
  }

  def addSinkFactory(sinkFactory: ActorSinkFactory): TeleporterCenter = {
    this.sinkFactory = sinkFactory
    this
  }

  def defaultRecoveryPoint(recoveryPoint: RecoveryPoint[Conf.Source]): TeleporterCenter = {
    this.defaultRecoveryPoint = recoveryPoint
    this
  }

  def addressing[T](conf: Conf.Source): Option[T] = addressFactory.addressing(conf)

  def addressing[T](id: Int): Option[T] = {
    addressFactory.addressing(id)
  }


  def addressing[T](name: String): Option[T] = {
    addressFactory.addressing(Conf.generateId(name))
  }

  def removeAddress[T](id: Int): Unit = addressFactory.close(id)

  def removeAddress[T](conf: Conf.Source): Unit = addressFactory.close(conf)

  def publisher[T](id: Int): Publisher[T] = sourceFactory.build(id)

  def subscriber[T](id: Int): Subscriber[T] = sinkFactory.build(id)

  def task(id: Int): Conf.Task = configFactory.task(id)

  def source[T](id: Int): Source[T, Unit] = Source(publisher(id))

  def sink[T](id: Int): Sink[T, Unit] = Sink(subscriber(id))

  def task(name: String): Conf.Task = configFactory.task(Conf.generateId(name))

  def source[T](name: String): Source[T, Unit] = Source(publisher(Conf.generateId(name)))

  def sink[T](name: String): Sink[T, Unit] = Sink(subscriber(Conf.generateId(name)))

  def actor(id: Int): ActorAddress = {
    actorAddresses(id)
  }

  def actor(name: String): ActorAddress = {
    actorAddresses(Conf.generateId(name))
  }

  def openMetrics(): TeleporterCenter = {
    this.metricRegistry = new MetricRegistry
    Slf4jReporter.forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build().start(1, TimeUnit.SECONDS)
    this
    //    val client = addressing[InfluxdbClient]("teleporter:metrics").get
    //    actorSystem.actorOf(Props(classOf[InfluxdbMetricsReporter], 1.minutes, this.metricRegistry, client), "teleporter:metrics:report")
  }
}

class TeleporterCenterImpl(override val configFactory: TeleporterConfigFactory) extends TeleporterCenter

object TeleporterCenter {
  def apply()(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): TeleporterCenter = {
    val config = ConfigFactory.load()
    apply(config)
  }

  def apply(config: Config)(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): TeleporterCenter = {
    val mongoConfig = config.getConfig("teleporter.mongo")
    val client: MongoClient = MongoClient(mongoConfig.getString("url"))
    implicit val db: MongoDatabase = client.getDatabase(mongoConfig.getString("database"))
    val configFactory = new MongoTeleporterConfigFactory()
    implicit val center = new TeleporterCenterImpl(configFactory)
    center
      .addMaterializer(materializer)
      .addActorSystem(system)
      .addAddressFactory(AddressFactory(configFactory))
      .addSourceFactory(ActorSourceFactory(configFactory))
      .addSinkFactory(ActorSinkFactory(configFactory))
      .defaultRecoveryPoint(new MongoStoreRecoveryPoint())
  }

  def apply[T](configFactory: TeleporterConfigFactory, recoveryPoint: RecoveryPoint[Conf.Source])(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): TeleporterCenter = {
    implicit val center = new TeleporterCenterImpl(configFactory)
    center
      .addMaterializer(materializer)
      .addActorSystem(system)
      .addAddressFactory(AddressFactory(configFactory))
      .addSourceFactory(ActorSourceFactory(configFactory))
      .addSinkFactory(ActorSinkFactory(configFactory))
      .defaultRecoveryPoint(recoveryPoint)
  }
}
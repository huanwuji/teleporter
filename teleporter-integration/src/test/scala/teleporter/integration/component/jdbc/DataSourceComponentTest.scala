package teleporter.integration.component.jdbc

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite
import teleporter.integration.component.{JdbcRecord, TeleporterJdbcMessage}
import teleporter.integration.conf.Conf
import teleporter.integration.conf.Conf.{Address, Source}
import teleporter.integration.core.conf.{LocalTeleporterConfigFactory, LocalTeleporterConfigStore}
import teleporter.integration.core.{TeleporterCenter, TeleporterMessage}
import teleporter.integration.transaction.LocalStoreRecoveryPoint

/**
 * date 2015/8/3.
 * @author daikui
 */
class DataSourceComponentTest extends FunSuite with SqlSupport with LazyLogging {
  val decider: Supervision.Decider = {
    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Resume
  }
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  import system.dispatcher

  val localStore = new LocalTeleporterConfigStore()
  val localTeleporterConfigFactory = new LocalTeleporterConfigFactory(localStore)
  val recoveryPoint = new LocalStoreRecoveryPoint(localStore)

  test("dataSource component") {
    val center = TeleporterCenter(localTeleporterConfigFactory, recoveryPoint)
    val addressName = "sh:local:etl"
    val address = Address(
      taskId = None,
      category = "dataSource",
      name = addressName,
      props = Map("jdbcUrl" → "jdbc:mysql://localhost:3306/etl", "username" → "root", "password" → "root")
    )
    val sourceName = "sh:local:etl:base"
    val source = Source(taskId = None,
      addressId = Option(address.id),
      category = "dataSource",
      name = sourceName,
      props = Map(
        "sql" → s"select * from base order by tid limit {offset},{pageSize}",
        "page" → "0",
        "pageSize" → "50",
        "maxPage" → "10",
        "start" → "2015-12-08T11:00:00",
        "deadline" → "now",
        "period" → "1.min"
      )
    )
    val sinkName = "sh:local:etl:base_test"
    val sink = Conf.Sink(
      taskId = None,
      addressId = Option(address.id),
      category = "dataSource",
      name = sinkName,
      props = Map()
    )
    localStore.addresses = localStore.addresses + (address.id → address)
    localStore.sources = localStore.sources + (source.id → source)
    localStore.sinks = localStore.sinks + (sink.id → sink)
    center.source[TeleporterJdbcMessage](sourceName)
      .map {
        x ⇒ TeleporterMessage[JdbcRecord](id = x.id, sourceRef = x.sourceRef, data = Seq(Upsert(update("base_test","tid", x.data), insert("base_test", x.data))))
      }.to(center.sink(sinkName)).run()

    Thread.sleep(190000)
  }
}
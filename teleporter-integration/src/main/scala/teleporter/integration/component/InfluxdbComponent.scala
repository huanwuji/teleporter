package teleporter.integration.component

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.Influxdb.InfluxdbConnections
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.{Address, AddressBuilder, TeleporterCenter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Author: kui.dai
 * Date: 2015/12/10.
 */
object Influxdb {
  type InfluxdbConnections = Flow[(HttpRequest, String), (Try[HttpResponse], String), HostConnectionPool]
}

class InfluxdbAddressBuilder(override val conf: Conf.Address)(implicit override val teleporterCenter: TeleporterCenter) extends AddressBuilder[InfluxdbClient]
with PropsSupport with LazyLogging {

  import teleporterCenter.{actorSystem, materializer}

  override def build: Address[InfluxdbClient] = {
    val props = conf.props
    val host = getStringOpt(props, "host").get
    val port = getIntOrElse(props, "port", 8080)
    val dbName = getStringOpt(props, "db").get
    val connectionPool = Http().cachedHostConnectionPool[String](host = host, port = port)
    new Address[InfluxdbClient] {
      override val _conf: Conf.Address = conf
      override val client: InfluxdbClient = InfluxdbClient(connectionPool, dbName)

      override def close(): Unit = Http().shutdownAllConnectionPools()
    }
  }
}

case class InfluxDto(key: String, data: Map[String, Any], timestamp: Long)

trait InfluxdbClient extends AsyncRepository[InfluxDto, String, InfluxdbClient] with LazyLogging {
  val connections: Flow[(HttpRequest, String), (Try[HttpResponse], String), HostConnectionPool]
  implicit val materializer: Materializer
  val dbName: String

  override def save(dto: InfluxDto)(implicit client: InfluxdbClient, ex: ExecutionContext): Future[Int] = {
    Source.single(RequestBuilding.Post(s"/write?db=$dbName") → writeData(dto.key, dto.data, dto.timestamp))
      .via(connections).runWith(Sink.head).map({ x ⇒ 1 })
  }

  private def writeData(key: String, data: Map[String, Any], timestamp: Long): String = {
    val fields = data.mapValues {
      case num: Int with Long with Short ⇒ s"$num\\i"
      case precision: Float with Double ⇒ s"$precision"
      case str: String ⇒ s""""$str""""
      case _ ⇒ logger.warn("UnSupport influxdb write data type")
    }.map(entry ⇒ s"${entry._1}=${entry._2}").mkString(",")
    s"$key $fields $timestamp"
  }
}

class InfluxdbClientImpl(override val connections: InfluxdbConnections, override val dbName: String)(implicit override val materializer: Materializer) extends InfluxdbClient

object InfluxdbClient {
  def apply(connectionPool: InfluxdbConnections, dbName: String)(implicit materializer: Materializer): InfluxdbClient = new InfluxdbClientImpl(connectionPool, dbName)
}

class InfluxdbComponent {

}

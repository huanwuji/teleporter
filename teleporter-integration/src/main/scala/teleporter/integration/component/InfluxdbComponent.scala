package teleporter.integration.component

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.Influxdb.InfluxdbConnections
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.{Address, AddressBuilder, TeleporterCenter}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Author: kui.dai
 * Date: 2015/12/10.
 */
object Influxdb {
  type InfluxdbConnections = Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]]
}

class InfluxdbAddress(val conf: Conf.Address, val client: InfluxdbClient)(implicit system: ActorSystem, materializer: Materializer) extends Address[InfluxdbClient] {
  override def close(): Unit = Http().shutdownAllConnectionPools()
}

class InfluxdbAddressBuilder(override val conf: Conf.Address)(implicit override val center: TeleporterCenter) extends AddressBuilder[InfluxdbClient]
with PropsSupport with LazyLogging {

  import center.{materializer, system}

  override def build: Address[InfluxdbClient] = {
    val cmpConfig = cmpProps(conf.props)
    val host = getStringOpt(cmpConfig, "host").get
    val port = getIntOrElse(cmpConfig, "port", 8080)
    val dbName = getStringOpt(cmpConfig, "db").get
    val outgoingConnection = Http().outgoingConnection(host = host, port = port)
    new InfluxdbAddress(conf, InfluxdbClient(outgoingConnection, dbName))
  }
}

case class InfluxDto(key: String, data: Map[String, Any], timestamp: Long)

trait InfluxdbClient extends AsyncRepository[InfluxDto, String, InfluxdbClient] with LazyLogging {
  val connections: InfluxdbConnections
  implicit val materializer: Materializer
  val dbName: String

  override def save(dto: InfluxDto)(implicit client: InfluxdbClient, ex: ExecutionContext): Future[Int] = {
    Source.single(RequestBuilding.Post(s"/write?db=$dbName", writeData(dto.key, dto.data, dto.timestamp)))
      .via(connections).runWith(Sink.head).map {
      resp ⇒
        resp.status match {
          case StatusCodes.OK | StatusCodes.NoContent ⇒ 1
          case _ ⇒ logger.error(resp.toString); 0
        }
    }
  }

  private def writeData(key: String, data: Map[String, Any], timestamp: Long): String = {
    val fields = data.mapValues {
      case x: Int ⇒ s"${x}i"
      case x: Short ⇒ s"${x}i"
      case x: Long ⇒ s"${x}i"
      case x: Float ⇒ s"$x"
      case x: Double ⇒ s"$x"
      case str: String ⇒ s""""$str""""
      case x ⇒ logger.warn(s"$x, UnSupport influxdb write data type" + x.getClass)
    }.map(entry ⇒ s"${entry._1}=${entry._2}").mkString(",")
    s"$key $fields"
  }
}

class InfluxdbClientImpl(override val connections: InfluxdbConnections, override val dbName: String)(implicit override val materializer: Materializer) extends InfluxdbClient

object InfluxdbClient {
  def apply(connection: InfluxdbConnections, dbName: String)(implicit materializer: Materializer): InfluxdbClient = new InfluxdbClientImpl(connection, dbName)
}

class InfluxdbComponent {

}
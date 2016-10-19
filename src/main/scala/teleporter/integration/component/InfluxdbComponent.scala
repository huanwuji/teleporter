package teleporter.integration.component

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration._
import teleporter.integration.component.Influxdb.InfluxdbServer
import teleporter.integration.core.{AddressContext, AddressMetadata, AutoCloseClientRef}
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.SimpleHttpClient

import scala.concurrent.{ExecutionContext, Future}

/**
  * Author: kui.dai
  * Date: 2015/12/10.
  */
object Influxdb {
  type InfluxdbServer = Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]]
}

trait InfluxdbMetadata {
  val FHost = "host"
  val FPort = "port"
  val FDb = "db"
}

case class InfluxDto(key: String, data: Map[String, Any], timestamp: Long)

trait InfluxdbClient extends SimpleHttpClient with AutoCloseable with LazyLogging {
  implicit val influxServer: InfluxdbServer
  val dbName: String

  def save(dto: InfluxDto)(implicit mater: Materializer, ec: ExecutionContext): Future[String] = {
    simpleRequest[String](RequestBuilding.Post(s"/write?db=$dbName", writeData(dto.key, dto.data, dto.timestamp)))
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

  override def close(): Unit = {}
}

class InfluxdbClientImpl(override val influxServer: InfluxdbServer, override val dbName: String) extends InfluxdbClient

object InfluxdbClient {
  def apply(connection: InfluxdbServer, dbName: String): InfluxdbClient = new InfluxdbClientImpl(connection, dbName)
}

object InfluxdbComponent extends InfluxdbMetadata with AddressMetadata {
  val influxdb: ClientApply[InfluxdbClient] = (key, center) ⇒ {
    import center.system
    implicit val config = center.context.getContext[AddressContext](key).config
    val clientConfig = lnsClient
    val host = clientConfig[String](FHost)
    val port = clientConfig[Int](FPort)
    val dbName = clientConfig[String](FDb)
    val outgoingConnection = Http().outgoingConnection(host = host, port = port)
    AutoCloseClientRef(key, InfluxdbClient(outgoingConnection, dbName))
  }
}
package teleporter.integration.component

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.component.Influxdb.InfluxdbServer
import teleporter.integration.core.{AddressContext, AddressMetaBean, AutoCloseClientRef, TeleporterCenter}
import teleporter.integration.metrics.Metrics.Measurement
import teleporter.integration.utils.SimpleHttpClient

import scala.concurrent.{ExecutionContext, Future}

/**
  * Author: kui.dai
  * Date: 2015/12/10.
  */
object Influxdb {
  type InfluxdbServer = Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]]

  def address(key: String)(implicit center: TeleporterCenter): AutoCloseClientRef[InfluxdbClient] = {
    import center.system
    val config = center.context.getContext[AddressContext](key).config.mapTo[InfluxdbMetaBean]
    val outgoingConnection = Http().outgoingConnection(host = config.host, port = config.port)
    AutoCloseClientRef(key, InfluxdbClient(outgoingConnection, config.db))
  }
}

class InfluxdbMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {
  val FHost = "host"
  val FPort = "port"
  val FDb = "db"

  def host: String = client[String](FHost)

  def port: Int = client[Int](FPort)

  def db: String = client[String](FDb)
}

case class InfluxDto(measurement: Measurement, data: Map[String, Any], timestamp: Long)

trait InfluxdbClient extends SimpleHttpClient with AutoCloseable with Logging {
  implicit val influxServer: InfluxdbServer
  val dbName: String

  def save(dto: InfluxDto)(implicit mater: Materializer, ec: ExecutionContext): Future[String] = {
    simpleRequest[String](RequestBuilding.Post(s"/write?db=$dbName", writeData(dto.measurement, dto.data, dto.timestamp)))
  }

  private def writeData(measurement: Measurement, data: Map[String, Any], timestamp: Long): String = {
    val fields = data.mapValues {
      case x: Int ⇒ s"${x}i"
      case x: Short ⇒ s"${x}i"
      case x: Long ⇒ s"${x}i"
      case x: Float ⇒ s"$x"
      case x: Double ⇒ s"$x"
      case str: String ⇒ s""""$str""""
      case x ⇒ logger.warn(s"$x, UnSupport influxdb write data type" + x.getClass)
    }.map(entry ⇒ s"${entry._1}=${entry._2}").mkString(",")
    s"${measurement.str} $fields"
  }

  override def close(): Unit = {}
}

class InfluxdbClientImpl(override val influxServer: InfluxdbServer, override val dbName: String) extends InfluxdbClient

object InfluxdbClient {
  def apply(connection: InfluxdbServer, dbName: String): InfluxdbClient = new InfluxdbClientImpl(connection, dbName)
}
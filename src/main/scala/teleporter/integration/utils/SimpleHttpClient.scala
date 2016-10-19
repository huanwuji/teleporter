package teleporter.integration.utils

import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, native}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Author: kui.dai
  * Date: 2016/6/28.
  */
trait SimpleHttpClient extends LazyLogging {

  import Json4sSupport._

  implicit val serialization = native.Serialization
  implicit val formats = DefaultFormats

  def simpleRequest[T](request: HttpRequest)(implicit server: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]], mater: Materializer, ec: ExecutionContext, m: Manifest[T]): Future[T] = {
    Source.single(request)
      .via(server)
      .runWith(Sink.head).flatMap {
      resp ⇒
        resp.status match {
          case StatusCodes.OK ⇒
            m match {
              case _ if m.runtimeClass.getSimpleName == "String" ⇒ Future.successful(resp.entity.asInstanceOf[T])
              case _ ⇒ Unmarshal(resp.entity).to[T]
            }
          case _ ⇒ logger.error(resp.toString()); throw new RuntimeException(s"Network error, statusCode:${resp.status}")
        }
    }
  }
}

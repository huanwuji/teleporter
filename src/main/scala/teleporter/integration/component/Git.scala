package teleporter.integration.component

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.google.common.cache.CacheBuilder
import teleporter.integration.ClientApply
import teleporter.integration.core.{AddressContext, AddressMetaBean, AutoCloseClientRef}
import teleporter.integration.utils.SimpleHttpClient

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by kui.dai on 2016/7/7.
  */
object GitMetadataBean {
  val FHost = "host"
  val FPort = "port"
  val FUsername = "username"
  val FPassword = "password"
}

class GitMetadataBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import GitMetadataBean._

  def host: String = client[String](FHost)

  def port: Int = client[Int](FPort)

  def username: String = client[String](FUsername)

  def password: String = client[String](FPassword)
}

case class GitClient(username: String, password: String)
                    (implicit gitServer: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]])
  extends SimpleHttpClient with AutoCloseable {

  private val requestCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build[String, String]()

  def cacheContent(path: String)(implicit mater: Materializer, ec: ExecutionContext): Future[String] = {
    requestCache.getIfPresent(path) match {
      case null ⇒ content(path)
      case value ⇒ Future.successful(value)
    }
  }

  def content(path: String)(implicit mater: Materializer, ec: ExecutionContext): Future[String] = {
    simpleRequest[String](RequestBuilding.Get(path)
      .addHeader(Authorization(BasicHttpCredentials(username, password))))
  }

  override def close(): Unit = {}
}

object GitComponent {
  val git: ClientApply = (key, center) ⇒ {
    import center.system
    val addressContext = center.context.getContext[AddressContext](key)
    val config = addressContext.config.mapTo[GitMetadataBean]
    implicit val outgoingConnection = Http().outgoingConnection(host = config.host, port = config.port)
    AutoCloseClientRef(key, GitClient(config.username, config.password))
  }
}
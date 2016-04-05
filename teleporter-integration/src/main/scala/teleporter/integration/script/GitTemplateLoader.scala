package teleporter.integration.script

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.google.common.cache.CacheBuilder
import teleporter.integration.conf.PropsSupport
import teleporter.integration.core.TeleporterCenter

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Author: kui.dai
 * Date: 2016/3/22.
 */
trait GitTemplateLoader extends PropsSupport {
  def log: LoggingAdapter

  val center: TeleporterCenter

  private val templateCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build[String,String]()

  private val gitProps = center.globalProps("git:template")

  //      http://git.yunat.com/projects/ETL/repos/teleporter/browse/teleporter-template/src/main/scala/teleporter/template/sh/local_ct_to_ccmstest.scala?at=develop&raw
  def loadTemplate(uri: String)(implicit mater: Materializer, system: ActorSystem, ec: ExecutionContext): String = {
    val path = uri.stripPrefix("git:")
    val host = getStringOpt(gitProps.props, "host").get
    val pathTemplate = getStringOpt(gitProps.props, "pathTemplate").get
    val username = getStringOpt(gitProps.props, "username").get
    val password = getStringOpt(gitProps.props, "password").get
    templateCache.getIfPresent(path) match {
      case null ⇒
        val resp = Source.single(RequestBuilding.Get(String.format(pathTemplate, path)).addHeader(Authorization(BasicHttpCredentials(username, password))))
          .via(Http().outgoingConnection(host)).runWith(Sink.head)
        val result = resp.flatMap {
          resp ⇒
            resp.status match {
              case StatusCodes.OK ⇒ Unmarshal(resp.entity).to[String].map {
                tmpl ⇒ templateCache.put(path, tmpl); tmpl
              }
              case x ⇒ log.info(x.toString()); Future.failed(new RuntimeException(s"request error, $x"))
            }
        }
        Await.result(result, 1.minutes)
      case s ⇒ s
    }
  }
}

package teleporter.integration.cluster.broker.http

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import ch.megard.akka.http.cors.CorsDirectives._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport._
import org.apache.logging.log4j.scala.Logging
import org.json4s.{DefaultFormats, native}
import teleporter.integration.cluster.broker.ConfigNotify.{Remove, Upsert}
import teleporter.integration.cluster.broker.PersistentProtocol.{AtomicKeyValue, KeyValue}
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.cluster.broker.tcp.ConnectionKeeper
import teleporter.integration.cluster.rpc.EventBody.LogTailRequest
import teleporter.integration.cluster.rpc.fbs.{EventStatus, EventType, Role}
import teleporter.integration.cluster.rpc.{EventBody, TeleporterEvent}
import teleporter.integration.utils.EventListener

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/15.
  */
object HttpServer extends Logging {
  def apply(host: String, port: Int,
            configNotify: ActorRef,
            configService: PersistentService,
            runtimeService: PersistentService,
            connectionKeepers: TrieMap[String, ConnectionKeeper],
            eventListener: EventListener[TeleporterEvent[_ <: EventBody]])(implicit mater: ActorMaterializer): Unit = {
    implicit val system = mater.system
    import system.dispatcher
    implicit val serialization = native.Serialization
    implicit val formats = DefaultFormats
    val route =
      pathPrefix("ui") {
        getFromResourceDirectory("ui")
      } ~ cors() {
        pathPrefix("config") {
          (path("atomic") & post & entity(as[AtomicKeyValue])) {
            atomicKY ⇒
              configService.atomicPut(atomicKY.key, atomicKY.expect, atomicKY.update)
              complete(StatusCodes.OK)
          } ~ (path("range") & get & parameters('key, 'start.as[Int].?, 'limit.as[Int].?)) {
            (key, start, limit) ⇒
              complete(configService.regexRange(key, start.getOrElse(0), limit.getOrElse(Int.MaxValue)))
          } ~ (path("id") & get) {
            complete(Map("id" → configService.id()))
          } ~ (path("notify") & get & parameter('key)) { key ⇒
            configNotify ! Upsert(key)
            complete(StatusCodes.OK)
          } ~ (get & parameter('key)) {
            key ⇒ complete(configService.apply(key))
          } ~ (post & entity(as[KeyValue])) {
            kv ⇒ complete(configService.put(kv.key, kv.value))
          } ~ (delete & parameter('key)) {
            key ⇒
              configNotify ! Remove(key)
              configService.delete(key)
              complete(StatusCodes.OK)
          }
        } ~ pathPrefix("runtime") {
          (path("range") & get & parameters('key, 'start.as[Int].?, 'limit.as[Int].?)) {
            (key, start, limit) ⇒ {
              complete(runtimeService.regexRange(key, start.getOrElse(0), limit.getOrElse(Int.MaxValue)))
            }
          } ~ (get & parameter('key)) {
            key ⇒ complete(runtimeService.get(key))
          } ~ (delete & parameter('key)) {
            key ⇒
              runtimeService.delete(key)
              complete(StatusCodes.OK)
          }
        } ~ (path("log") & parameter('instance)) {
          instance ⇒
            connectionKeepers.get(instance) match {
              case Some(keeper) ⇒
                var sourceRef = ActorRef.noSender
                val source = Source.actorRef[TextMessage](100, OverflowStrategy.fail).mapMaterializedValue(sourceRef = _)
                val logFlow = Flow.fromSinkAndSource[Message, Message](Sink.foreach {
                  case tm: TextMessage ⇒
                    val (_, fu) = eventListener.asyncEvent({ seqNr ⇒
                      tm.textStream.runForeach { txt ⇒
                        keeper.senderRef ! TeleporterEvent.request(seqNr = seqNr, eventType = EventType.LogTail,
                          body = LogTailRequest(request = 1, cmd = txt))
                      }
                    }, seqNr ⇒ Success(TeleporterEvent(seqNr = seqNr,
                      eventType = EventType.LogTail,
                      status = EventStatus.Failure,
                      role = Role.Response,
                      body = Some(EventBody.Empty.empty))))
                    fu.onComplete {
                      case Success(event) ⇒
                        val logResponse = event.toBody[EventBody.LogTailResponse]
                        sourceRef ! TextMessage(logResponse.line)
                      case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
                    }
                  case bm: BinaryMessage ⇒
                    bm.dataStream.runWith(Sink.ignore)
                    Nil
                }, source)
                handleWebSocketMessages(logFlow)
              case None ⇒
                val flow = Flow[Message].map(_ ⇒ TextMessage("Instance not Found, It's disconnection or not register"))
                handleWebSocketMessages(flow)
            }
        } ~ path("ping") {
          complete("pong")
        } ~ extractUnmatchedPath {
          _ ⇒ redirect(Uri("/ui/index.html"), StatusCodes.PermanentRedirect)
        }
      }
    Http().bindAndHandle(handler = route, interface = host, port = port)
  }
}
package teleporter.integration.cluster.broker.http

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, native}
import teleporter.integration.cluster.broker.ConfigNotify.{Remove, Upsert}
import teleporter.integration.cluster.broker.PersistentProtocol.{AtomicKeyValue, KeyValue}
import teleporter.integration.cluster.broker.PersistentService
import teleporter.integration.cluster.broker.tcp.ConnectionKeeper
import teleporter.integration.cluster.rpc.proto.Rpc.{EventType, TeleporterEvent}
import teleporter.integration.cluster.rpc.proto.broker.Broker.LogRequest
import teleporter.integration.cluster.rpc.proto.instance.Instance.LogResponse
import teleporter.integration.utils.EventListener

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/15.
  */
object HttpServer extends LazyLogging {

  import Json4sSupport._

  def apply(host: String, port: Int,
            configNotify: ActorRef,
            configService: PersistentService,
            runtimeService: PersistentService,
            connectionKeepers: TrieMap[String, ConnectionKeeper],
            eventListener: EventListener[TeleporterEvent])(implicit mater: ActorMaterializer): Unit = {
    implicit val system = mater.system
    import system.dispatcher
    implicit val serialization = native.Serialization
    implicit val formats = DefaultFormats
    val route =
      pathPrefix("ui") {
        getFromResourceDirectory("ui")
      } ~ pathPrefix("config") {
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
          kv ⇒
            configService.put(kv.key, kv.value)
            complete(StatusCodes.OK)
        } ~ (delete & parameter('key)) {
          key ⇒
            configService.delete(key)
            configNotify ! Remove(key)
            complete(StatusCodes.OK)
        }
      } ~ pathPrefix("runtime") {
        (path("range") & get & parameters('key, 'start.as[Int].?, 'limit.as[Int].?)) {
          (key, start, limit) ⇒ {
            complete(runtimeService.regexRange(key, start.getOrElse(0), limit.getOrElse(Int.MaxValue)))
          }
        } ~ (get & parameter('key)) {
          key ⇒ complete(runtimeService.apply(key))
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
                      keeper.senderRef ! TeleporterEvent.newBuilder()
                        .setSeqNr(seqNr)
                        .setRole(TeleporterEvent.Role.SERVER)
                        .setType(EventType.LogRequest)
                        .setBody(
                          LogRequest.newBuilder().setCmd(txt).setRequest(1).build().toByteString
                        ).build()
                    }
                  }, seqNr ⇒ Success(TeleporterEvent.newBuilder().setSeqNr(seqNr).build()))
                  fu.onComplete {
                    case Success(event) ⇒
                      val logResponse = LogResponse.parseFrom(event.getBody)
                      sourceRef ! TextMessage(logResponse.getLine)
                    case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
                  }
                case bm: BinaryMessage ⇒
                  bm.dataStream.runWith(Sink.ignore)
                  Nil
              }, source)
              handleWebSocketMessages(logFlow)
            case None ⇒
              val flow = Flow[Message].map(m ⇒ TextMessage("Instance not Found, It's disconnection or not register"))
              handleWebSocketMessages(flow)
          }
      } ~ path("ping") {
        complete("pong")
      } ~ extractUnmatchedPath {
        path ⇒ redirect(Uri("/ui/index.html"), StatusCodes.PermanentRedirect)
      }
    Http().bindAndHandle(handler = route, interface = host, port = port)
  }
}
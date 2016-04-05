package teleporter.task.tests.tcp

import java.io.FileInputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.coding.Gzip
import akka.stream._
import akka.stream.io._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.{KafkaProtoDataList, TcpComponent, TeleporterKafkaMessage, TlsHelper}
import teleporter.integration.core._

/**
 * date 2015/8/3.
 * @author daikui
 */
object KafkaTcpTlsServerBoot extends App with LazyLogging {

  import TlsHelper._

  try {
    implicit val cipherSuites = defaultCipherSuites
    implicit val sslContext = initSslContext(
      "test".toCharArray,
      //      getClass.getResourceAsStream("/security/keystore.jks"),
      //      getClass.getResourceAsStream("/security/truststore.jks")
      new FileInputStream("D:\\git\\etl\\teleporter\\teleporter-task-tests\\config\\security\\keystore.jks"),
      new FileInputStream("D:\\git\\etl\\teleporter\\teleporter-task-tests\\config\\security\\truststore.jks")
    )

    val decider: Supervision.Decider = {
      case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Resume
    }
    implicit val system = ActorSystem()
    implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    import system.dispatcher

    val center = TeleporterCenter()
    Tcp().bind("0.0.0.0", 9092).to(Sink.foreach {
      connection ⇒
        if (connection.remoteAddress.toString.startsWith("/100.97")) {
          connection.handleWith(Flow[ByteString])
        } else {
          logger.info(s"New connection from: ${connection.remoteAddress}")
          val serverLogic =
            Flow.fromGraph(
              FlowGraph.create() { implicit builder =>
                import FlowGraph.Implicits._
                val merge = builder.add(Merge[TeleporterKafkaMessage](1))
                val send = Flow[TeleporterKafkaMessage]
                  .buffer(5, OverflowStrategy.backpressure)
                  .grouped(2)
                  .map(KafkaProtoDataList(_).toByteArray)
                  .map(ByteString(_))
                  .map(Gzip.encode)
                  .map(TcpComponent.addLengthHeader)
                  .map(SendBytes)
                val out = builder.add(send)

                center.source("source-kuidai-kafka") ~> merge
                merge ~> out

                val receive = builder.add(Flow[SslTlsInbound].map {
                  case SessionBytes(a, b) ⇒ b
                  case x ⇒ logger.info(x.toString); ByteString.empty
                }
                  .via(TcpComponent.lengthFraming(10 * 1024 * 1024))
                  .to(Sink.foreach {
                    x: ByteString ⇒
                      x.toArray.grouped(TId.length).foreach {
                        bytes ⇒
                          val tId = TId.keyFromBytes(bytes)
                          center.actor(tId.persistenceId).actorRef ! tId
                      }
                  }))
                FlowShape(receive.inlet, out.outlet)
              })
          val sslFlow = serverTls(IgnoreComplete).reversed join serverLogic
          connection.flow.join(sslFlow).run()
        }
    }).run()
  } catch {
    case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
  }
}
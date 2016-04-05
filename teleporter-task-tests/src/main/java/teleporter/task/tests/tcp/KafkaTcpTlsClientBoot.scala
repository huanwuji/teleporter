package teleporter.task.tests.tcp

import java.io.FileInputStream

import akka.actor.ActorSystem
import akka.stream.io._
import akka.stream.scaladsl.Tcp
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord
import teleporter.integration.component.{TcpComponent, TeleporterKafkaRecord, TlsHelper}
import teleporter.integration.compress.TeleporterGzip
import teleporter.integration.core.{TId, TeleporterCenter, TeleporterMessage}
import teleporter.integration.proto.KafkaBuf.KafkaProtos

import scala.collection.JavaConversions._
import scala.concurrent.Future

/**
 * date 2015/8/3.
 * @author daikui
 */
object KafkaTcpTlsClientBoot extends App with LazyLogging {

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

    implicit val system = ActorSystem()
    import system.dispatcher
    val decider: Supervision.Decider = {
      case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Restart
    }
    implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val center = TeleporterCenter()
    val outgoingConn = clientTls(Closing.ignoreComplete) join Tcp().outgoingConnection("0.0.0.0", 9092)
    val forwardSourceId = "source-forward"
    center.source[TId](forwardSourceId)
      .grouped(2).map(_.map(_.toBytes).foldLeft(Array[Byte]())(_ ++ _))
      .map(x ⇒ SendBytes(TcpComponent.addLengthHeader(ByteString(x))))
      .via(outgoingConn)
      .map {
        case SessionBytes(a, b) ⇒ b
        case x ⇒ logger.info(x.toString); ByteString.empty
      }
      .via(TcpComponent.lengthFraming(10 * 1024 * 1024))
      .mapAsyncUnordered(1)(x ⇒ Future {
        TeleporterGzip.decode(x.toArray)
      })
      .map(x ⇒ KafkaProtos.parseFrom(x))
      .mapConcat(_.getProtosList.toIndexedSeq)
      .map {
        m ⇒
          val tId = TId.keyFromBytes(m.getTId.toByteArray)
          logger.info(m.getMessage.toStringUtf8)
          TeleporterMessage(id = tId,
            data = new ProducerRecord[Array[Byte], Array[Byte]](m.getTopic + "_test", m.getPartition, m.getKey.toByteArray, m.getMessage.toByteArray),
            toNext = {
              msg: TeleporterKafkaRecord ⇒ center.actor(forwardSourceId).actorRef ! tId
            })
      }.to(center.sink("sink-kuidai-sink")).run()
  } catch {
    case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
  }
}
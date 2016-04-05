package teleporter.task.tests

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.{KafkaRecord, TeleporterKafkaRecord}
import teleporter.integration.core.{TId, TeleporterCenter}

import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2015/10/26.
 */
object KafkaProducer extends App with LazyLogging {
  val decider: Supervision.Decider = {
    case e: Exception ⇒ logger.error(e.getLocalizedMessage, e); Supervision.Resume
  }
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  import system.dispatcher

  val center = TeleporterCenter()
  center.openMetrics(5.seconds)
  try {
    var i = 0
    Source.tick(1.seconds, 1000.millis, "test1").map {
      msg ⇒
        i += 1
        val kafkaRecord = new KafkaRecord("test", 0, msg.getBytes, (msg + i).getBytes)
        logger.info(msg)
        new TeleporterKafkaRecord(
          id = TId(11, 11, 11),
          sourceRef = null,
          data = kafkaRecord
        )
    }.to(center.sink("sh:kafka")).run()
  } catch {
    case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
  }
}
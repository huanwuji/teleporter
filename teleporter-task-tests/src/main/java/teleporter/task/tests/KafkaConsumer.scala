package teleporter.task.tests

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.TeleporterKafkaMessage
import teleporter.integration.core.TeleporterCenter

/**
 * Author: kui.dai
 * Date: 2015/10/26.
 */
object KafkaConsumer extends App with LazyLogging {
  val decider: Supervision.Decider = {
    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Restart
  }
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  import system.dispatcher

  val center = TeleporterCenter()
  val sourceName = "sh:kafka:kui.dai:test"
  center.source[TeleporterKafkaMessage](sourceName)
    .runForeach {
      msg ⇒
        println(msg.id)
        msg.toNext(msg)
    }
  Thread.sleep(100000)
  //  val actorRef = center.actor(sourceName).actorRef
  //  gracefulStop(actorRef, 10.seconds, CompleteThenStop)
  //    .onComplete {
  //      case Success(state) ⇒ println(state)
  //      case Failure(ex) ⇒ ex.printStackTrace()
  //    }
}
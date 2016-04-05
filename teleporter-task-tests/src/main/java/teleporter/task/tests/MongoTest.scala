package teleporter.task.tests

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.mongo.Bson2Json
import teleporter.integration.core.TeleporterCenter
import teleporter.integration.script.ScriptEngines

/**
 * Author: kui.dai
 * Date: 2016/2/22.
 */
object MongoTest extends App with LazyLogging {
  val decider: Supervision.Decider = {
    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Restart
  }
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  import system.dispatcher

  val center = TeleporterCenter()
  val sourceName = "156:mongo"
  val mapper = Bson2Json()
  val func = ScriptEngines.scala.eval(
    """
      |import akka.actor.ActorSystem
      |import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
      |import com.typesafe.scalalogging.LazyLogging
      |import teleporter.integration.component._
      |import teleporter.integration.component.mongo.Bson2Json
      |import teleporter.integration.core.TeleporterCenter
      |import teleporter.integration.script.ScriptEngines
      |
      |  val a: (TeleporterCenter, StreamConfig) ⇒ Unit = center ⇒ {
      |    implicit val system = center.actorSystem
      |    implicit val mater = center.materializer
      |    center.source[TeleporterMongoMessage]("156:mongo").map(msg ⇒ {
      |      msg.toNext(msg)
      |      println(msg)
      |      1
      |    }).runFold(0)((m, c) ⇒ c + 1)
      |  }
      |  a
    """.stripMargin).asInstanceOf[TeleporterCenter ⇒ Unit]
  //  val future = center.source[TeleporterMongoMessage](sourceName).map(msg ⇒ {
  //    msg.toNext(msg)
  //    println(msg)
  //    1
  //  })
  func(center)
  Thread.sleep(100000)

  case class StreamConfig(source: Map[String, String], sink: Map[String, String], template: String, templateId: Int)

}
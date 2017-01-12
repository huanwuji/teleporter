package teleporter.stream.integration.transaction

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Attributes}

import scala.concurrent.duration._

/**
  * Created by huanwuji on 2016/10/31.
  */
object LogTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  implicit val log = Logging(system, "")
  Source.tick(1.second, 1.second, 1).log("test").withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel)).runForeach(println(_))
}

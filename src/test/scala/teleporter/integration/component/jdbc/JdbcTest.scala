package teleporter.integration.component.jdbc

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Merge, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, TeleporterAttributes}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2017/3/17.
  */
object JdbcTest extends App {
  implicit val system = ActorSystem("test", ConfigFactory.load("instance"))
  implicit val mater = ActorMaterializer()
  Source.combine(
    Source(1 to 10).map { i ⇒
      Thread.sleep(1000)
      println(Thread.currentThread().getName)
      i
    }.addAttributes(ActorAttributes.dispatcher(TeleporterAttributes.CacheDispatcher.dispatcher)).async,
    Source(20 to 30).map { i ⇒
      Thread.sleep(10000)
      println(Thread.currentThread().getName)
      i
    }.addAttributes(ActorAttributes.dispatcher(TeleporterAttributes.CacheDispatcher.dispatcher)).async,
    Source.tick(1.second, 1.second, 1111111).async,
    Source.tick(1.second, 1.second, 44444444).async
  )(Merge(_))
    .runForeach(println)
  //  Source(1 to 10).flatMapMerge(10, i ⇒ {
  //    if (i == 2) {
  //      Source.tick(1.second, 1.second, 1111111)
  //    } else {
  //      Source(i * 10 to i * 10 + 10).map { i ⇒ Thread.sleep(1000); i }
  //    }
  //  }).runForeach(println)
}

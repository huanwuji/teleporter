package teleporter.integration.component

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2017/3/20.
  */
class Cron$Test extends FunSuite {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  test("testSource") {
    val cronRef = Cron.cronRef("test_cron")
    //    val fu = Cron.source(cronRef, "*/1 * * * *", "test").to(Sink.foreach(println)).run()
    //    Thread.sleep(1000)
    //    fu.cancel()
    //    Thread.sleep(1000000)

    val fu = Cron.source(cronRef, "*/1 * * * *", "test").runForeach(println)
    Await.result(fu, 10.minutes)
  }

}

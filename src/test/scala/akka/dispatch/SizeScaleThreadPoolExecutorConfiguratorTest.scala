package akka.dispatch

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

/**
  * Created by huanwuji 
  * date 2017/2/21.
  */
class SizeScaleThreadPoolExecutorConfiguratorTest extends FunSuite {
  test("init") {
    val config = ConfigFactory.load("instance.conf")
    val system = ActorSystem("test", config)
    val dispatcher = system.dispatchers.lookup("akka.teleporter.blocking-dispatcher")
    dispatcher
  }
}
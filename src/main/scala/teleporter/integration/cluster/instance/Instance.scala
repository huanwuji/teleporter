package teleporter.integration.cluster.instance

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import teleporter.integration.cluster.instance.Brokers.LoaderBroker
import teleporter.integration.core.TeleporterCenter

/**
  * Created by kui.dai on 2016/8/9.
  */
object Instance {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("instance")
    implicit val system = ActorSystem("instance", config)
    implicit val mater = ActorMaterializer()
    val instanceConfig = config.getConfig("teleporter")
    val (key, brokers) = (instanceConfig.getString("key"), instanceConfig.getString("brokers"))
    val center = TeleporterCenter(key, instanceConfig)
    center.brokers ! LoaderBroker(brokers)
  }
}
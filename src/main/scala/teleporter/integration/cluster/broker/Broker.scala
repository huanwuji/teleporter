package teleporter.integration.cluster.broker

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import teleporter.integration.cluster.broker.http.HttpServer
import teleporter.integration.cluster.broker.tcp.RpcServer
import teleporter.integration.cluster.rpc.{RpcServerConnectionHandler, fbs}
import teleporter.integration.utils.EventListener

import scala.collection.concurrent.TrieMap

/**
  * Created by kui.dai on 2016/8/9.
  */
object Broker {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("broker")
    implicit val system = ActorSystem("broker", config)
    implicit val mater = ActorMaterializer()
    import system.dispatcher
    val brokerConfig = config.getConfig("teleporter")
    val (configService, runtimeService) = PersistentService(brokerConfig)
    val connectionKeepers = TrieMap[String, RpcServerConnectionHandler]()
    val configNotify = system.actorOf(Props(classOf[ConfigNotify], connectionKeepers, configService, runtimeService))
    val eventListener = EventListener[fbs.RpcMessage]()
    RpcServer(brokerConfig.getConfig("tcp"), configService, runtimeService, configNotify, connectionKeepers, eventListener)
    HttpServer(brokerConfig.getConfig("http"), configNotify, configService, runtimeService, connectionKeepers, eventListener)
  }
}
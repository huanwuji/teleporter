package teleporter.integration.core

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import org.scalatest.FunSuite
import teleporter.integration.utils.MapBean

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2017/2/23.
  */
class SourceAck$Test extends FunSuite {
  val decider: Supervision.Decider = {
    case e ⇒ println(e); Supervision.Resume
  }

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  def ackFlow[T](id: Long, config: MapBean): Flow[SourceMessage[MapBean, T], AckMessage[MapBean, T], NotUsed] = {
    Flow.fromGraph(new SourceAck[MapBean, T](
      id = id,
      config = SourceAckConfig(config),
      commit = coordinate ⇒ println(s"commit $coordinate"),
      finish = coordinate ⇒ println(s"complete $coordinate")
    ))
  }

  test("throughput") {
    val start = System.currentTimeMillis()
    val result = Source(1 to 1000000)
      .map(data ⇒ Message.source(MapBean(Map("XY" → data)), data))
      .via(ackFlow[Int](1L, MapBean(Map(
        "ack" → Map(
          "channelSize" → 1,
          "commitInterval" → "1.minute",
          "cacheSize" → 64,
          "maxAge" → "1.minute")
      )))
      )
      .watchTermination()(Keep.right)
      .to(SourceAck.confirmSink()).run()
    Await.result(result, 1.minute)
    println(System.currentTimeMillis() - start)
    materializer.shutdown()
    Await.result(system.terminate(), 1.minute)
  }
}

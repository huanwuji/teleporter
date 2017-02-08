//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitch, KillSwitches}
import teleporter.integration.component.Kafka
import teleporter.integration.core.Streams.StreamLogic
import teleporter.integration.core.{SourceAck, TeleporterCenter}

import scala.concurrent.Future

/**
  * Created by kui.dai on 2016/8/16.
  */
object ReceiveKafka extends StreamLogic {
  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    println("--------------ReceiveKafka---------------------")
    import center.{materializer, self}
    Kafka.sourceAck("/source/test/test_task/kafka_receiver/kafka_source")
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .via(SourceAck.confirmFlow()).to(Sink.foreach(x ⇒ println(x))).run()
  }
}
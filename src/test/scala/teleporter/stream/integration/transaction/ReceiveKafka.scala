//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.Keep
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
    println("--------------stream1---------------------")
    import center.{materializer, self}
    Kafka.sourceAck("/source/test/kuidai_test_task2/stream_task2_stream1/kafka_test")
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(SourceAck.confirmSink()).run()
  }
}
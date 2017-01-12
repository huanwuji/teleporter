//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.{KillSwitch, KillSwitches}
import org.apache.kafka.clients.producer.ProducerRecord
import teleporter.integration.component.Kafka
import teleporter.integration.core.Streams.StreamLogic
import teleporter.integration.core.{Message, TeleporterCenter}
import teleporter.integration.utils.Bytes._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by kui.dai on 2016/8/16.
  */
object SendKafka extends StreamLogic {
  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    println("--------------stream1---------------------")
    import center.{materializer, self}
    Source.tick(1.second, 1.second, "test")
      .map { str ⇒
        println(str)
        Message(data = new ProducerRecord[Array[Byte], Array[Byte]]("test", str))
      }
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(Kafka.sink("/sink/test/kuidai_test_task1/kuidai_task1_stream1/kafka_test"))
      .run()
  }
}
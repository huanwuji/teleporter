//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches}
import org.apache.commons.lang3.RandomStringUtils
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
    println("--------------SendKafka---------------------")
    import center.{materializer, self}
    Source.tick(1.second, 1.second, "test")
      .map { _ ⇒
        val s = RandomStringUtils.randomAlphabetic(10)
        println(s)
        Message(data = new ProducerRecord[Array[Byte], Array[Byte]]("ppp", "1", s))
      }
      .via(Kafka.flow("/sink/test/test_task/kafka_send/send_sink"))
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(Sink.ignore).run()
  }
}
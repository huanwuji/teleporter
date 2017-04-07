package teleporter.stream.integration.template

import akka.Done
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches}
import org.apache.hadoop.hbase.client.Put
import teleporter.integration.component.hbase.{Hbase, HbaseAction}
import teleporter.integration.core.Streams._
import teleporter.integration.core.{Message, TeleporterCenter}
import teleporter.integration.utils.Bytes._

import scala.concurrent.Future

/**
  * Created by huanwuji on 2016/10/20.
  * arguments: fields, index, type
  */
object HbaseTest extends StreamLogic {
  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    import center.{materializer, self}
    Source.single(1)
      .map { o ⇒
        val put = new Put("1")
        put.addColumn("f", "q", "v")
        Message.apply(HbaseAction("teleporter", put))
      }
      .via(Hbase.flow("/sink/test/hbase_test/hbase_test"))
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(Sink.ignore).run()
  }
}
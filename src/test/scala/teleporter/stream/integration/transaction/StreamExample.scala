//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitch, KillSwitches}
import teleporter.integration.component.jdbc.SqlSupport._
import teleporter.integration.component.jdbc.{Jdbc, Upsert}
import teleporter.integration.core.Streams._
import teleporter.integration.core.{SourceAck, TeleporterCenter}

import scala.concurrent.Future

/**
  * Created by huanwuji on 2016/10/20.
  */
object StreamExample extends StreamLogic {
  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    import center.{materializer, self}
    Jdbc.sourceAck("/source/ns/task/stream/source")
      .map(_.map(data ⇒ Seq(Upsert(
        updateSql("table_name", "id", data),
        insertIgnoreSql("table_name", data)
      ))).toMessage)
      .via(Jdbc.flow("/source/ns/task/stream/sink"))
      .via(SourceAck.confirmFlow())
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .to(Sink.ignore).run()
  }
}
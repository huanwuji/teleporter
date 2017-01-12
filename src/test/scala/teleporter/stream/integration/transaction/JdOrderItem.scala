//package teleporter.stream.integration.transaction

import akka.Done
import akka.stream.scaladsl.Keep
import akka.stream.{KillSwitch, KillSwitches}
import teleporter.integration.component.jdbc.SqlSupport._
import teleporter.integration.component.jdbc.{Jdbc, Upsert}
import teleporter.integration.core.Streams._
import teleporter.integration.core.{SourceAck, TeleporterCenter}

import scala.concurrent.Future

/**
  * Created by huanwuji on 2016/10/20.
  */
object JdOrderItem extends StreamLogic {
  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {
    import center.{materializer, self}
    Jdbc.sourceAck("/source/ns/task/stream/source")
      .map(x ⇒
        x.map(data ⇒ Seq(Upsert(
          updateSql("table_name", "id", data),
          insertIgnoreSql("table_name", data)
        ))).toMessage
      )
      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)
      .via(Jdbc.flow("/source/ns/task/stream/sink"))
      .to(SourceAck.confirmSink())
      .run()
  }
}
package teleporter.integration.protocol.fbs

import akka.actor.ActorRef
import org.scalatest.FunSuite
import teleporter.integration.component._
import teleporter.integration.core.TId
import teleporter.integration.protocol.fbs.FbsJdbc

/**
 * Author: kui.dai
 * Date: 2016/4/12.
 */
class FbsJdbc$Test extends FunSuite {
  test("apply") {
    val record = new TeleporterJdbcRecord(id = TId(1, 1, 1), data = null)
    val flatBuilder = FbsJdbc(Seq(record), 256)
    flatBuilder.dataBuffer()
    val records = FbsJdbc.unapply(flatBuilder.sizedByteArray(), ActorRef.noSender)
    records
  }
}

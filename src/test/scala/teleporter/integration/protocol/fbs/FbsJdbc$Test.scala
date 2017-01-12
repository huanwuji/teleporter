package teleporter.integration.protocol.fbs

import org.scalatest.FunSuite
import teleporter.integration.component.jdbc.Action
import teleporter.integration.core.{TId, TransferMessage}

/**
  * Author: kui.dai
  * Date: 2016/4/12.
  */
class FbsJdbc$Test extends FunSuite {
  test("apply") {
    val message = TransferMessage[Seq[Action]](id = TId(1, 1, 1), data = Seq.empty)
    val flatBuilder = FbsJdbc.unapply(Seq(message), 256)
    flatBuilder.dataBuffer()
    val records = FbsJdbc(flatBuilder.sizedByteArray())
    records
  }
}
package teleporter.integration.transaction

import org.scalatest.FunSuite

/**
 * Author: kui.dai
 * Date: 2015/10/26.
 */
class BatchCommitTransactionTest extends FunSuite {
  //  test("BatchCommitTransaction") {
  //    implicit val system = ActorSystem()
  //    implicit val noSender = ActorRef.noSender
  //    implicit val recoveryPoint = new EmptyRecoveryPoint
  //    val batchCoordinate = "url://currDay"
  //    val transaction = new BatchCommitTransaction[Any, Any] {
  //      override implicit val recoveryPoint: RecoveryPoint[Any] = _
  //      override val transactionConf: TransactionConf = TransactionConf()
  //
  //      override def id: Int = ???
  //    }
  //    val list = ListBuffer[TeleporterMessage[Any]]()
  //    for (i ← 1 to 10) {
  //      transaction.begin(batchCoordinate, i)(msg ⇒ list += msg)(ActorRef.noSender)
  //    }
  //    for (msg ← list) {
  //      transaction.end(msg.id)
  //    }
  //    transaction
  //  }
  //  test("BatchCommitTransaction multi channel") {
  //    implicit val system = ActorSystem()
  //    implicit val noSender = ActorRef.noSender
  //    implicit val recoveryPoint = new EmptyRecoveryPoint
  //    val batchCoordinate = "url://currDay"
  //    val transaction = new BatchCommitTransaction[Any, Any] {
  //      override implicit val recoveryPoint: RecoveryPoint[Any] = _
  //      override val transactionConf: TransactionConf = _
  //
  //      override def id: Int = ???
  //    }
  //    val list = ListBuffer[TeleporterMessage[Any]]()
  //    for (i ← 1 to 10) {
  //      transaction.begin(batchCoordinate, i)(msg ⇒ list += msg)(ActorRef.noSender)
  //    }
  //    for (msg ← list) {
  //      transaction.end(msg.id)
  //    }
  //    for (msg ← list) {
  //      transaction.end(msg.id.switchChannel(2))
  //    }
  //    for (msg ← list) {
  //      transaction.end(msg.id.switchChannel(3))
  //    }
  //    transaction
  //  }
}

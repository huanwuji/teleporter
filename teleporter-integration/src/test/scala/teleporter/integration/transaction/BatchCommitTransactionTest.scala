package teleporter.integration.transaction

import org.scalatest.FunSuite

import scala.collection.immutable.LongMap
import scala.collection.mutable
import scala.concurrent.duration._

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
  test("long map min") {
    val longMap = LongMap[Int](111L →3, 111L→4,11L→6)
    println(longMap.min)
  }
  test("test queue") {
    val time = System.currentTimeMillis()
    val queue = mutable.Queue(("000", time - 3000), ("111", time - 2000), ("222", time - 1000), ("333", time))
    while ( {
      val firstPoint = queue.headOption
      firstPoint.isDefined && (System.currentTimeMillis() - firstPoint.get._2) > 1000
    }) {
      println(s"------- ${queue.dequeue()}")
    }
    println(queue)
  }
  test("commit") {
    val batchGrouped = mutable.LongMap(
      2L → BatchInfo(Seq.empty[Any], 10, System.currentTimeMillis(), System.currentTimeMillis() - 2.minutes.toMillis),
      3L → BatchInfo(Seq.empty[Any], 10, System.currentTimeMillis()),
      4L → BatchInfo(Seq.empty[Any], 10, System.currentTimeMillis(), System.currentTimeMillis() - 3.minutes.toMillis),
      5L → BatchInfo(Seq.empty[Any], 10, System.currentTimeMillis(), System.currentTimeMillis() - 3.minutes.toMillis),
      6L → BatchInfo(Seq.empty[Any], 10, System.currentTimeMillis())
    )
    val commitDelay = None
    val min = batchGrouped.minBy(_._1)._1
    var curr = min
    var lastCommitKey: Option[Long] = None
    while (canCommit(batchGrouped.get(curr), commitDelay)) {
      println(curr)
      lastCommitKey = Some(curr)
      curr += 1L
    }
    lastCommitKey.foreach(x ⇒ batchGrouped --= (min to x))
    println(batchGrouped)
  }

  def canCommit(batchInfoOpt: Option[BatchInfo[Any]], commitDelay: Option[Duration]): Boolean =
    batchInfoOpt.exists(batchInfo ⇒ batchInfo.isComplete && (commitDelay.isEmpty || commitDelay.exists(d ⇒ System.currentTimeMillis() - batchInfo.completedTime > d.toMillis)))
}

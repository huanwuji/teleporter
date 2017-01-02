package teleporter.integration.transaction

import akka.actor.ActorRef
import teleporter.integration.core._

/**
  * author: huanwuji
  * created: 2015/9/4.
  */
trait Transaction[T, B] extends AutoCloseable {
  implicit val recoveryPoint: RecoveryPoint[B]
  implicit val center: TeleporterCenter

  def id: Long

  def tryBegin(batchCoordinate: B, grab: ⇒ Option[T], handler: TeleporterMessage[T] ⇒ Unit)(implicit actorRef: ActorRef): Transaction.State

  def end(id: TId): Unit

  protected def doCommit(point: B): Unit

  def isComplete: Boolean

  def close(): Unit = {}
}

object Transaction {

  sealed trait State

  case object Retry extends State

  case object OverLimit extends State

  case object Normal extends State

  case object NoData extends State

  def apply[T, B](key: String)(implicit center: TeleporterCenter): Transaction[T, B] = {
    apply(key, center.defaultRecoveryPoint)(center).asInstanceOf[Transaction[T, B]]
  }

  def apply[T, B](key: String, recoveryPoint: RecoveryPoint[B])(implicit center: TeleporterCenter): Transaction[T, B] = {
    val sourceContext = center.context.getContext[SourceContext](key)
    implicit val sourceConfig = sourceContext.config
    val txnConfig = RingTxnConfig(sourceConfig)
    new RingTransactionImpl[T, B](sourceContext.id, txnConfig)(center, recoveryPoint)
  }
}
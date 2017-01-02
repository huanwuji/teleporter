package teleporter.integration.transaction

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.core._
import teleporter.integration.utils.MapBean

import scala.collection.mutable
import scala.concurrent.duration.{Duration, _}

/**
  * Created by huanwuji 
  * date 2016/12/23.
  */
object RingTransactionMetaBean {
  val FTransaction = "transaction"
  val FRecoveryPointEnabled = "recoveryPointEnabled"
  val FChannelSize = "channelSize"
  val FCacheSize = "cacheSize"
  val FBatchSize = "batchSize"
  val FMaxAge = "maxAge"
  val FTimeoutRetry = "timeoutRetry"
  val FCommitDelay = "commitDelay"
}

class RingTransactionMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {

  import RingTransactionMetaBean._

  def transaction: MapBean = client[MapBean](FTransaction)

  def recoveryPointEnabled: Boolean = transaction.get[Boolean](FRecoveryPointEnabled).getOrElse(true)

  def channelSize: Int = transaction.get[Int](FChannelSize).getOrElse(1)

  def cacheSize: Int = transaction.get[Int](FCacheSize).getOrElse(256)

  def batchSize: Int = transaction.get[Int](FBatchSize).getOrElse(500)

  def maxAge: Duration = transaction.get[Duration](FMaxAge).getOrElse(2.minutes)

  def timeoutRetry: Boolean = transaction.get[Boolean](FTimeoutRetry).getOrElse(true)

  def commitDelay: Option[Duration] = transaction.get[Duration](FCommitDelay)
}

case class ConfirmData(data: Any, created: Long)

class BitSets(bitSets: Array[mutable.BitSet]) {
  def apply(idx: Int): Boolean = {
    !bitSets.exists(_ (idx) == false)
  }

  def apply(idx: Int, channel: Int): Boolean = {
    !bitSets.exists(_ (idx) == false)
  }

  def +=(idx: Int): Unit = bitSets.foreach(_ += idx)

  def -=(idx: Int, channel: Int): mutable.BitSet = bitSets(channel) -= idx

  def update(idx: Int, channel: Int): Unit = {
    bitSets(channel) += idx
  }
}

object BitSets {
  def apply(size: Int, channelSize: Int) = new BitSets(Array.fill(size)(new mutable.BitSet(size)))
}

class RingPool(size: Int, channelSize: Int) extends LazyLogging {
  val bitSets: BitSets = BitSets(size, channelSize)
  val elements: Array[ConfirmData] = new Array(size)
  private var freeSpace: Int = size
  private var usedCursor: Long = -1
  private var canConfirmedCursor: Long = 0
  private var confirmedCursor: Long = -1

  def getNextIdx: Long = if (freeSpace > 0) usedCursor + 1 else -1

  def add(element: Any): Long = {
    if (freeSpace > 0) {
      usedCursor += 1
      val ringIdx = (usedCursor % size).toInt
      bitSets += ringIdx
      elements.update(ringIdx, ConfirmData(element, System.currentTimeMillis()))
      usedCursor
    } else {
      -1
    }
  }

  def remove(idx: Long, channel: Int): Unit = {
    if (idx > usedCursor || idx < confirmedCursor) {
      logger.debug(s"Invalid $idx, used: $usedCursor, confirm: $confirmedCursor, This was be confirmed!")
    }
    val ringIdx = (idx % size).toInt
    if (bitSets(ringIdx, channel)) {
      bitSets -= (ringIdx, channel)
      if (bitSets(ringIdx)) {
        freeSpace += 1
        elements.update(ringIdx, null)
        canConfirmed
      }
    } else {
      logger.debug(s"Invalid $idx, This was be confirmed!")
    }
  }

  def canConfirmed: Long = {
    while ( {
      val nextConfirmedCursor = canConfirmedCursor + 1
      nextConfirmedCursor < usedCursor && !bitSets((nextConfirmedCursor % size).toInt)
    }) {
      canConfirmedCursor += 1
    }
    canConfirmedCursor
  }

  def canConfirmedSize: Long = canConfirmed - confirmedCursor

  def unConfirmedSize: Long = usedCursor - confirmedCursor

  def confirmed(): Unit = confirmedCursor = canConfirmedCursor

  def isFull: Boolean = freeSpace == 0
}

object RingPool {
  def apply(size: Int, channelSize: Int): RingPool = new RingPool(size, channelSize)
}

case class RingTxnConfig(
                          recoveryPointEnabled: Boolean = true,
                          channelSize: Int,
                          cacheSize: Int,
                          batchSize: Int,
                          maxAge: Duration,
                          timeoutRetry: Boolean = true,
                          commitDelay: Option[Duration]
                        )

object RingTxnConfig {
  def apply(config: MapBean): RingTxnConfig = {
    val ringMetaBean = config.mapTo[RingTransactionMetaBean]
    RingTxnConfig(
      recoveryPointEnabled = ringMetaBean.recoveryPointEnabled,
      channelSize = ringMetaBean.channelSize,
      cacheSize = ringMetaBean.cacheSize,
      batchSize = ringMetaBean.batchSize,
      maxAge = ringMetaBean.maxAge,
      timeoutRetry = ringMetaBean.timeoutRetry,
      commitDelay = ringMetaBean.commitDelay
    )
  }
}

trait RingTransaction[T, B] extends Transaction[T, B] with LazyLogging {
  val txnConfig: RingTxnConfig
  val ringPool: RingPool = RingPool(txnConfig.cacheSize, txnConfig.channelSize)
  private val coordinates = mutable.Queue[(Long, B)]()
  private var lastCoordinate: B = _

  def id: Long

  def tryBegin(coordinate: B, grab: ⇒ Option[T], handler: TeleporterMessage[T] ⇒ Unit)(implicit actorRef: ActorRef): Transaction.State = {
    if (txnConfig.timeoutRetry && {
      val expiredMessage = expiredRetry()
      expiredMessage.exists { message ⇒
        handler(message)
        true
      }
    }) {
      Transaction.Retry
    } else {
      if (ringPool.isFull) {
        Transaction.OverLimit
      } else {
        grab match {
          case Some(data) ⇒
            val seqNr = ringPool.getNextIdx
            if (lastCoordinate != coordinate) {
              lastCoordinate = coordinate
              coordinates += (seqNr → lastCoordinate)
            }
            val message = TeleporterMessage[T](TId(id, seqNr), actorRef, data)
            ringPool.add(message)
            handler(message)
            Transaction.Normal
          case None ⇒
            Transaction.NoData
        }
      }
    }
  }

  private var checkTime = System.currentTimeMillis()
  private var expiredMessages: Iterator[TeleporterMessage[T]] = Iterator.empty

  def expiredRetry(): Option[TeleporterMessage[T]] = {
    if (expiredMessages.hasNext) {
      Some(expiredMessages.next())
    } else {
      if (System.currentTimeMillis() - checkTime > txnConfig.maxAge.toMillis) {
        checkTime = System.currentTimeMillis()
        val _expiredMessages = ringPool.elements
          .filter(checkTime - _.created > txnConfig.maxAge.toMillis)
          .map(_.data.asInstanceOf[TeleporterMessage[T]])
        if (_expiredMessages.nonEmpty) {
          logger.warn(s"$id expired message size:${_expiredMessages.length}")
          expiredMessages = _expiredMessages.toIterator
          return expiredRetry()
        }
      }
      None
    }
  }

  def end(id: TId): Unit = {
    ringPool.remove(id.seqNr, id.channelId)
    if (ringPool.canConfirmedSize > txnConfig.batchSize) {
      val canConfirmedIdx = ringPool.canConfirmed
      var latestCoordinate: B = coordinates.head._2
      while (coordinates.head._1 < canConfirmedIdx) {
        latestCoordinate = coordinates.dequeue()._2
      }
      doCommit(latestCoordinate)
    }
  }

  val commitQueue: mutable.Queue[(Long, B)] = mutable.Queue[(Long, B)]()

  protected def doCommit(point: B): Unit = {
    val key = center.context.getContext[SourceContext](id).key
    txnConfig.commitDelay match {
      case None ⇒ recoveryPoint.save(key, point)
      case Some(delay) ⇒
        commitQueue.enqueue(System.currentTimeMillis() → point)
        commitQueue.dequeueAll(System.currentTimeMillis() - _._1 > delay.toMillis).lastOption
          .foreach(point ⇒ recoveryPoint.save(key, point._2))
    }
  }

  def isComplete: Boolean = if (ringPool.unConfirmedSize == 0) {
    val key = center.context.getContext[SourceContext](id).key
    recoveryPoint.complete(key)
    true
  } else {
    false
  }

  override def close(): Unit = {}
}

class RingTransactionImpl[T, B](val id: Long, val txnConfig: RingTxnConfig)
                               (implicit val center: TeleporterCenter, val recoveryPoint: RecoveryPoint[B]) extends RingTransaction[T, B]

package teleporter.integration.transaction

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.core._
import teleporter.integration.transaction.ChunkTransaction.{ChunkTxnConfig, RingChunkPool}
import teleporter.integration.utils.Converters._
import teleporter.integration.utils.{MapBean, MapMetadata}

import scala.collection.mutable
import scala.concurrent.duration.{Duration, _}
import scala.reflect.ClassTag

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

  def tryComplete(): Boolean

  def close(): Unit = {}
}

trait ChunkTransaction[T, B] extends Transaction[T, B] with LazyLogging {
  val txnConfig: ChunkTxnConfig
  val chunkPools = RingChunkPool[TeleporterMessage[T]](txnConfig)
  private var seqNr: Long = -1
  private val chunkQueue = mutable.Queue[(Long, B)]()

  def id: Long

  def tryBegin(chunkCoordinate: B, grab: ⇒ Option[T], handler: TeleporterMessage[T] ⇒ Unit)(implicit actorRef: ActorRef): Transaction.State = {
    if (txnConfig.timeoutRetry && {
      val expiredMessage = expiredRetry()
      expiredMessage.exists {
        message ⇒
          handler(message)
          true
      }
    }) {
      Transaction.Retry
    } else {
      seqNr += 1
      val chunkIdx = seqNr / txnConfig.blockSize
      if (chunkQueue.isEmpty) {
        chunkQueue.enqueue(chunkIdx → chunkCoordinate)
      } else if (chunkQueue.last._1 != chunkIdx) {
        if (chunkQueue.size >= txnConfig.maxBlockNum) {
          seqNr -= 1
          return Transaction.OverLimit
        } else {
          chunkQueue.enqueue(chunkIdx → chunkCoordinate)
        }
      }
      grab match {
        case Some(data) ⇒
          val message = TeleporterMessage[T](TId(id, seqNr), actorRef, data)
          chunkPools.add(seqNr, message)
          handler(message)
          Transaction.Normal
        case None ⇒ Transaction.NoData
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
        val _expiredMessages = chunkPools.expired()
        if (_expiredMessages.nonEmpty) {
          logger.warn(s"$id expired message size:${_expiredMessages.size}")
          expiredMessages = _expiredMessages.toIterator
          return expiredRetry()
        }
      }
      None
    }
  }

  def end(id: TId): Unit = {
    chunkPools.confirm(id.seqNr, id.channelId)
    while (chunkQueue.nonEmpty && chunkPools.allConfirm(chunkQueue.head._1)) {
      doCommit(chunkQueue.dequeue()._2)
    }
  }

  val commitQueue = mutable.Queue[(Long, B)]()

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

  def tryComplete(): Boolean = chunkQueue.size == 1 && chunkPools.unConfirmed(chunkQueue.last._1).isEmpty

  override def close(): Unit = {}
}

class ChunkTransactionImpl[T, B](val id: Long, val txnConfig: ChunkTxnConfig)
                                (implicit val center: TeleporterCenter, val recoveryPoint: RecoveryPoint[B]) extends ChunkTransaction[T, B]

object ChunkTransaction {

  trait ChunkTransactionMetadata extends MapMetadata {
    val FTransaction = "transaction"
    val FChannelSize = "channelSize"
    val FBlockSize = "blockSize"
    val FCommitDelay = "commitDelay"
    val FMaxAge = "maxAge"
    val FMaxBlockNum = "maxBlockNum"
    val FRecoveryPointEnabled = "recoveryPointEnabled"
    val FTimeoutRetry = "timeoutRetry"
  }

  case class ChunkTxnConfig(channelSize: Int = 1,
                            blockSize: Int = 50,
                            commitDelay: Option[Duration],
                            maxAge: Duration = 1.minutes,
                            maxBlockNum: Int = 5,
                            recoveryPointEnabled: Boolean = true,
                            timeoutRetry: Boolean = true)

  object ChunkTxnConfig extends ChunkTransactionMetadata {
    def apply(config: MapBean): ChunkTxnConfig = {
      val transactionConfig = config[MapBean](FTransaction)
      ChunkTxnConfig(
        channelSize = transactionConfig.__dict__[Int](FChannelSize).getOrElse(1),
        blockSize = transactionConfig.__dict__[Int](FBlockSize).getOrElse(10000),
        commitDelay = transactionConfig.__dict__[Duration](FCommitDelay),
        maxAge = transactionConfig.__dict__[Duration](FMaxAge).getOrElse(2.minutes),
        maxBlockNum = transactionConfig.__dict__[Int](FMaxBlockNum).getOrElse(5),
        recoveryPointEnabled = transactionConfig.__dict__[Boolean](FRecoveryPointEnabled).getOrElse(true),
        timeoutRetry = transactionConfig.__dict__[Boolean](FTimeoutRetry).getOrElse(true)
      )
    }
  }

  private[transaction] case class Chunk[T](bitSets: Array[mutable.BitSet], elements: Array[Any], blockSize: Int, created: Long) extends LazyLogging {
    private var confirmedSize: Int = 0

    def add(inx: Int, ele: T) = elements.update(inx, ele)

    def confirm(inx: Int, channel: Int): Unit = {
      val currBitSet = bitSets(channel)
      if (!currBitSet(inx)) {
        currBitSet += inx
        if (!bitSets.exists(!_ (inx))) {
          confirmedSize += 1
          elements.update(inx, null)
        }
      } else {
        logger.warn(s"Some confirm is delay, $inx")
      }
    }

    def allConfirm(): Boolean = confirmedSize == blockSize

    def unConfirmed(): Array[T] = elements.filterNot(_ == null).asInstanceOf[Array[T]]

    def clear(): Unit = {
      confirmedSize = 0
      bitSets.foreach(_.clear())
    }
  }

  object Chunk {
    def apply[T: ClassTag](config: ChunkTxnConfig) = {
      new Chunk[T](
        bitSets = Array.fill(config.channelSize)(new mutable.BitSet(config.blockSize)),
        elements = new Array(config.blockSize),
        blockSize = config.blockSize,
        created = System.currentTimeMillis())
    }
  }

  private[transaction] class RingChunkPool[T](chunks: Array[Chunk[T]], blockSize: Int, maxBlockNum: Int, maxAge: Duration) extends LazyLogging {
    val currUsedChunk = Array.fill(maxBlockNum)(-1L)

    def add(seqNr: Long, ele: T): Boolean = {
      val chunkIdx = getChunkIdx(seqNr)
      val realIdx = getRealIdxByChunkIdx(chunkIdx)
      val currUsedIdx = currUsedChunk(realIdx)
      if (currUsedIdx == -1) {
        currUsedChunk.update(realIdx, chunkIdx)
        chunks.update(realIdx, chunks(realIdx).copy[T](created = System.currentTimeMillis()))
      }
      if (currUsedIdx == chunkIdx) {
        chunks(realIdx).add((seqNr % blockSize).toInt, ele)
        true
      } else {
        false
      }
    }

    def expired(): Seq[T] = (0 until maxBlockNum)
      .filter(currUsedChunk(_) == -1)
      .filter(System.currentTimeMillis() - chunks(_).created > maxAge.toMillis)
      .flatMap(chunks(_).unConfirmed())

    def confirm(seqNr: Long, channel: Int): Unit = {
      val chunkIdx = getChunkIdx(seqNr)
      val realIdx = getRealIdxByChunkIdx(chunkIdx)
      if (currUsedChunk(realIdx) == chunkIdx) {
        val chunk = chunks(realIdx)
        chunk.confirm((seqNr % blockSize).toInt, channel)
        if (chunk.allConfirm()) {
          chunk.clear()
          currUsedChunk.update(realIdx, -1)
        } else {
          logger.debug(s"Miss confirm, $seqNr, $channel")
        }
      } else {
        logger.debug(s"Miss confirm, $seqNr, $channel")
      }
    }

    def allConfirm(chunkIdx: Long): Boolean = {
      val chunkUsed = currUsedChunk(getRealIdxByChunkIdx(chunkIdx))
      chunkUsed == -1 || chunkUsed != chunkIdx
    }

    def unConfirmed(chunkIdx: Long): Array[T] = chunks(getRealIdxByChunkIdx(chunkIdx)).unConfirmed()

    private def getChunkIdx(seqNr: Long): Long = seqNr / blockSize

    private def getRealIdx(seqNr: Long): Int = getRealIdxByChunkIdx(getChunkIdx(seqNr))

    private def getRealIdxByChunkIdx(chunkIdx: Long): Int = (chunkIdx % maxBlockNum).toInt
  }

  object RingChunkPool {
    def apply[T: ClassTag](config: ChunkTxnConfig) = {
      new RingChunkPool[T](
        chunks = Array.fill(config.maxBlockNum)(Chunk[T](config)),
        blockSize = config.blockSize,
        maxBlockNum = config.maxBlockNum,
        maxAge = config.maxAge
      )
    }
  }

}

object Transaction extends SourceMetadata {

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
    val txnConfig = ChunkTxnConfig(sourceConfig)
    new ChunkTransactionImpl[T, B](sourceConfig.id(), txnConfig)(center, recoveryPoint)
  }
}
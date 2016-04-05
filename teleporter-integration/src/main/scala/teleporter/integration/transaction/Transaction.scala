package teleporter.integration.transaction

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.conf.Conf.Source
import teleporter.integration.conf.TransactionProps
import teleporter.integration.core.{TId, TeleporterMessage}

import scala.collection.mutable
import scala.concurrent.duration.{Duration, _}

/**
 * author: huanwuji
 * created: 2015/9/4.
 */
case class BatchInfo[B](var batchCoordinates: Seq[B], var count: Int)

private[transaction] case class TransactionRecord[T, V](record: TeleporterMessage[T], batchCoordinate: V, var channel: Int, expired: Long) {
  def isExpired: Boolean = System.currentTimeMillis() - expired > 0
}

case class TransactionConf(channelAll: Int = 1, batchSize: Int = 50,
                           maxAge: Duration = 1.minutes, maxCacheSize: Int = 100000)

object TransactionConf {

  import TransactionProps._

  def apply(conf: Source): TransactionConf = {
    val props = conf.props
    val channelSize = props.channelSize
    val batchSize = props.batchSize
    val maxAge = props.maxAge
    val maxCacheSize = props.maxCacheSize
    TransactionConf(
      channelAll = Int.MaxValue >>> (31 - channelSize),
      batchSize = batchSize,
      maxAge = maxAge,
      maxCacheSize = maxCacheSize
    )
  }
}

trait Transaction[T, V] extends AutoCloseable {
  def id: Int

  def begin(batchCoordinate: V, data: T)(handler: TeleporterMessage[T] ⇒ Unit)(implicit actorRef: ActorRef): Unit

  def end(id: TId)

  def recovery(): Iterator[TeleporterMessage[T]]

  def timeOutCheck(): Unit

  def isComplete: Boolean

  protected def doCommit(point: BatchInfo[V]): Unit

  def complete(): Unit

  def close(): Unit = {}
}

trait BatchCommitTransaction[T, V] extends Transaction[T, V] with LazyLogging {
  implicit val recoveryPoint: RecoveryPoint[V]
  val transactionConf: TransactionConf
  private val transactionCache = mutable.LongMap[TransactionRecord[T,V]]()
  protected val batchGrouped = mutable.LongMap[BatchInfo[V]]()
  private var seqNr: Long = 0
  private var recoveryCache: Iterator[TeleporterMessage[T]] = Iterator.empty
  private var lastCheck = System.currentTimeMillis()

  override def begin(batchCoordinate: V, data: T)(handler: TeleporterMessage[T] ⇒ Unit)(implicit actorRef: ActorRef): Unit = {
    val tId = TId(id, seqNr, 1)
    val msg = TeleporterMessage(tId, actorRef, data)
    batchCollect(seqNr, batchCoordinate)
    val record = TransactionRecord(record = msg, batchCoordinate = batchCoordinate, channel = 1, expired = System.currentTimeMillis() + transactionConf.maxAge.toMillis)
    transactionCache +=(seqNr, record)
    handler(msg)
    seqNr += 1
    if (lastCheck + transactionConf.maxAge.toMillis < System.currentTimeMillis()) {
      timeOutCheck()
      lastCheck = System.currentTimeMillis()
    }
  }

  protected def batchCollect(seqNr: Long, batchCoordinate: V): Unit = {
    if (seqNr % transactionConf.batchSize == 0) {
      batchGrouped +=(seqNr / transactionConf.batchSize, BatchInfo(Seq(batchCoordinate), 0))
    }
  }

  private def transStatus = s"$id transactionCache size:${transactionCache.size}, batch group size:${batchGrouped.size}, recovery size:${recoveryCache.size}, curr seqNr:$seqNr"

  override def end(tId: TId): Unit = {
    val seqNr = tId.seqNr
    transactionCache.contains(seqNr) match {
      case true ⇒
        val recordOpt = transactionCache.get(seqNr)
        if (recordOpt.isEmpty) {
          logger.warn(s"$id, not matched seqNr: $seqNr")
          return
        }
        val record = recordOpt.get
        val mergeChannelId = record.channel | tId.channelId
        if (transactionConf.channelAll == mergeChannelId) {
          transactionCache -= seqNr
          val batchNum = seqNr / transactionConf.batchSize
          val batchInfo = batchGrouped(batchNum)
          if (batchInfo.count == transactionConf.batchSize - 1) {
            doCommit(batchInfo)
            batchGrouped -= batchNum
          } else {
            batchInfo.count += 1
          }
        } else {
          record.channel = mergeChannelId
        }
      case false ⇒ logger.warn(s"This tId:$id not exists")
    }
  }

  override def recovery(): Iterator[TeleporterMessage[T]] = recoveryCache

  def timeOutCheck(): Unit = {
    logger.info(transStatus)
    if (!recoveryCache.hasNext) {
      recoveryCache = transactionCache.values.filter(_.isExpired).map(_.record).toIterator
      if (recoveryCache.nonEmpty) {
        logger.warn(s"$id timeout message size:${recoveryCache.size}")
      }
    }
  }

  override def doCommit(point: BatchInfo[V]): Unit = {
    recoveryPoint.save(id, point)
  }

  override def isComplete: Boolean = {
    timeOutCheck()
    transactionCache.isEmpty
  }

  override def complete(): Unit = recoveryPoint.complete(id)
}
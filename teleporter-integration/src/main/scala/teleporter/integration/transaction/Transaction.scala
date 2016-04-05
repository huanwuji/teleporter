package teleporter.integration.transaction

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.conf.{Conf, TransactionProps}
import teleporter.integration.core.{TId, TeleporterCenter, TeleporterMessage}

import scala.collection.mutable
import scala.concurrent.duration.{Duration, _}

/**
 * author: huanwuji
 * created: 2015/9/4.
 */
case class BatchInfo[B](var batchCoordinates: Seq[B], var count: Int, createdTime: Long = System.currentTimeMillis(), var completedTime: Long = 0) {
  def isComplete = completedTime != 0
}

private[transaction] case class TransactionRecord[T, V](record: TeleporterMessage[T], batchCoordinate: V, var channel: Int, expired: Long) {
  def isExpired: Boolean = System.currentTimeMillis() - expired > 0
}

case class TransactionConf(channelAll: Int = 1, batchSize: Int = 50, commitDelay: Option[Duration],
                           maxAge: Duration = 1.minutes, maxCacheSize: Int = 100000, recoveryPointEnabled: Boolean = true, timeoutRetry: Boolean = true)

object TransactionConf {
  def apply(props: TransactionProps): TransactionConf = {
    TransactionConf(
      channelAll = Int.MaxValue >>> (31 - props.channelSize),
      batchSize = props.batchSize,
      commitDelay = props.commitDelay,
      maxAge = props.maxAge,
      maxCacheSize = props.maxCacheSize,
      recoveryPointEnabled = props.recoveryPointEnabled,
      timeoutRetry = props.timeoutRetry
    )
  }
}

trait Transaction[T, V] extends AutoCloseable {
  def id: Int

  def begin(batchCoordinate: V, data: T)(handler: TeleporterMessage[T] ⇒ Unit)(implicit actorRef: ActorRef): Unit

  def end(id: TId)

  def recovery: Iterator[TeleporterMessage[T]]

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
    val record = TransactionRecord(record = msg, batchCoordinate = batchCoordinate, channel = 1, expired = expiredTime)
    transactionCache +=(seqNr, record)
    handler(msg)
    seqNr += 1
    if (lastCheck + transactionConf.maxAge.toMillis < System.currentTimeMillis()) {
      timeOutCheck()
      lastCheck = System.currentTimeMillis()
      //When data is too little, will always can't commit,so try to commit by date, ex:api,database sync etc
      val minRecord = transactionCache.minBy(_._1)._2
      doCommit(BatchInfo(Seq(minRecord.batchCoordinate), 1))
    }
  }

  private def expiredTime() = System.currentTimeMillis() + transactionConf.maxAge.toMillis

  protected def batchCollect(seqNr: Long, batchCoordinate: V): Unit = {
    if (seqNr % transactionConf.batchSize == 0) {
      batchGrouped +=(seqNr / transactionConf.batchSize, BatchInfo(Seq(batchCoordinate), 0))
    }
  }

  private def transStatus = s"$id transactionCache size:${transactionCache.size}, batch group size:${batchGrouped.size}, recovery size:${recoveryCache.hasNext}, curr seqNr:$seqNr"

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
          batchInfo.count += 1
          if (batchInfo.count == transactionConf.batchSize) {
            batchInfo.completedTime = System.currentTimeMillis()
            doCommit(batchInfo)
          }
        } else {
          record.channel = mergeChannelId
        }
      case false ⇒ logger.debug(s"This tId:$tId not exists")
    }
  }

  override def recovery: Iterator[TeleporterMessage[T]] = recoveryCache

  def timeOutCheck(): Unit = {
    logger.info(transStatus)
    if (transactionConf.timeoutRetry && !recoveryCache.hasNext) {
      val timeoutRecords = transactionCache.filter(_._2.isExpired)
      if (timeoutRecords.nonEmpty) {
        logger.warn(s"$id timeout message size:${timeoutRecords.size}")
        recoveryCache = timeoutRecords.map {
          case e@(k, v) ⇒ transactionCache.update(k, v.copy(expired = expiredTime())); v.record
        }.toIterator
      }
    }
  }

  override def doCommit(point: BatchInfo[V]): Unit = {
    val commitDelay = None
    val min = batchGrouped.minBy(_._1)._1
    var curr = min
    var lastCommitKey: Option[Long] = None
    while (canCommit(batchGrouped.get(curr), commitDelay)) {
      lastCommitKey = Some(curr)
      curr += 1L
    }
    lastCommitKey.foreach(x ⇒ {
      recoveryPoint.save(id, batchGrouped(x))
      batchGrouped --= (min to x)
    })
  }

  private def canCommit(batchInfoOpt: Option[BatchInfo[V]], commitDelay: Option[Duration]): Boolean =
    batchInfoOpt.exists(batchInfo ⇒ batchInfo.isComplete && (commitDelay.isEmpty || commitDelay.exists(d ⇒ System.currentTimeMillis() - batchInfo.completedTime > d.toMillis)))

  override def isComplete: Boolean = {
    timeOutCheck()
    transactionCache.isEmpty
  }

  override def complete(): Unit = recoveryPoint.complete(id)
}

trait DefaultBatchCommitTransaction[T] extends BatchCommitTransaction[T, Conf.Source] {
  implicit val center: TeleporterCenter

  def loadConf: Conf.Source = center.sourceFactory.loadConf(id)

  protected var currConf = loadConf
  override val transactionConf: TransactionConf = TransactionConf(currConf.props)
  override implicit val recoveryPoint: RecoveryPoint[Conf.Source] = center.recoveryPoint(transactionConf)
}
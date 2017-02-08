package teleporter.integration.core

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.{Done, NotUsed}
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.utils.MapBean

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, _}
import scala.util.control.NonFatal

/**
  * Created by huanwuji 
  * date 2016/12/23.
  */
object SourceAckMetaBean {
  val FAck = "ack"
  val FChannelSize = "channelSize"
  val FBatchCoordinateCommitNum = "batchCoordinateCommitNum"
  val FCacheSize = "cacheSize"
  val FMaxAge = "maxAge"
}

class SourceAckMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {

  import SourceAckMetaBean._

  def ack: MapBean = apply[MapBean](FAck)

  def channelSize: Int = ack.get[Int](FChannelSize).getOrElse(1)

  def batchCoordinateCommitNum: Int = ack.get[Int](FBatchCoordinateCommitNum).getOrElse(512)

  def cacheSize: Int = ack.get[Int](FCacheSize).getOrElse(1024)

  def maxAge: Duration = ack.get[Duration](FMaxAge).getOrElse(2.minutes)
}

case class ConfirmData(data: Any, created: Long)

class BitSets(bitSets: Array[mutable.BitSet]) {
  def apply(idx: Int): Boolean = {
    !bitSets.exists(_ (idx) == false)
  }

  def apply(idx: Int, channel: Int): Boolean = {
    bitSets(channel)(idx)
  }

  def +=(idx: Int): Unit = bitSets.foreach(_ += idx)

  def -=(idx: Int, channel: Int): mutable.BitSet = bitSets(channel) -= idx

  def +=(idx: Int, channel: Int): Unit = {
    bitSets(channel) += idx
  }
}

object BitSets {
  def apply(size: Int, channelSize: Int) = new BitSets(Array.fill(size)(new mutable.BitSet(size)))
}

class RingPool(size: Int, channelSize: Int) extends Logging {
  val bitSets: BitSets = BitSets(size, channelSize)
  val elements: Array[ConfirmData] = new Array(size)
  private val usedCursor: AtomicLong = new AtomicLong(0)
  private val freeSpace: AtomicInteger = new AtomicInteger(size)
  private var canConfirmedCursor: Long = 0
  private var confirmedCursor: Long = 0

  def add(addElem: Long ⇒ Any): Long = {
    require(freeSpace.get() > 0, "Having no space to add")
    val currCursor = usedCursor.incrementAndGet()
    val ringIdx = (currCursor % size).toInt
    bitSets += ringIdx
    freeSpace.decrementAndGet()
    elements.update(ringIdx, ConfirmData(addElem(currCursor), System.currentTimeMillis()))
    currCursor
  }

  def remove(idx: Long, channel: Int): Unit = {
    if (idx > usedCursor.get() || idx < confirmedCursor) {
      logger.debug(s"Invalid $idx, used: $usedCursor, confirm: $confirmedCursor, This was be confirmed!")
    }
    val ringIdx = (idx % size).toInt
    if (bitSets(ringIdx, channel)) {
      bitSets -= (ringIdx, channel)
      if (bitSets(ringIdx)) {
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
      nextConfirmedCursor < usedCursor.get() && !bitSets((nextConfirmedCursor % size).toInt)
    }) {
      canConfirmedCursor += 1
    }
    canConfirmedCursor
  }

  def canConfirmedSize: Long = canConfirmed - confirmedCursor

  def unConfirmedSize: Long = usedCursor.get() - confirmedCursor

  def confirmed(): Unit = confirmed(canConfirmedCursor)

  def confirmed(confirmCursor: Long): Unit = {
    require(confirmCursor <= canConfirmed, s"Confirm cursor must less then canConfirmed, $confirmCursor, $canConfirmed")
    if (confirmCursor > this.confirmedCursor) {
      freeSpace.addAndGet((confirmCursor - this.confirmedCursor).toInt)
      confirmedCursor = confirmCursor
    } else {
      logger.debug(s"confirmCursor:$confirmCursor was great than ${this.confirmedCursor}")
    }
  }

  def remainingCapacity(): Int = freeSpace.get()
}

object RingPool {
  def apply(size: Int, channel: Int): RingPool = new RingPool(size, channel)
}

case class SourceAckConfig(
                            channelSize: Int,
                            cacheSize: Int,
                            batchCoordinateCommitNum: Int,
                            maxAge: Duration)

object SourceAckConfig {
  def apply(config: MapBean): SourceAckConfig = {
    val ackMetaBean = config.mapTo[SourceAckMetaBean]
    SourceAckConfig(
      channelSize = ackMetaBean.channelSize,
      cacheSize = ackMetaBean.cacheSize,
      batchCoordinateCommitNum = ackMetaBean.batchCoordinateCommitNum,
      maxAge = ackMetaBean.maxAge
    )
  }
}

object SourceAck {
  def flow[XY, T](id: Long, config: SourceAckConfig, commit: XY ⇒ Unit, finish: () ⇒ Unit)
                 (implicit center: TeleporterCenter): Flow[SourceMessage[XY, T], AckMessage[XY, T], NotUsed] = {
    Flow.fromGraph(new SourceAck[XY, T](id, config, commit, finish))
  }

  def flow[T](id: Long, config: MapBean)(implicit center: TeleporterCenter): Flow[SourceMessage[MapBean, T], AckMessage[MapBean, T], NotUsed] = {
    val context = center.context.getContext[SourceContext](id)
    Flow.fromGraph(new SourceAck[MapBean, T](
      id = id,
      config = SourceAckConfig(config),
      commit = coordinate ⇒ center.defaultSourceCheckPoint.save(context.key, coordinate),
      finish = () ⇒ center.defaultSourceCheckPoint.complete(context.key)
    ))
  }

  def flow[XY, T](id: Long, config: MapBean, checkPoint: CheckPoint[XY])(implicit center: TeleporterCenter): Unit = {
    val context = center.context.getContext[SourceContext](id)
    Flow.fromGraph(new SourceAck[XY, T](
      id = id,
      config = SourceAckConfig(config),
      commit = coordinate ⇒ checkPoint.save(context.key, coordinate),
      finish = () ⇒ checkPoint.complete(context.key)
    ))
  }

  def confirmFlow[T](): Flow[Message[T], Message[T], NotUsed] = {
    Flow[Message[T]].map {
      case m: AckMessage[_, _] ⇒ m.confirmed.invoke(m.id); m
    }
  }

  def confirmSink[T](): Sink[Message[T], Future[Done]] = {
    confirmFlow[T]().toMat(Sink.ignore)(Keep.right)
  }
}

class SourceAck[XY, T](id: Long, config: SourceAckConfig, commit: XY ⇒ Unit, finish: () ⇒ Unit)(implicit center: TeleporterCenter)
  extends GraphStage[FlowShape[SourceMessage[XY, T], AckMessage[XY, T]]] {
  var ringPool: RingPool = _
  val in: Inlet[SourceMessage[XY, T]] = Inlet[SourceMessage[XY, T]]("source.ack.in")
  val out: Outlet[AckMessage[XY, T]] = Outlet[AckMessage[XY, T]]("source.ack.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = Attributes.name("source.ack")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private def decider = inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      private val coordinates = mutable.Queue[(Long, XY)]()
      private var lastCoordinate: XY = _
      var self: StageActor = _
      val confirmed: AsyncCallback[TId] = getAsyncCallback[TId](tId ⇒ self.ref ! tId)
      var expiredMessages: Iterator[AckMessage[XY, T]] = Iterator.empty
      var lastCheck: Long = _

      @scala.throws[Exception](classOf[Exception])
      override def preStart(): Unit = {
        ringPool = RingPool(config.cacheSize, config.channelSize)
        var batchCoordinateCommitNum = 0
        self = getStageActor {
          case (_, tId: TId) ⇒
            ringPool.remove(tId.seqNr, tId.channelId)
            val canConfirmedIdx = ringPool.canConfirmed
            var latestCoordinate = coordinates.head
            while (coordinates.head._1 < canConfirmedIdx) {
              latestCoordinate = coordinates.dequeue()
              batchCoordinateCommitNum += 1
            }
            if (batchCoordinateCommitNum >= config.batchCoordinateCommitNum) {
              commit(latestCoordinate._2)
              ringPool.confirmed(latestCoordinate._1)
              batchCoordinateCommitNum = 0
            }
        }
      }

      override def onPush(): Unit = {
        if (ringPool.remainingCapacity() == 0) {
          if (!expiredMessages.hasNext) {
            expired()
          }
          if (expiredMessages.hasNext) {
            emit(out, expiredMessages.next())
            return
          }
          scheduleOnce('push, 1.second)
          return
        }
        val elem = grab(in)
        try {
          val seqNr = ringPool.add { idx ⇒
            val ackMessage = AckMessage(id = TId(id, idx), coordinate = elem.coordinate, data = elem.data, confirmed = confirmed)
            push(out, ackMessage)
            ackMessage
          }
          if (lastCoordinate != coordinates) {
            lastCoordinate = elem.coordinate
            coordinates += (seqNr → lastCoordinate)
          }
        } catch {
          case NonFatal(ex) ⇒ decider(ex) match {
            case Supervision.Stop ⇒ failStage(ex)
            case _ ⇒ pull(in)
          }
        }
      }

      override def onPull(): Unit = pull(in)

      private def expired(): Unit = {
        expiredMessages = ringPool.elements
          .filter(x ⇒ x != null && System.currentTimeMillis() - x.created > config.maxAge.toMillis)
          .map(_.data.asInstanceOf[AckMessage[XY, T]]).toIterator
      }

      @scala.throws[Exception](classOf[Exception])
      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case 'push ⇒ onPush()
        }
      }

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFinish(): Unit = {
        if (ringPool.unConfirmedSize == 0) {
          finish()
          completeStage()
        } else {

        }
      }

      setHandlers(in, out, this)
    }
}
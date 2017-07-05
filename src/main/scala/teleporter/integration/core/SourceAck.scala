package teleporter.integration.core

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.Terminated
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.{Done, NotUsed}
import com.google.common.collect.EvictingQueue
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.core.TeleporterConfigActor.LoadStream
import teleporter.integration.core.TeleporterContext.Update
import teleporter.integration.utils.{MapBean, MapMetaBean}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, _}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by huanwuji 
  * date 2016/12/23.
  */
class SourceAckMetaBean(override val underlying: Map[String, Any]) extends SourceMetaBean(underlying) {
  val FAck = "ack"
  val FChannelSize = "channelSize"
  val FCommitInterval = "commitInterval"
  val FCacheSize = "cacheSize"
  val FMaxAge = "maxAge"

  def ack: MapBean = apply[MapBean](FAck)

  def channelSize: Int = ack.get[Int](FChannelSize).getOrElse(1)

  def commitInterval: Long = ack.get[Duration](FCommitInterval).getOrElse(1.minute).toMillis

  def cacheSize: Int = ack.get[Int](FCacheSize).getOrElse(1024)

  def maxAge: Duration = ack.get[Duration](FMaxAge).getOrElse(2.minutes)
}

case class ConfirmData(data: Any, created: Long)

class BitSets(bitSets: Array[mutable.BitSet]) {
  def apply(idx: Int): Boolean = {
    !bitSets.exists(!_ (idx))
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
  def apply(size: Int, channelSize: Int) = new BitSets(Array.fill(channelSize)(new mutable.BitSet(size)))
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
      if (!bitSets(ringIdx)) {
        elements.update(ringIdx, null)
        canConfirmed
      }
    } else {
      logger.debug(s"Invalid $idx, This was be confirmed!")
    }
  }

  def canConfirmCursor: Long = canConfirmedCursor

  private def canConfirmed: Long = {
    while ( {
      val nextConfirmedCursor = canConfirmedCursor + 1
      nextConfirmedCursor <= usedCursor.get() && !bitSets((nextConfirmedCursor % size).toInt)
    }) {
      canConfirmedCursor += 1
    }
    canConfirmedCursor
  }

  def canConfirmedSize: Long = canConfirmedCursor - confirmedCursor

  def unConfirmedSize: Long = usedCursor.get() - confirmedCursor

  def confirmEnd(): Boolean = {
    if (canConfirmedCursor == usedCursor.get()) {
      confirmed()
      true
    } else false
  }

  def confirmed(): Unit = confirmed(canConfirmedCursor)

  def confirmed(confirmCursor: Long): Unit = {
    require(confirmCursor <= canConfirmedCursor, s"Confirm cursor must less then canConfirmed, $confirmCursor, $canConfirmedCursor")
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
                            commitInterval: Long,
                            maxAge: Duration)

object SourceAckConfig {
  def apply(config: MapBean): SourceAckConfig = {
    val ackMetaBean = config.mapTo[SourceAckMetaBean]
    SourceAckConfig(
      channelSize = ackMetaBean.channelSize,
      cacheSize = ackMetaBean.cacheSize,
      commitInterval = ackMetaBean.commitInterval,
      maxAge = ackMetaBean.maxAge
    )
  }
}

object SourceAck extends Logging {
  def flow[XY, T](id: Long, config: SourceAckConfig, commit: XY ⇒ Unit, finish: XY ⇒ Unit)
                 (implicit center: TeleporterCenter): Flow[SourceMessage[XY, T], AckMessage[XY, T], NotUsed] = {
    Flow.fromGraph(new SourceAck[XY, T](id, config, commit, finish))
  }

  def flow[T](id: Long, config: MapBean)(implicit center: TeleporterCenter): Flow[SourceMessage[MapBean, T], AckMessage[MapBean, T], NotUsed] = {
    import center.system.dispatcher
    val context = center.context.getContext[SourceContext](id)
    Flow.fromGraph(new SourceAck[MapBean, T](
      id = id,
      config = SourceAckConfig(config),
      commit = {
        coordinate ⇒
          center.defaultSourceSavePoint.save(context.key, coordinate)
            .onComplete(saveComplete(context, coordinate, _))
      },
      finish = {
        coordinate ⇒
          center.defaultSourceSavePoint.complete(context.key, coordinate)
            .onComplete(saveComplete(context, coordinate, _))
      }
    ))
  }

  private def saveComplete(context: SourceContext, coordinate: MapBean, result: Try[Boolean])(implicit center: TeleporterCenter): Unit = {
    result match {
      case Success(v) ⇒
        if (v) {
          center.context.ref ! Update(context.copy(config = MapMetaBean[SourceMetaBean](coordinate)))
        } else {
          center.configRef ! LoadStream(context.key)
        }
      case Failure(ex) ⇒ logger.info(s"${context.key} save failure", ex)
    }
  }

  def flow[XY, T](id: Long, config: MapBean, savePoint: SavePoint[XY])(implicit center: TeleporterCenter): Unit = {
    val context = center.context.getContext[SourceContext](id)
    Flow.fromGraph(new SourceAck[XY, T](
      id = id,
      config = SourceAckConfig(config),
      commit = coordinate ⇒ savePoint.save(context.key, coordinate),
      finish = coordinate ⇒ savePoint.complete(context.key, coordinate)
    ))
  }

  def confirmFlow[T](): Flow[Message[T], Message[T], NotUsed] = {
    Flow[Message[T]].map {
      case m: AckMessage[_, _] ⇒ m.confirmed(m.id); m
      case m ⇒ m
    }
  }

  def confirmSink[T](): Sink[Message[T], Future[Done]] = {
    confirmFlow[T]().toMat(Sink.ignore)(Keep.right)
  }
}

class SourceAck[XY, T](id: Long, config: SourceAckConfig, commit: XY ⇒ Unit, finish: XY ⇒ Unit)
  extends GraphStage[FlowShape[SourceMessage[XY, T], AckMessage[XY, T]]] with Logging {
  var ringPool: RingPool = _
  val in: Inlet[SourceMessage[XY, T]] = Inlet[SourceMessage[XY, T]]("source.ack.in")
  val out: Outlet[AckMessage[XY, T]] = Outlet[AckMessage[XY, T]]("source.ack.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = Attributes.name("source.ack")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private def decider = inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      private val coordinates = EvictingQueue.create[(Long, XY)](config.cacheSize)
      private var lastCoordinate: XY = _
      var self: StageActor = _
      val confirmed: TId ⇒ Unit = tId ⇒ self.ref ! tId
      var expiredMessages: Iterator[AckMessage[XY, T]] = Iterator.empty
      @volatile var fullWait = false

      @scala.throws[Exception](classOf[Exception])
      override def preStart(): Unit = {
        ringPool = RingPool(config.cacheSize, config.channelSize)
        var lastCommitTime = System.currentTimeMillis()
        var unCommitCount = 0
        self = getStageActor {
          case (_, tId: TId) ⇒
            ringPool.remove(tId.seqNr, tId.channelId)
            var latestCoordinate: (Long, XY) = null
            while (!coordinates.isEmpty && (coordinates.peek()._1 <= ringPool.canConfirmCursor)) {
              latestCoordinate = coordinates.poll()
              unCommitCount += 1
            }
            if (latestCoordinate != null) {
              if (System.currentTimeMillis() - lastCommitTime >= config.commitInterval) {
                commit(latestCoordinate._2)
                ringPool.confirmed(latestCoordinate._1)
                lastCommitTime = System.currentTimeMillis()
                unCommitCount = 0
              } else if (unCommitCount > 1) {
                ringPool.confirmed(latestCoordinate._1)
                unCommitCount = 0
              }
            }
            if (fullWait) {
              fullWait = false
              if (isAvailable(in) && isAvailable(out)) onPush()
            }
            tryFinish()
          case (_, Terminated(_)) ⇒ logger.info(s"$id stageActor will terminated")
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
          fullWait = true
          return
        }
        val elem = grab(in)
        try {
          val seqNr = ringPool.add { idx ⇒
            val ackMessage = Message.ack(id = TId(id, idx), coordinate = elem.coordinate, data = elem.data, confirmed = confirmed)
            push(out, ackMessage)
            ackMessage
          }
          if (lastCoordinate != coordinates) {
            lastCoordinate = elem.coordinate
            coordinates.offer(seqNr → lastCoordinate)
          }
        } catch {
          case NonFatal(ex) ⇒ decider(ex) match {
            case Supervision.Stop ⇒ failStage(ex)
            case _ ⇒ pull(in)
          }
        }
      }

      override def onPull(): Unit = if (!isClosed(in)) pull(in)

      private def expired(): Unit = {
        expiredMessages = ringPool.elements
          .filter(x ⇒ x != null && System.currentTimeMillis() - x.created > config.maxAge.toMillis)
          .map(_.data.asInstanceOf[AckMessage[XY, T]]).toIterator
      }

      @scala.throws[Exception](classOf[Exception])
      override def onUpstreamFinish(): Unit = tryFinish()

      private def tryFinish(): Unit = {
        if (ringPool.confirmEnd() && isClosed(in) && !isClosed(out)) {
          logger.info("Is down stream really finish")
          if (lastCoordinate != null) finish(lastCoordinate)
          super.onUpstreamFinish()
        }
      }

      setHandlers(in, out, this)
    }
}
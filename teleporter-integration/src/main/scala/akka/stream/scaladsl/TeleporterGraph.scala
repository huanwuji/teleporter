package akka.stream.scaladsl

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable

/**
 * Author: kui.dai
 * Date: 2015/12/3.
 */
class Shunt[T](val outputPorts: Int, shuntLogic: T ⇒ Int) extends GraphStage[UniformFanOutShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("Shunt.in")
  val out: immutable.IndexedSeq[Outlet[T]] = Vector.tabulate(outputPorts)(i ⇒ Outlet[T]("Shunt.out" + i))
  override val shape: UniformFanOutShape[T, T] = UniformFanOutShape(in, out: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var pendingCount = outputPorts
    private var downstreamsRunning = outputPorts

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        val idx = shuntLogic(elem)
        val o = out(idx)
        if (!isClosed(o)) {
          push(o, elem)
        }
      }
    })

    private def tryPull(): Unit =
      if (pendingCount == 0 && !hasBeenPulled(in)) pull(in)

    {
      var idx = 0
      val itr = out.iterator
      while (itr.hasNext) {
        val out = itr.next()
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (pendingCount != 0) pendingCount -= 1
            tryPull()
          }

          override def onDownstreamFinish() = {
            downstreamsRunning -= 1
            if (downstreamsRunning == 0) completeStage()
          }
        })
        idx += 1
      }
    }
  }

  override def toString = "Shunt"
}

object Shunt {
  def apply[T](outputPorts: Int, shuntLogic: T ⇒ Int): Shunt[T] = new Shunt[T](outputPorts, shuntLogic)
}

object TeleporterGraph
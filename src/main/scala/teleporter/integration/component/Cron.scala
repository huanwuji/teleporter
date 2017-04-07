package teleporter.integration.component

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.{ActorRef, ActorSystem, Cancellable, Terminated}
import akka.stream.Attributes.name
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.markatta.akron.CronTab.{Scheduled, UnSchedule}
import com.markatta.akron.{CronExpression, CronTab}

/**
  * Created by huanwuji 
  * date 2017/3/20.
  */
object Cron {
  def cronRef(name: String)(implicit system: ActorSystem): ActorRef = system.actorOf(CronTab.props, name)

  def source[T](cronRef: ActorRef, cron: String, tick: T): Source[T, Cancellable] = {
    Source.fromGraph(new CronSource[T](cronRef, cron, tick))
  }
}

class CronSource[T](cronRef: ActorRef, cron: String, val tick: T)
  extends GraphStageWithMaterializedValue[SourceShape[T], Cancellable] {
  override val shape = SourceShape(Outlet[T]("CronSource.out"))
  val out: Outlet[T] = shape.out

  override def initialAttributes: Attributes = name("cronSource")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Cancellable) = {

    val logic = new GraphStageLogicWithLogging(shape) with Cancellable {
      val cancelled = new AtomicBoolean(false)
      val cancelCallback: AtomicReference[Option[AsyncCallback[Unit]]] = new AtomicReference(None)
      var jobId: UUID = _

      override def preStart(): Unit = {
        cancelCallback.set(Some(getAsyncCallback[Unit](_ ⇒ completeStage())))
        if (cancelled.get) {
          completeStage()
        } else {
          val self = getStageActor {
            case (_, `tick`) ⇒ if (isAvailable(out) && !isCancelled) push(out, tick)
            case (_, Scheduled(id, _, _)) ⇒ jobId = id
            case (_, Terminated(_)) ⇒ log.info(s"$toString stageActor will terminated")
          }
          cronRef.tell(CronTab.Schedule(self.ref, tick, CronExpression(cron)), self.ref)
        }
      }

      setHandler(out, eagerTerminateOutput)

      override def cancel(): Boolean = {
        val success = !cancelled.getAndSet(true)
        if (success) {
          cancelCallback.get.foreach(_.invoke(()))
          cronRef ! UnSchedule(jobId)
        }
        success
      }

      override def isCancelled: Boolean = cancelled.get

      override def toString: String = "CronSourceLogic"
    }

    (logic, logic)
  }

  override def toString: String = s"CronSource($cron, $tick)"
}
package teleporter.integration.component

import java.util.UUID

import akka.actor.{ActorLogging, Cancellable}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import com.markatta.akron.CronTab.UnSchedule
import com.markatta.akron.{CronExpression, CronTab}
import teleporter.integration.SourceControl
import teleporter.integration.SourceControl.{CompleteThenStop, ErrorThenStop, Notify, Reload}
import teleporter.integration.StreamControl.StreamCmpRegister
import teleporter.integration.conf.Conf._
import teleporter.integration.conf.ScheduleProps
import teleporter.integration.core.{Component, Enforcer, TId, TeleporterMessage}
import teleporter.integration.transaction.DefaultBatchCommitTransaction

import scala.annotation.tailrec
import scala.collection.Iterator
import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2016/3/18.
 */
trait SchedulePublisher[T, A]
  extends ActorPublisher[TeleporterMessage[T]]
  with DefaultBatchCommitTransaction[T]
  with Component with ActorLogging {

  import context.{dispatcher, system}

  val scheduleProps: ScheduleProps = currConf.props
  private var cancellable: Cancellable = null
  private var cronJobId: UUID = null
  private var iterator: Iterator[T] = Iterator.empty
  protected val streamContext = center.streamContext(currConf.id)
  protected val enforcer = Enforcer(currConf)
  protected var address: A = _
  protected val streamManager = center.streamManager(currConf.taskId)
  protected var pause = false

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    init()
  }

  def init(): Unit = {
    scheduleClear()
    try {
      pause = false
      address = center.addressing[A](currConf.addressId.get, currConf)
      if (scheduleProps.isTimerRoller && scheduleProps.isContinuous) {
        cancellable = TimeRoller.timeRollerSchedule(currConf.props)
      }
      scheduleProps.cron match {
        case Some(cron) ⇒ center.crontab ! CronTab.Schedule(self, SourceControl.Reload, CronExpression(cron))
        case None ⇒ iterator = Rollers(loadConf.props, _handler).iterator
      }
      center.streamManager(currConf.taskId) ! StreamCmpRegister(currConf)
    } catch {
      case e: Exception ⇒ errorProcess("init", e)
    }
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"preRestart: ${reason.getLocalizedMessage}", reason.getLocalizedMessage)
    super.preRestart(reason, message)
  }

  override def receive: Receive = {
    case Request(n) ⇒ deliver()
    case tId: TId ⇒ end(tId)
    case Notify ⇒ deliver()
    case Reload ⇒ init(); deliver()
    case cronSchedule@CronTab.Scheduled(jobId, recipient, message) ⇒ cronJobId = jobId; logger.info(s"CronSchedule $cronSchedule was start")
    case ErrorThenStop(reason: Throwable) ⇒
      logger.info(s"$currConf error then stop")
      onErrorThenStop(reason)
    case CompleteThenStop ⇒
      logger.info(s"$currConf will complete then stop")
      onCompleteThenStop()
    case x ⇒ log.warning("can't arrived, {}", x)
  }

  @tailrec
  final def deliver(): Unit = {
    if (totalDemand > 0 && isActive && !pause) {
      if (recovery.hasNext) {
        onNext(recovery.next())
        deliver()
      } else {
        if (iterator.hasNext && streamContext.currParallelismSource.get() < streamContext.maxParallelismSource) {
          try {
            begin(currConf, iterator.next()) {
              msg ⇒ onNext(msg)
            }
          } catch {
            case e: Exception ⇒ errorProcess("dataSend", e)
          }
          deliver()
        } else {
          if (!scheduleProps.isContinuous) {
            doComplete()
          }
        }
      }
    }
  }

  private def doComplete(): Unit = {
    if (!pause) {
      if (isComplete) {
        complete()
        if (scheduleProps.cron.isEmpty) {
          onCompleteThenStop()
        }
      } else {
        context.system.scheduler.scheduleOnce(5.seconds, self, SourceControl.Notify)
      }
    }
  }

  private def _handler(props: Props): Iterator[T] = {
    currConf = currConf.copy(props = props)
    try {
      handler(props)
    } catch {
      case e: Exception ⇒ errorProcess("dataIterator", e); Iterator.empty
    }
  }

  def errorProcess(errorId: String, cause: Exception) = {
    pause = true
    enforcer.execute(errorId, cause)
  }

  def handler(props: Props): Iterator[T]

  private def scheduleClear(): Unit = {
    if (cancellable != null) {
      log.info(s"$id last time schedule cancel")
      cancellable.cancel()
      cancellable = null
    }
    if (cronJobId != null) {
      log.info(s"$id last crontab cancel")
      center.crontab ! UnSchedule(cronJobId)
      cronJobId = null
    }
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    log.info(s"${currConf.name} will stop")
    scheduleClear()
  }
}
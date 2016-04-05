package teleporter.integration.core

import akka.actor.{ActorRef, Cancellable}
import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.SourceControl.ErrorThenStop
import teleporter.integration.StreamControl.Restart
import teleporter.integration.component.SubscriberSupport
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core.SinkRetry.RetryState
import teleporter.integration.{SinkControl, SourceControl}

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.util.matching.Regex

/**
 * Author: kui.dai
 * Date: 2016/3/23.
 */
object ExceptionLevel extends Enumeration {
  type ExceptionLevel = Value
  val DEATH, DANGER, WARNING, INFO = Value
}

case class ExceptionRule[T](conf: T, idMatch: Regex, typeMatch: Regex, messageMatch: Regex, handler: ExceptionHandler[T], level: ExceptionLevel.ExceptionLevel)

object ExceptionRule {
  val delayRestart = "restart\\(([^)]*)\\)".r
  val delayRetry = "retry\\(([^)]*)\\)".r
  val delayRetries = "retry\\(([^)]*),([^)]*)\\)".r
  val ALL_MATCH = ".".r

  def apply(conf: Conf.Source, config: Config): ExceptionRule[Conf.Source] = {
    val id = config.getString("id")
    val errorType = config.getString("type")
    val errorMessage = config.getString("message")
    val cmd = config.getString("cmd")
    val level = config.getString("level")
    val handler: ExceptionHandler[Conf.Source] = cmd match {
      case "restart" ⇒ new SourceRestart()
      case "stop" ⇒ new SourceStop
      case "retry" ⇒ new SourceRetry
      case "ignore" ⇒ new SourceIgnore
      case delayRetry(delay) ⇒ new SourceRetry(Duration(delay))
      case delayRetries(delay) ⇒ new SourceRetry(Duration(delay))
      case delayRestart(delay) ⇒ new SourceRestart(Duration(delay))
    }
    ExceptionRule(conf, id.r, errorType.r, errorMessage.r, handler, ExceptionLevel.withName(level))
  }

  def apply(conf: Conf.Sink, config: Config): ExceptionRule[Conf.Sink] = {
    val id = config.getString("id")
    val errorType = config.getString("type")
    val errorMessage = config.getString("message")
    val cmd = config.getString("cmd")
    val level = config.getString("level")
    val handler: ExceptionHandler[Conf.Sink] = cmd match {
      case "restart" ⇒ new SinkRestart
      case "stop" ⇒ new SinkStop
      case "retry" ⇒ new SinkRetry
      case "ignore" ⇒ new SinkIgnore
      case delayRetries(delay, maxNrOfRetries) ⇒ new SinkRetry(Duration(delay), maxNrOfRetries.toInt)
      case delayRetry(delay) ⇒ new SinkRetry(Duration(delay))
      case delayRestart(delay) ⇒ new SinkRestart(Duration(delay))
    }
    ExceptionRule(conf, id.r, errorType.r, errorMessage.r, handler, ExceptionLevel.withName(level))
  }
}

trait ExceptionHandler[T] {
  def handle(conf: T, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext)
}

class SourceRestart(delay: Duration = Duration.Zero, message: Any = None) extends ExceptionHandler[Conf.Source] {
  override def handle(conf: Conf.Source, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    center.streamManager(conf.taskId) ! Restart(center.configFactory.stream(conf.streamId), delay)
  }

  override def toString: String = s"restart($delay)"
}

class SourceStop extends ExceptionHandler[Conf.Source] {
  override def handle(conf: Conf.Source, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    self ! ErrorThenStop(cause)
  }
}

class SourceRetry(delay: Duration = Duration.Zero) extends ExceptionHandler[Conf.Source] {
  override def handle(conf: Conf.Source, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    delay match {
      case d: FiniteDuration ⇒ center.system.scheduler.scheduleOnce(d, self, SourceControl.Reload)
      case _ ⇒ self ! SourceControl.Reload
    }
  }

  override def toString: String = s"retry($delay)"
}

class SourceIgnore extends ExceptionHandler[Conf.Source] {
  override def handle(conf: Conf.Source, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {}
}

class SinkStop extends ExceptionHandler[Conf.Sink] {
  override def handle(conf: Conf.Sink, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    self ! SinkControl.Cancel
  }
}

class SinkRestart(delay: Duration = Duration.Zero) extends ExceptionHandler[Conf.Sink] {
  override def handle(conf: Conf.Sink, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    center.streamManager(conf.taskId) ! Restart(center.configFactory.stream(conf.streamId), delay)
  }

  override def toString: String = s"restart($delay)"
}

class SinkRetry(delay: Duration = Duration.Zero, maxNrOfRetries: Int = Int.MaxValue) extends ExceptionHandler[Conf.Sink] with LazyLogging {
  val retriesQueue = TrieMap[Any, RetryState]()
  var cancellable: Cancellable = _

  def delayRetryManager()(implicit subscriberRef: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    if (cancellable != null) {
      return
    }
    if (delay.isFinite()) {
      this.synchronized {
        val finiteDuration = delay.asInstanceOf[FiniteDuration]
        if (cancellable == null) {
          center.system.scheduler.schedule(finiteDuration, finiteDuration) {
            //clear retry count
            retriesQueue.foreach {
              case entry@(needRetryMessage, state) ⇒
                if (state.retries == maxNrOfRetries) {
                  overMaxRetriesHandler(needRetryMessage)
                  logger.warn(s"Ignore data after retry ${state.retries} times, $needRetryMessage")
                } else {
                  retriesQueue.update(needRetryMessage, RetryState(state.retries + 1, System.currentTimeMillis()))
                }
                val nowOffset = System.currentTimeMillis() - state.retriedTime
                if (nowOffset > delay.toMillis * 5) {
                  retriesQueue -= needRetryMessage
                }
            }
          }
        }
        if (retriesQueue.isEmpty) {
          if (cancellable != null) {
            cancellable.cancel()
            cancellable = null
          }
        }
      }
    }
  }

  def overMaxRetriesHandler(message: Any)(implicit subscriberRef: ActorRef): Unit = {
    message match {
      case onNext: OnNext ⇒
        onNext.element match {
          case teleporterMessage: TeleporterMessage[Any] ⇒ teleporterMessage.toNext(teleporterMessage)
          case _ ⇒ //ignore
        }
        subscriberRef ! SubscriberSupport.Reply(Right(onNext))
      case _ ⇒ //ignore
    }
    retriesQueue -= message
  }

  override def handle(conf: Conf.Sink, cause: Throwable, message: Any = None)(implicit subscriberRef: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    val state = retriesQueue.getOrElseUpdate(message, RetryState(-1, System.currentTimeMillis()))
    if (state.retries == maxNrOfRetries) {
      overMaxRetriesHandler(message)
      logger.warn(s"Ignore data after retry ${state.retries} times, $message")
    } else {
      retriesQueue.update(message, RetryState(state.retries + 1, System.currentTimeMillis()))
      delay match {
        case d: FiniteDuration ⇒
          center.system.scheduler.scheduleOnce(d, subscriberRef, message)
          delayRetryManager()
        case _ ⇒ subscriberRef ! message
      }
    }
  }

  override def toString: String = s"retry($delay, $maxNrOfRetries)"
}

object SinkRetry {

  case class RetryState(retries: Int, retriedTime: Long)

}

class SinkIgnore extends ExceptionHandler[Conf.Sink] {
  override def handle(conf: Conf.Sink, cause: Throwable, message: Any = None)(implicit subscriberRef: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    message match {
      case onNext: OnNext ⇒
        onNext.element match {
          case teleporterMessage: TeleporterMessage[Any] ⇒ teleporterMessage.toNext(teleporterMessage)
          case _ ⇒ //ignore
        }
        subscriberRef ! SubscriberSupport.Reply(Right(onNext))
      case _ ⇒ //ignore
    }
  }
}

class Enforcer[T](conf: T, rules: Seq[ExceptionRule[T]], defaultHandler: ExceptionHandler[T])(implicit subscriberRef: ActorRef, center: TeleporterCenter, ec: ExecutionContext) extends LazyLogging {
  def execute(errorId: String, cause: Throwable, message: Any = None): Unit = {
    logger.error(cause.getMessage, cause)
    if (!rules.exists {
      rule ⇒
        rule.idMatch.findFirstIn(errorId).flatMap {
          errorIdMatched ⇒
            rule.typeMatch.findFirstIn(cause.getClass.getName).flatMap {
              typeMatched ⇒ rule.messageMatch.findFirstIn(String.valueOf(cause.getMessage))
            }
        }.exists {
          x ⇒
            logger.info(s"use handler: $rule")
            rule.handler.handle(conf, cause, message)
            true
        }
    }) {
      logger.info(s"use default handler: $defaultHandler, $conf")
      defaultHandler.handle(conf, cause, message)
    }
  }
}

object Enforcer extends PropsSupport with LazyLogging {
  val errorRule = "(.*?):(.*?):(.*?):(.*?):(.*?)".r
  val delayRestart = "restart\\(([^)]*)\\)".r
  val delayRetry = "retry\\(([^)]*)\\)".r
  val ALL_MATCH = ".".r

  /**
   * *: restart|stop
   * runtimeException: restart|stop|retry|restart(1.seconds)
   * regex: handler
   * Unknown column: stop
   */
  def apply(conf: Conf.Source)(implicit subscriberRef: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Enforcer[Conf.Source] = {
    import teleporter.integration.conf.SourceProps._
    val rules = conf.props.errorRules.map(ConfigFactory.parseString)
      .map(_.getConfigList("rules").map(ExceptionRule(conf, _))).getOrElse(Seq.empty)
    new Enforcer(conf, rules, new SourceRetry(30.seconds))
  }

  def apply(conf: Conf.Sink)(implicit subscriberRef: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Enforcer[Conf.Sink] = {
    import teleporter.integration.conf.SinkProps._
    val rules = conf.props.errorRules.map(ConfigFactory.parseString)
      .map(_.getConfigList("rules").map(ExceptionRule(conf, _))).getOrElse(Seq.empty)
    new Enforcer(conf, rules, new SinkRetry(1.minute))
  }
}
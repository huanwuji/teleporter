package teleporter.integration.core

import akka.actor.ActorRef
import akka.stream.actor.ActorSubscriberMessage.OnNext
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.{ScheduleActorPublisherMessage, SubscriberMessage}
import teleporter.integration.core.Streams.{DelayCommand, Stop}
import teleporter.integration.metrics.Metrics.{Measurement, Tag, Tags}
import teleporter.integration.metrics.MetricsCounter

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.matching.Regex

/**
  * Author: kui.dai
  * Date: 2016/3/23.
  */
trait ExceptionHandler {
  def handle(key: String, cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext)
}

object ExceptionHandler {

  object Stream {

    class StreamCommand(streamKey: String, delayCommand: DelayCommand) extends ExceptionHandler {
      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
        val streamContext = center.context.getContext[StreamContext](streamKey)
        streamContext.task().streamSchedule ! delayCommand
      }
    }

  }

  object Source {

    class Reload extends ExceptionHandler {
      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
        self ! ScheduleActorPublisherMessage.ClientInit
      }
    }

    class Retry(delay: Duration = Duration.Zero) extends ExceptionHandler {
      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
        delay match {
          case d: FiniteDuration ⇒ center.system.scheduler.scheduleOnce(d, self, ScheduleActorPublisherMessage.Grab)
          case _ ⇒ self ! ScheduleActorPublisherMessage.Grab
        }
      }
    }

    class Ignore extends ExceptionHandler {
      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {}
    }

  }

  object Sink {

    class Reload extends ExceptionHandler {
      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
        self ! SubscriberMessage.ClientInit
      }
    }

    class Retry(delay: Duration = Duration.Zero, maxNrOfRetries: Int) extends ExceptionHandler {
      val ignore = new Sink.Ignore

      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
        message match {
          case SubscriberMessage.Failure(_, _, nrOfRetries) ⇒
            if (maxNrOfRetries > 0 && nrOfRetries > maxNrOfRetries) {
              ignore.handle(key, cause, message)
              return
            }
        }
        delay match {
          case d: FiniteDuration ⇒ center.system.scheduler.scheduleOnce(d, self, message)
          case _ ⇒ self ! message
        }
      }
    }

    class Ignore extends ExceptionHandler {
      override def handle(key: String, cause: Throwable, message: Any)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
        message match {
          case onNext: OnNext ⇒
            onNext.element match {
              case teleporterMessage: TeleporterMessage[_] ⇒ teleporterMessage.confirmed(teleporterMessage)
              case teleporterMessages: Seq[TeleporterMessage[_]] ⇒ teleporterMessages.foreach(msg ⇒ msg.confirmed(msg))
              case _ ⇒ //ignore
            }
            self ! SubscriberMessage.Success(onNext)
          case _ ⇒ //ignore
        }
      }
    }

  }

}

case class ExceptionRule(errorMatch: Regex, level: String, handler: ExceptionHandler)

object ExceptionRule {
  //stream.start(1.minutes, 3)
  val actionMatch: Regex = "\\s+(\\w+\\.\\w+)(\\(([\\w.]+)(,\\s+(\\d+))?\\))?".r
  val ALL_MATCH: Regex = ".".r

  def apply(streamKey: String, errorMatch: String, level: String, cmd: String): ExceptionRule = {
    val handler: ExceptionHandler = cmd match {
      case actionMatch(action, _, delay, _, retries, _*) ⇒
        val delayDuration: Duration = delay match {
          case null ⇒ Duration.Zero
          case _ ⇒ Duration(delay)
        }
        action match {
          case "stream.start" ⇒ new ExceptionHandler.Stream.StreamCommand(streamKey, Streams.DelayCommand(Streams.Start(streamKey), delayDuration))
          case "stream.stop" ⇒ new ExceptionHandler.Stream.StreamCommand(streamKey, Streams.DelayCommand(Streams.Stop(streamKey), delayDuration))
          case "stream.restart" ⇒ new ExceptionHandler.Stream.StreamCommand(streamKey, Streams.DelayCommand(Streams.Restart(streamKey), delayDuration))
          case "source.reload" ⇒ new ExceptionHandler.Source.Reload
          case "source.retry" ⇒ new ExceptionHandler.Source.Retry(delayDuration)
          case "source.ignore" ⇒ new ExceptionHandler.Source.Ignore
          case "sink.reload" ⇒ new ExceptionHandler.Sink.Reload
          case "sink.retry" ⇒ new ExceptionHandler.Sink.Retry(delayDuration, retries.toInt)
          case "sink.ignore" ⇒ new ExceptionHandler.Sink.Ignore
        }
      case _ ⇒ new ExceptionHandler.Stream.StreamCommand(streamKey, Streams.DelayCommand(Stop(streamKey)))
    }
    ExceptionRule(errorMatch.r, level, handler)
  }
}

class Enforcer(key: String, rules: Seq[ExceptionRule], defaultHandler: ExceptionHandler)(implicit center: TeleporterCenter) extends LazyLogging {
  val errorMetrics: mutable.HashMap[String, MetricsCounter] = mutable.HashMap[String, MetricsCounter]()
  val defaultMetrics: MetricsCounter = center.metricsRegistry.counter(Measurement(key, Seq(Tags.error, Tag("level", "INFO"))))

  def execute(cause: Throwable, message: Any = None)(implicit self: ActorRef, center: TeleporterCenter, ec: ExecutionContext): Unit = {
    logger.error(s"$key, ${cause.getMessage}", cause)
    rules.find(r ⇒ r.errorMatch.findFirstIn(cause.getMessage).isDefined || r.errorMatch.findFirstIn(cause.getClass.getName).isDefined) match {
      case Some(rule) ⇒
        logger.info(s"Use handler $key: $defaultHandler")
        errorMetrics.getOrElseUpdate(rule.level, center.metricsRegistry.counter(Measurement(key, Seq(Tag("level", rule.level))))).inc()
        rule.handler.handle(key, cause, message)
      case None ⇒
        logger.info(s"Use default handler $key: $defaultHandler")
        defaultMetrics.inc()
        defaultHandler.handle(key, cause, message)
    }
  }
}

object Enforcer extends LazyLogging {
  val ruleMatch: Regex = "(.*)(:(DEBUG|INFO|WARN|ERROR))?\\s*=>\\s*(.*)".r

  /**
    * *: restart|stop
    * runtimeException => stream.restart|stop|retry|restart(1.seconds)
    * regex: handler
    * Unknown column: stop
    */
  def apply(key: String, config: ConfigMetaBean)(implicit center: TeleporterCenter): Enforcer = {
    val stream = center.context.getContext[ComponentContext](key) match {
      case sourceContext: SourceContext ⇒ sourceContext.stream
      case sinkContext: SinkContext ⇒ sinkContext.stream
    }
    val streamKey = stream.key
    val rules = config.errorRules.map {
      case ruleMatch(errorMatch, _, level, action) ⇒ ExceptionRule(streamKey, errorMatch, level, action)
    }
    new Enforcer(key, rules, new ExceptionHandler.Stream.StreamCommand(streamKey, Streams.DelayCommand(Stop(streamKey))))
  }
}
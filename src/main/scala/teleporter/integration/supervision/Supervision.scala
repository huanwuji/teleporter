package teleporter.integration.supervision

import akka.actor.ActorRef
import akka.stream.TeleporterAttributes
import akka.stream.TeleporterAttributes.{SupervisionStrategy, TeleporterSupervisionStrategy}
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.core.Streams.DelayCommand
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics.{Measurement, Tag, Tags}
import teleporter.integration.supervision.Decider.Decide
import teleporter.integration.supervision.Supervision._

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.util.matching.Regex

/**
  * Author: kui.dai
  * Date: 2016/3/23.
  */
object Supervision {

  trait Directive {
    def retries: Int

    def delay: Duration

    def next: Option[Directive]
  }

  case class Start(retries: Int = 1, delay: Duration = Duration.Zero, next: Option[Directive]) extends Directive

  case class Stop(retries: Int = 1, delay: Duration = Duration.Zero, next: Option[Directive]) extends Directive

  case class Reload(retries: Int = 1, delay: Duration = Duration.Zero, next: Option[Directive]) extends Directive

  case class Retry(retries: Int = 1, delay: Duration = Duration.Zero, next: Option[Directive]) extends Directive

  case class Restart(retries: Int = 1, delay: Duration = Duration.Zero, next: Option[Directive]) extends Directive

  case class Resume(retries: Int = 1, delay: Duration = Duration.Zero, next: Option[Directive]) extends Directive

  object Directive extends Logging {
    //retry(delay = 1.minutes, retries = 3, next = stop)
    val actionMatch: Regex = "\\s*(\\w+)\\((.*)\\)".r

    def apply(directive: String): Directive = {
      directive match {
        case actionMatch(action, params) ⇒
          val paramsMap = params.split(",")
            .map(_.replaceAll("\\s*", "").split("="))
            .collect { case Array(name, value) ⇒ (name, value) }
            .toMap
          val retries = paramsMap.get("retries").map(_.toInt).getOrElse(1)
          val delay = paramsMap.get("delay").map(Duration(_)).getOrElse(Duration.Zero)
          val next = paramsMap.get("next").map(parseDirective(_, 1, Duration.Zero, None))
          parseDirective(action, retries, delay, next)
      }
    }

    private def parseDirective(action: String, retries: Int, delay: Duration, next: Option[Directive]): Directive = {
      action match {
        case "start" ⇒ Supervision.Start(retries, delay, next)
        case "stop" ⇒ Supervision.Stop(retries, delay, next)
        case "restart" ⇒ Supervision.Restart(retries, delay, next)
        case "reload" ⇒ Supervision.Reload(retries, delay, next)
        case "retry" ⇒ Supervision.Retry(retries, delay, next)
        case "resume" ⇒ Supervision.Resume(retries, delay, next)
        case _ ⇒ throw new IllegalArgumentException(s"Can't match any rules, $action, $retries, $delay, $next")
      }
    }
  }

}

object Decider {
  type Decide = PartialFunction[(Throwable, Directive), Any]
}

trait Decider extends Logging {
  def supervisionStrategy: TeleporterAttributes.SupervisionStrategy

  def teleporterFailure(ex: Throwable): Unit = logger.error(s"Exception was catch and process, ${ex.getMessage}", ex)

  protected def matchRule(ex: Throwable): Option[DecideRule] = {
    supervisionStrategy match {
      case TeleporterSupervisionStrategy(key, rules, center) ⇒
        matchRule(ex, rules).map { rule ⇒
          center.metricsRegistry.counter(Measurement(key, Seq(Tags.error, Tag("level", rule.level)))).inc()
          rule
        }
      case supervisionStrategy: SupervisionStrategy ⇒
        matchRule(ex, supervisionStrategy.rules)
    }
  }

  private def matchRule(cause: Throwable, rules: Seq[DecideRule]): Option[DecideRule] = {
    rules.find(r ⇒ (cause.getMessage != null && r.errorMatch.findFirstIn(cause.getMessage).isDefined) || r.errorMatch.findFirstIn(cause.getClass.getName).isDefined)
  }

  protected def decide: Decide
}

class StreamDecider(key: String, streamRef: ActorRef, val supervisionStrategy: SupervisionStrategy)(implicit center: TeleporterCenter) extends Decider {
  var retries = 0
  var lastExecuteTime: Long = System.currentTimeMillis()

  override def teleporterFailure(ex: Throwable): Unit = {
    super.teleporterFailure(ex)
    lastExecuteTime = System.currentTimeMillis()
    if (System.currentTimeMillis() - lastExecuteTime > 5.minutes.toMillis) {
      retries = 0
    }
    retries += 1
    matchRule(ex) match {
      case Some(rule) ⇒
        if (rule.directive.retries == -1 || retries < rule.directive.retries) {
          rule.directive.delay match {
            case Duration.Zero ⇒ decide(ex, rule.directive)
            case d: FiniteDuration ⇒
              rule.directive match {
                case _: Start ⇒ streamRef ! DelayCommand(Streams.Start(key), d)
                case _: Stop ⇒ streamRef ! DelayCommand(Streams.Stop(key), d)
                case _: Restart ⇒ streamRef ! DelayCommand(Streams.Restart(key), d)
              }
            case _ ⇒
          }
        } else {
          rule.directive.next.foreach(decide(ex, _))
        }
      case None ⇒ stop(ex)
    }
  }

  override def decide: Decide = {
    case (ex, _: Start) ⇒ start(ex)
    case (ex, _: Stop) ⇒ stop(ex)
    case (ex, _: Restart) ⇒ restart(ex)
  }

  def start(ex: Throwable): Unit = center.streams ! Streams.Start(key)

  def stop(ex: Throwable): Unit = center.streams ! Streams.Stop(key)

  def restart(ex: Throwable): Unit = center.streams ! Streams.Restart(key)
}

trait SourceDecider extends Decider {
  override def decide: Decide = {
    case (ex, _: Reload) ⇒ reload(ex)
    case (ex, _: Retry) ⇒ retry(ex)
    case (ex, _: Resume) ⇒ resume(ex)
    case (ex, _: Stop) ⇒ stop(ex)
  }

  def reload(ex: Throwable): Unit

  def retry(ex: Throwable): Unit

  def resume(ex: Throwable): Unit

  def stop(ex: Throwable): Unit
}

trait SinkDecider extends Decider {
  override def decide: Decide = {
    case (ex, _: Reload) ⇒ reload(ex)
    case (ex, _: Retry) ⇒ retry(ex)
    case (ex, _: Resume) ⇒ resume(ex)
    case (ex, _: Stop) ⇒ stop(ex)
  }

  def reload(ex: Throwable): Unit

  def retry(ex: Throwable): Unit

  def resume(ex: Throwable): Unit

  def stop(ex: Throwable): Unit
}

case class DecideRule(errorMatch: Regex, level: String, directive: Directive)

object DecideRule {
  val ruleMatch: Regex = "(.*?)(:(DEBUG|INFO|WARN|ERROR))?\\s*=>\\s*(.*)".r
  val ALL_MATCH: Regex = ".".r

  def apply(errorMatch: String, level: String, directive: String): DecideRule = {
    DecideRule(errorMatch.r, level, Directive(directive))
  }

  /**
    * runtimeException => restart|stop|retry|restart(delay = 1.seconds, retries = 1, next = stop)
    * regex: handler
    * Unknown column: stop
    */
  def apply(config: ConfigMetaBean)(implicit center: TeleporterCenter): Seq[DecideRule] = {
    config.errorRules.map {
      case ruleMatch(errorMatch, _, level, directive) ⇒ DecideRule(errorMatch, level, directive)
    }
  }
}
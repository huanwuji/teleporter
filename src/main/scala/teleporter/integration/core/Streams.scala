package teleporter.integration.core

import java.util.UUID
import java.util.concurrent.{Callable, TimeUnit}

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.stream.KillSwitch
import com.google.common.base.Charsets
import com.google.common.cache.CacheBuilder
import com.google.common.hash.Hashing
import com.markatta.akron.CronTab.{UnSchedule, UnScheduled}
import com.markatta.akron.{CronExpression, CronTab}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.core.Streams.{ExecuteStream, _}
import teleporter.integration.script.ScriptEngines
import teleporter.integration.utils.CronFixed

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success}

/**
  * Created by kui.dai on 2016/7/7.
  */
case class StreamState(returnValues: ReturnValues)

class StreamsActor()(implicit center: TeleporterCenter) extends Actor with LazyLogging {

  import center.materializer
  import context.dispatcher

  private val streamLogicCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build[String, StreamLogic]()
  private val streamStates = TrieMap[String, StreamState]()
  private val cronCache = mutable.Map[Any, UUID]()

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
  }

  override def receive: Receive = schedule.orElse(command)

  def schedule: Receive = {
    case DelayCommand(command, delay) ⇒
      delay match {
        case Duration.Zero ⇒ self ! command
        case d: FiniteDuration ⇒ context.system.scheduler.scheduleOnce(d, self, command)
      }
    case CronCommand(command, cron) ⇒
      val fixedCron = CronFixed.fixed(cron)
      cronCache.remove(command).foreach {
        jobId ⇒
          logger.info(s"Cancel exists cron job, $command, $jobId")
          center.crontab ! UnSchedule(jobId)
          center.crontab ! CronTab.Schedule(self, command, CronExpression(fixedCron))
      }
      center.crontab ! CronTab.Schedule(self, command, CronExpression(fixedCron))
    case cronSchedule@CronTab.Scheduled(jobId, _, message) ⇒
      logger.info(s"CronSchedule $cronSchedule was start")
      cronCache += (message → jobId)
    case UnScheduled(jobId) ⇒
      logger.info(s"Cron job was cancel, $jobId")
  }

  def command: Receive = {
    case Start(key) ⇒ start(key)
    case Stop(key) ⇒ stop(key)
    case Remove(key) ⇒ stop(key)
    case Restart(key) ⇒ restart(key)
    case ExecuteStream(key, stream) ⇒ executeStream(key, stream)
  }

  def remove(key: String): Unit = {
    stop(key).onComplete {
      case Success(_) ⇒ streamStates -= key
      case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
    }
  }

  def restart(key: String): Unit = {
    stop(key).onComplete {
      case Success(_) ⇒ start(key)
      case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
    }
  }

  def start(key: String): Unit = {
    center.context.getContext[StreamContext](key).config.cronOption match {
      case Some(cron) if cron.nonEmpty ⇒
        self ! CronCommand(Streams.Start(key), cron)
      case _ ⇒
        loadTemplate(key) match {
          case Some(result) ⇒
            result.onComplete {
              case Success(template) ⇒ self ! ExecuteStream(key, template)
              case Failure(e) ⇒ logger.error(e.getLocalizedMessage, e)
            }
          case None ⇒ logger.warn(s"Can't load template $key")
        }
    }
  }

  def stop(key: String): Future[Done] = {
    streamStates.get(key) match {
      case Some(state) ⇒
        val (killSwitch, fu) = state.returnValues
        killSwitch.shutdown()
        fu
      case None ⇒ Future.successful(Done)
    }
  }

  def executeStream(key: String, template: String): Unit = {
    val streamLogic = streamLogicCache.get(Hashing.md5().hashString(template, Charsets.UTF_8).toString,
      new Callable[StreamLogic]() {
        override def call(): StreamLogic = {
          ScriptEngines.scala.eval(template).asInstanceOf[StreamLogic]
        }
      })
    logger.info(s"$key stream will executed")
    val (_, resultListener) = streamLogic(key, center)
    resultListener.onComplete {
      case Success(v) ⇒ logger.info(s"$key was completed, $v")
      case Failure(e) ⇒ logger.error(s"$key execute failed", e)
    }
    streamStates += key → StreamState(streamLogic(key, center))
  }

  private def loadTemplate(streamId: String, cache: Boolean = true): Option[Future[String]] = {
    val streamContext = center.context.getContext[StreamContext](streamId)
    val template = streamContext.config.template.orElse(streamContext.task().config.template)
    template.map {
      case defined if defined.startsWith("git:") ⇒
        if (cache) {
          center.gitClient.cacheContent(defined)
        } else {
          center.gitClient.content(defined)
        }
      case defined ⇒ Future.successful(defined)
    }
  }
}

object Streams {
  type ReturnValues = (KillSwitch, Future[Done])
  type StreamLogic = (String, TeleporterCenter) ⇒ ReturnValues

  sealed trait StreamCommand

  case class Start(key: String) extends StreamCommand

  case class Stop(key: String) extends StreamCommand

  case class Remove(key: String) extends StreamCommand

  case class Restart(key: String) extends StreamCommand

  case class ExecuteStream(key: String, template: String) extends StreamCommand

  case class DelayCommand(command: StreamCommand, delay: Duration = Duration.Zero)

  case class CronCommand(command: StreamCommand, cron: String)

  def apply()(implicit center: TeleporterCenter): ActorRef = {
    center.system.actorOf(Props(classOf[StreamsActor], center))
  }
}
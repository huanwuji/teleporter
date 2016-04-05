package teleporter.integration.component

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import teleporter.integration.SinkControl
import teleporter.integration.component.SubscriberSupport.{Error, Reply}
import teleporter.integration.component.jdbc.SqlSupport
import teleporter.integration.conf.PropsSupport
import teleporter.integration.core.{Component, Enforcer, TeleporterCenter}

import scala.collection.mutable

/**
 * Author: kui.dai
 * Date: 2016/3/25.
 */
class SubscriberWorker(handler: SubscriberHandler[Any]) extends Actor {

  override def receive: Actor.Receive = {
    case onNext@OnNext(ele) ⇒
      try {
        handler.handle(onNext)
        sender() ! Reply(Right(onNext))
      } catch {
        case e: Exception ⇒ sender() ! Reply(Left(Error(onNext, e)))
      }
  }
}

trait SubscriberHandler[A] {
  val client: A

  def handle(onNext: OnNext): Unit
}

trait SubscriberSupport[A] extends ActorSubscriber with Component with SqlSupport with PropsSupport with ActorLogging {
  implicit val center: TeleporterCenter

  import context.dispatcher
  import teleporter.integration.conf.SubscriberProps._

  val conf = center.sinkFactory.loadConf(id)
  val enforcer = Enforcer(conf)
  val parallelism = conf.props.parallelism
  var submitSize = 0

  override protected val requestStrategy: RequestStrategy = RequestStrategyManager(new MaxInFlightRequestStrategy(max = parallelism * 4) {
    override def inFlightInternally: Int = submitSize
  })
  val requestStrategyManager = requestStrategy.asInstanceOf[RequestStrategyManager]
  var client: A = _
  var router: ActorRef = _
  protected val errorQueue = mutable.Queue[OnNext]()

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    self ! SinkControl.Init
  }

  def init(): Unit = {
    try {
      client = center.addressing[A](conf.addressId.get, conf)
      router = context.actorOf(BalancingPool(parallelism).props(workProps))
      if (requestStrategyManager.isPause()) {
        requestStrategyManager.resume()
        request(remainingRequested)
      }
    } catch {
      case e: Exception ⇒ requestStrategyManager.pause(); enforcer.execute("init", e, SinkControl.Init)
    }
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"${conf.name} preRestart, ${reason.getLocalizedMessage}", reason)
    super.preRestart(reason, message)
  }

  override def receive: Actor.Receive = {
    case onNext: OnNext ⇒
      submitSize += 1
      if (router == null) {
        enforcer.execute("dataReceive", new IllegalArgumentException("Init not success!"), onNext)
      } else {
        router ! onNext
      }
    case SubscriberSupport.Reply(result) ⇒
      submitSize -= 1
      result match {
        case Left(errorInfo) ⇒ enforcer.execute("dataReceive", errorInfo.throwable, errorInfo.message)
        case Right(onNext) ⇒
          if (requestStrategyManager.isPause() && submitSize < parallelism) {
            requestStrategyManager.resume()
          }
      }
    case OnComplete ⇒ context.stop(self)
    case OnError(e) ⇒ log.error(s"${conf.name} error", e); context.stop(self)
    case SinkControl.Init ⇒ init()
    case x ⇒ log.warning("can't arrived, {}", x)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info(s"${conf.name} will stop")
    center.removeAddress(conf.addressId.get, conf)
  }

  def workProps: Props = Props(classOf[SubscriberWorker], createHandler)

  def createHandler: SubscriberHandler[A]
}

object SubscriberSupport {

  case class Error(message: OnNext, throwable: Throwable)

  case class RegisterAddress(address: Any)

  case class Reply(result: Either[Error, OnNext])

}
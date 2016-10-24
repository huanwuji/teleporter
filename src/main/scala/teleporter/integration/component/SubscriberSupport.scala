package teleporter.integration.component

import akka.actor.{Actor, ActorRef, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import teleporter.integration.component.SubscriberMessage.{ClientInit, Failure, Success}
import teleporter.integration.component.jdbc.SqlSupport
import teleporter.integration.core._

import scala.collection.mutable

/**
  * Author: kui.dai
  * Date: 2016/3/25.
  */
class SubscriberWorker(handler: SubscriberHandler[Any]) extends Actor {

  override def receive: Actor.Receive = {
    case onNext: OnNext ⇒ handle(onNext, () ⇒ sender() ! Success(onNext))
    case failure@Failure(onNext, _, _) ⇒ handle(onNext, () ⇒ sender() ! failure.copy(nrOfRetries = failure.nrOfRetries + 1))
  }

  def handle(onNext: OnNext, failureHandle: () ⇒ Unit): Unit = {
    try {
      handler.handle(onNext)
      sender() ! Success(onNext)
    } catch {
      case e: Exception ⇒ failureHandle()
    }
  }
}

trait SubscriberHandler[A] {
  val client: A

  def handle(onNext: OnNext): Unit
}

trait SubscriberSupport[A] extends ActorSubscriber with Component with SqlSupport with SinkMetadata {
  implicit val center: TeleporterCenter

  implicit val sinkContext = center.context.getContext[SinkContext](key)
  implicit val sinkConfig = sinkContext.config
  val parallelism = lnsParallelism
  var submitSize = 0
  val counter = center.metricsRegistry.counter(key)

  override protected val requestStrategy: RequestStrategy = RequestStrategyManager(autoStart = false)
  val requestStrategyManager = requestStrategy.asInstanceOf[RequestStrategyManager]
  var client: A = _
  var router: ActorRef = _
  protected val errorQueue = mutable.Queue[OnNext]()

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    self ! ClientInit
  }

  @throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"$key preRestart, ${reason.getLocalizedMessage}", reason)
    super.preRestart(reason, message)
  }

  override def receive: Actor.Receive = {
    case ClientInit ⇒
      try {
        val sinkContext = center.context.getContext[SinkContext](key)
        client = center.components.address[A](sinkContext.addressKey)
        router = context.actorOf(BalancingPool(parallelism).props(workProps))
        requestStrategyManager.autoRequestStrategy(new MaxInFlightRequestStrategy(max = parallelism * 4) {
          override def inFlightInternally: Int = submitSize
        })
        requestResume()
        context.become(customReceive)
      } catch {
        case e: Exception ⇒ requestStrategyManager.stop()
      }
    case x ⇒ logger.warn(s"can't arrived, $x")
  }

  private def requestResume(): Unit = {
    if (requestStrategyManager.isPause) {
      requestStrategyManager.start()
      request(remainingRequested)
    }
  }

  def customReceive: Receive = ({
    case onNext: OnNext ⇒
      submitSize += 1
      router ! onNext
      counter.inc()
    case Success(onNext) ⇒ submitSize -= 1
    case failure@Failure(onNext, e, count) ⇒
      if (sender() == self) {
        router ! failure
      } else {

      }
    case x ⇒ logger.warn(s"can't arrived, $x")
  }: Receive).orElse(signalReceive)

  def signalReceive: Receive = {
    case OnComplete ⇒
      context.stop(self)
      sinkContext.address().clientRefs.close(key)
    case OnError(e) ⇒
      context.stop(self)
      sinkContext.address().clientRefs.close(key)
      logger.error(s"$key error", e);
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info(s"$key will stop")
  }

  def workProps: Props = Props(classOf[SubscriberWorker], createHandler)

  def createHandler: SubscriberHandler[A]
}

object SubscriberMessage {

  sealed trait Action

  case object ClientInit extends Action

  case class Success(onNext: OnNext) extends Action

  case class Failure(onNext: OnNext, throwable: Throwable, nrOfRetries: Int = 0) extends Action

}
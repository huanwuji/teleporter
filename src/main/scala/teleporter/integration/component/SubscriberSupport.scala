package teleporter.integration.component

import akka.actor.{Actor, ActorRef, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import teleporter.integration.component.SubscriberMessage.{ClientInit, Failure, Success}
import teleporter.integration.component.jdbc.SqlSupport
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics.{Measurement, Tags}

import scala.collection.mutable

/**
  * Author: kui.dai
  * Date: 2016/3/25.
  */
trait SubscriberWorker[A] extends Actor {
  val client: A

  override def receive: Actor.Receive = {
    case onNext: OnNext ⇒ handle(onNext, e ⇒ sender() ! Failure(onNext, e))
    case failure@Failure(onNext, _, _) ⇒ handle(onNext, e ⇒ sender() ! failure.copy(throwable = e, nrOfRetries = failure.nrOfRetries + 1))
  }

  def handle(onNext: OnNext): Unit

  protected def handle(onNext: OnNext, failureHandle: Throwable ⇒ Unit): Unit = {
    try {
      handle(onNext)
      sender() ! Success(onNext)
    } catch {
      case e: Exception ⇒ failureHandle(e)
    }
  }
}

trait SubscriberSupport[A] extends ActorSubscriber with Component with SqlSupport with SinkMetadata {

  import context.dispatcher

  implicit val center: TeleporterCenter
  implicit val sinkContext = center.context.getContext[SinkContext](key)
  implicit val sinkConfig = sinkContext.config
  val parallelism = lnsParallelism
  var submitSize = 0
  val counter = center.metricsRegistry.counter(Measurement(key, Seq(Tags.success)))
  protected var enforcer: Enforcer = _

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
        enforcer = Enforcer(key, sinkConfig)
        client = center.components.address[A](sinkContext.addressKey)
        router = context.actorOf(BalancingPool(parallelism).props(workProps).withDispatcher("akka.teleporter.blocking-io-dispatcher"))
        requestStrategyManager.autoRequestStrategy(new MaxInFlightRequestStrategy(max = parallelism * 16) {

          override def batchSize: Int = parallelism * 8

          override def inFlightInternally: Int = submitSize
        })
        requestResume()
        context.become(customReceive)
      } catch {
        case e: Exception ⇒
          requestStrategyManager.stop()
          enforcer.execute(e)
      }
    case x ⇒ logger.warn(s"can't arrived, $x")
  }

  private def requestResume(): Unit = {
    if (requestStrategyManager.isPause) {
      requestStrategyManager.start()
      request(requestStrategy.requestDemand(remainingRequested))
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
        enforcer.execute(e, failure)
      }
  }: Receive).orElse(signalReceive)

  def signalReceive: Receive = {
    case OnComplete ⇒
      context.stop(self)
      sinkContext.address().clientRefs.close(key)
    case OnError(e) ⇒
      context.stop(self)
      sinkContext.address().clientRefs.close(key)
      enforcer.execute(e)
    case x ⇒ logger.warn(s"can't arrived, $x")
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info(s"$key will stop")
  }

  def workProps: Props
}

object SubscriberMessage {

  sealed trait Action

  case object ClientInit extends Action

  case class Success(onNext: OnNext) extends Action

  case class Failure(onNext: OnNext, throwable: Throwable, nrOfRetries: Int = 1) extends Action

}
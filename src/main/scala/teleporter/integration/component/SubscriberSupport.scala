package teleporter.integration.component

import akka.actor.{Actor, ActorRef, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy, RequestStrategy}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.component.SubscriberMessage.{ClientInit, Failure, Success}
import teleporter.integration.core._
import teleporter.integration.metrics.Metrics.{Measurement, Tags}
import teleporter.integration.metrics.MetricsCounter

import scala.collection.mutable

/**
  * Author: kui.dai
  * Date: 2016/3/25.
  */
trait SubscriberWorker[A] extends Actor {
  val client: A

  override def receive: Actor.Receive = {
    case onNext: OnNext ⇒ wrapHandle(onNext)
    case Failure(onNext, _, nrOfRetries) ⇒ wrapHandle(onNext, nrOfRetries)
  }

  protected def handle(onNext: OnNext, nrOfRetries: Int): Unit

  private def wrapHandle(onNext: OnNext, nrOfRetries: Int = 1): Unit = {
    try {
      handle(onNext, nrOfRetries)
    } catch {
      case e: Exception ⇒ failure(onNext, e, nrOfRetries)
    }
  }

  def success[T <: TeleporterMessage[Any]](message: T): Unit = {
    message.confirmed(message)
  }

  def success(onNext: OnNext): Unit = {
    onNext.element match {
      case message: TeleporterMessage[Any] ⇒ message.confirmed(message)
      case messages: Seq[TeleporterMessage[Any]] ⇒ messages.foreach(message ⇒ message.confirmed(message))
    }
    sender() ! Success(onNext)
  }

  def failure(onNext: OnNext, throwable: Throwable, nrOfRetries: Int): Unit = {
    sender() ! Failure(onNext, throwable, nrOfRetries + 1)
  }
}

trait SubscriberSupport[A] extends ActorSubscriber with Component with LazyLogging {

  import context.dispatcher

  implicit val center: TeleporterCenter
  implicit val sinkContext: SinkContext = center.context.getContext[SinkContext](key)
  implicit val sinkConfig: SinkMetaBean = sinkContext.config
  val parallelism: Int = sinkConfig.parallelism
  var submitSize = 0
  val counter: MetricsCounter = center.metricsRegistry.counter(Measurement(key, Seq(Tags.success)))
  protected var enforcer: Enforcer = _

  override protected val requestStrategy: RequestStrategy = RequestStrategyManager(autoStart = false)
  val requestStrategyManager: RequestStrategyManager = requestStrategy.asInstanceOf[RequestStrategyManager]
  var client: A = _
  var router: ActorRef = _
  protected val errorQueue: mutable.Queue[OnNext] = mutable.Queue[OnNext]()

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
        client = center.components.address[A](sinkContext.config.address)
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
    case Success(_) ⇒ submitSize -= 1
    case failure@Failure(_, e, _) ⇒
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
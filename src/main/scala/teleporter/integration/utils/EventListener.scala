package teleporter.integration.utils

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.util.Timeout

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.{Failure, Try}

/**
  * Created by kui.dai on 2016/8/3.
  */
trait EventListener[T] {
  val system: ActorSystem
  type ErrorHandler = Long ⇒ Try[T]

  def timeoutHandler(seqNr: Long): Try[T] = Failure(new TimeoutException(s"Event response timeout $seqNr"))

  import system.dispatcher

  private val events = TrieMap[Long, Promise[T]]()
  private val _seqNr = new AtomicLong()

  private def getSeqNr: Long = _seqNr.incrementAndGet()

  private def registerEvents(default: ErrorHandler = timeoutHandler)(implicit timeout: Timeout): (Long, Future[T]) = {
    val idx = getSeqNr
    val promise = Promise[T]()
    events += idx → promise
    system.scheduler.scheduleOnce(timeout.duration) {
      resolve(idx, default(idx))
    }
    (idx, promise.future)
  }

  def asyncEvent(handler: Long ⇒ Unit, default: ErrorHandler = timeoutHandler)(implicit timeout: Timeout = Timeout(2.minutes)): (Long, Future[T]) = {
    val result@(seqNr, _) = registerEvents()
    handler(seqNr)
    result
  }

  def resolve(seqNr: Long, result: T): Unit = events.remove(seqNr).foreach(_.success(result))

  def resolve(seqNr: Long, result: Try[T]): Unit = events.remove(seqNr).foreach(_.tryComplete(result))

  def clear(): Unit = {
    events.values.foreach(_.failure(new RuntimeException("Event will clear, So some unComplete event will interrupt")))
    events.clear()
  }
}

class EventListenerImpl[T]()(implicit val system: ActorSystem) extends EventListener[T]

object EventListener {
  def apply[T]()(implicit system: ActorSystem): EventListener[T] = new EventListenerImpl[T]()
}
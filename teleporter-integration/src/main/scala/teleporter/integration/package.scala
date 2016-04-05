package teleporter

import akka.actor.ActorRef
import teleporter.integration.core.StreamConfig

import scala.concurrent.duration.Duration

/**
 * Author: kui.dai
 * Date: 2015/12/4.
 */
package object integration {

  sealed trait SourceControl

  object SourceControl {

    object Notify extends SourceControl

    object Reload extends SourceControl

    object CompleteThenStop extends SourceControl

    case class ErrorThenStop(reason: Throwable) extends SourceControl

    case class Register(actorRef: ActorRef) extends SourceControl

  }


  sealed trait SinkControl

  object SinkControl {

    object Cancel extends SinkControl

    object Init extends SinkControl

  }

  sealed trait StreamControl

  object StreamControl {

    case class Start[T <: StreamConfig](streamConfig: T) extends StreamControl

    case class Stop[T](streamConfig: T) extends StreamControl

    case class TryStart[T](streamConfig: T) extends StreamControl

    case class Restart[T](streamConfig: T, delay: Duration = Duration.Zero) extends StreamControl

    object Scan extends StreamControl

    case class NotifyStream(streamId: Int) extends StreamControl

    case class StreamCmpRegister(conf: Any, streamId: Int = -1) extends StreamControl

  }

  sealed trait CmpType

  object CmpType {

    object Task extends CmpType

    object Stream extends CmpType

    object Address extends CmpType

    object Source extends CmpType {
      override def toString: String = "source"
    }

    object Sink extends CmpType {
      override def toString: String = "sink"
    }

    object Shadow extends CmpType

    object Undefined extends CmpType

  }

  case class ErrorMessage(id: Int, cmpType: CmpType, message: Any = null, exception: Throwable = null)

}
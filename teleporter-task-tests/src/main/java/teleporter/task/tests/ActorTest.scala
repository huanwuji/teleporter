package teleporter.task.tests

import akka.actor.ActorRef
import akka.stream.actor.{WatermarkRequestStrategy, ActorSubscriber}

/**
 * Created by joker on 15/10/23.
 */
class ActorTest {



}

class Receiver(probe: ActorRef) extends ActorSubscriber {
  import akka.stream.actor.ActorSubscriberMessage._

  override val requestStrategy = WatermarkRequestStrategy(10)

  def receive = {
    case OnNext(s: String) ⇒
      probe ! s
  }
}
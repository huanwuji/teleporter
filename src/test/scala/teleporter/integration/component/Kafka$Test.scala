package teleporter.integration.component

import java.time.LocalDateTime

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, SinkQueueWithCancel, Source}

/**
  * Created by huanwuji 
  * date 2017/3/14.
  */
object Kafka$Test extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  val queue: SinkQueueWithCancel[Int] = Source.repeat(1).runWith(Sink.queue())
  Source.repeat(2).mapAsyncUnordered(1)(_ ⇒ queue.pull()).runFold(0) {
    case (i, o) ⇒
      if (i % 200000 == 0) {
        println(i, LocalDateTime.now().getSecond)
      }
      i + o.get
  }
}

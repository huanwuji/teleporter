package teleporter.task.tests

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.{Keep, Merge, Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2016/3/15.
 */
class GraphTestPublisher(start: Int, end: Int) extends ActorPublisher[Int] {
  var count = start

  override def receive: Receive = {
    case Request(n) ⇒
      for (i ← 1L to n if !isCompleted) {
        if (count < end) {
          count += 1
          onNext(count)
        } else {
          onCompleteThenStop()
        }
      }
  }
}

object GraphStreamsTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  Source.combine(Source.tick(0.seconds, 1.seconds, 1), Source(20 to 30), Source(40 to 50))(Merge(_)).runForeach {
    x ⇒ Thread.sleep(1000); println(x)
  }
  val sources = Source.actorPublisher[Int](Props(classOf[GraphTestPublisher], 1, 5)) ++ Source.actorPublisher[Int](Props(classOf[GraphTestPublisher], 10, 20))
  val result = sources.watchTermination()(Keep.right).to(Sink.foreach(println)).run()
  println(Await.result(result, 5.seconds))
}

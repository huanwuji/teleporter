package teleporter.task.tests

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Keep, Source}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2015/10/26.
 */
object TidTest extends App {
  implicit val system = ActorSystem("system")
  implicit val mater = ActorMaterializer()

  import system.dispatcher

  //  val queue = Source.queue[Int](2, OverflowStrategy.backpressure).to(Sink.foreach(println)).run()
  val queue = Source(1 to 2).runWith(Sink.queue())
  //  Source(1 to 2).watchTermination()
  println(Await.result(queue.pull(), 1.minutes))
  println(Await.result(queue.pull(), 1.minutes))
  println(Await.result(queue.pull(), 1.minutes))
  println(Await.result(queue.pull(), 10.seconds))
  Source(1 to 5).mapAsyncUnordered(1)(x ⇒ {
//    Thread.sleep(1000)
    queue.pull()
  }).runForeach(x ⇒ println("2" + x))
  Thread.sleep(10000)
  Source.fromIterator(() ⇒ Iterator.continually(queue.pull())).mapAsyncUnordered(1)(x ⇒ x).runForeach(x ⇒ println("3" + x))
//    println(Await.result(queue.pull(), 1.minutes))
  //
  //  //  Source.tick(1.seconds, 1.seconds, 1).runForeach(queue.offer _)
  //
  //  Source(1 to 3).batch(max = 2, seed = i ⇒ i)(aggregate = _ + _).to(Sink.foreach(println)).run()

  val future = Source(1 to 4).watchTermination()(Keep.right).runForeach(println)
  println(Await.result(future, 1.minutes))
}
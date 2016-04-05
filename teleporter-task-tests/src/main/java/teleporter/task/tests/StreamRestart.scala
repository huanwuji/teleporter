package teleporter.task.tests

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.ByteString

import scala.concurrent.duration._

/**
 * Author: kui.dai
 * Date: 2015/10/23.
 */
object StreamRestart extends App {

  object CompletedThenStop

  class TestPublisher extends ActorPublisher[Long] {
    var count = 1L

    override def receive: Receive = {
      case Request(num) ⇒
        for (i ← 1L to num) {
          onNext(count)
          count += 1
          TimeUnit.SECONDS.sleep(1)
        }
      case CompletedThenStop ⇒
        println("publisher will stop!")
        onCompleteThenStop()
    }

    @throws[Exception](classOf[Exception])
    override def postStop(): Unit = {
      println("postStop")
      TimeUnit.SECONDS.sleep(5)
    }
  }

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val actorRef = streamCreated()
  TimeUnit.SECONDS.sleep(10)
  val sourceRef = system.actorSelection("/user/test_id")
  //  sourceRef ! CompletedThenStop
  gracefulStop(actorRef, 10.seconds, CompletedThenStop).foreach {
    x ⇒
      println(s"program will restart, $x")
      streamCreated()
  }

  def streamCreated()(implicit system: ActorSystem, materializer: ActorMaterializer): ActorRef = {
    val actorRef = system.actorOf(Props[TestPublisher], "test_id")
    RunnableGraph.fromGraph(
      FlowGraph.create() {
        implicit b ⇒
          import FlowGraph.Implicits._
          val publisher = ActorPublisher[Long](actorRef)
          val bcast = b.add(Broadcast[ByteString](2))

          Source(publisher) ~> Flow[Long].map {
            msg ⇒ println(msg); ByteString(msg)
          } ~> bcast.in
          bcast.out(0) ~> Sink.file(new File("d://aa.txt"))
          bcast.out(1) ~> Sink.file(new File("d://bb.txt"))
          ClosedShape
      }).run()
    actorRef
  }
}

package akka.stream.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, ClosedShape, Supervision}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite

/**
 * Author: kui.dai
 * Date: 2015/12/3.
 */
class ShuntTest extends FunSuite with LazyLogging {
  implicit val system = ActorSystem()
  val decider: Supervision.Decider = {
    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Restart
  }
  implicit val mater = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
  test("shunt") {
    println()
    try {
      RunnableGraph.fromGraph {
        FlowGraph.create() {
          implicit b ⇒
            import FlowGraph.Implicits._
            val shunt = b.add(Shunt[Int](3, _ % 3))
            Source(1 to 10000) ~> shunt
            shunt.out(0) ~> Sink.foreach[Int](x ⇒ {Thread.sleep(100);logger.info(s"0 -> ${x%3} -> $x")})
            shunt.out(1) ~> Sink.foreach[Int](x ⇒ {Thread.sleep(1);logger.info(s"1 -> ${x%3} -> $x")})
            shunt.out(2) ~> Sink.foreach[Int](x ⇒ {Thread.sleep(1);logger.info(s"2 -> ${x%3} -> $x")})
            ClosedShape
        }
      }.run()
    } catch {
      case e: Exception ⇒ logger.error(e.getLocalizedMessage, e)
    }
    Thread.sleep(60000)
  }
  test("broadcast") {
    println()
    RunnableGraph.fromGraph {
      FlowGraph.create() {
        implicit b ⇒
          import FlowGraph.Implicits._
          val broadcast = b.add(Broadcast[Int](3))
          Source(1 to 20) ~> broadcast
          broadcast.out(0) ~> Sink.foreach[Int](x ⇒ {Thread.sleep(1);logger.info(s"0 -> ${x%3} -> $x")})
          broadcast.out(1) ~> Sink.foreach[Int](x ⇒ {Thread.sleep(1);logger.info(s"1 -> ${x%3} -> $x")})
          broadcast.out(2) ~> Sink.foreach[Int](x ⇒ {Thread.sleep(1);logger.info(s"2 -> ${x%3} -> $x")})
          ClosedShape
      }
    }.run()
    Thread.sleep(60000)
  }
}

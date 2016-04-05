package teleporter.integration.component

import org.scalatest.FunSuite

import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class BridgeProducerTest extends FunSuite {
  test("queue") {
    val queue = mutable.Queue[Int]()
    queue.enqueue(1)
    println(queue)
  }
}

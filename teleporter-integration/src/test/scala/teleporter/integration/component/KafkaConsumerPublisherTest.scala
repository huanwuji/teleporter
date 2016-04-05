package teleporter.integration.component

import org.scalatest.FunSuite

/**
 * date 2015/8/3.
 * @author daikui
 */
class KafkaConsumerPublisherTest extends FunSuite {
  test("while") {
    var i = 0
    while (i < 10) {
      println(i)
      i += 1
    }
  }
}
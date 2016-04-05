package teleporter.integration.component

import org.scalatest.FunSuite

import scala.collection.mutable

/**
 * date 2015/8/3.
 * @author daikui
 */
class BridgeSubscriberTest extends FunSuite {
  test("confirmIds") {
    val confirmIds = mutable.SortedSet[Long](0)
    for (i ← 1L to 50L) {
      confirmIds += i
    }
    println(confirmIds)
  }
  test("confirmIds sequence") {
    val confirmIds = mutable.SortedSet[Long](1, 2, 3, 5, 6, 7, 8, 9, 10)
    var lastSeqNr = confirmIds.firstKey
    confirmIds.find {
      currSeqNr ⇒
        println(s"$currSeqNr, $lastSeqNr")
        if (currSeqNr - lastSeqNr > 1) {
          true
        } else {
          lastSeqNr = currSeqNr
          false
        }
    }
    println(lastSeqNr)
  }
}

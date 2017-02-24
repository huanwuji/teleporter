package teleporter.integration.core

import org.scalatest.{FunSuite, Matchers}

/**
  * Created by huanwuji 
  * date 2017/1/17.
  */
class RingPoolTest extends FunSuite with Matchers {
  test("bitSets test") {
    val bitSets = BitSets(10, 3)
    bitSets(1) should be(false)
    bitSets(1, 1) should be(false)
    bitSets += (1, 1)
    bitSets(1, 1) should be(true)
    bitSets -= (1, 1)
    bitSets(1, 1) should be(false)
    bitSets += 1
    bitSets(1) should be(true)
    bitSets(1, 2) should be(true)
  }
  test("RingPool") {
    val size = 30
    val ringPool = RingPool(size, 1)
    val useSize = 25
    val confirming = 16
    (1 to useSize) foreach {
      x ⇒ ringPool.add { idx ⇒ idx }
    }
    ringPool.remainingCapacity() should be(5)
    (1 to confirming) foreach {
      idx ⇒ ringPool.remove(idx, 0)
    }
    ringPool.remainingCapacity() should be(5)
    ringPool.canConfirmCursor should be(confirming)
    ringPool.canConfirmedSize should be(confirming)
    ringPool.unConfirmedSize should be(25)
    var confirmed = 10
    ringPool.confirmed(confirmed)
    ringPool.remainingCapacity() should be(size - useSize + confirmed)
    confirmed = 15
    ringPool.confirmed(confirmed)
    ringPool.remainingCapacity() should be(size - useSize + confirmed)
    (useSize to useSize + 9) foreach {
      _ ⇒ ringPool.add { idx ⇒ idx }
    }
    ringPool.remainingCapacity() should be(size - useSize + confirmed - 10)
    (confirming to confirming + 5) foreach {
      idx ⇒ ringPool.remove(idx, 0)
    }
    ringPool.remainingCapacity() should be(10)
  }
}
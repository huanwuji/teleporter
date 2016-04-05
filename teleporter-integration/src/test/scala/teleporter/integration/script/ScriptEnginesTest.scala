package teleporter.integration.script

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.scalatest.FunSuite

/**
 * date 2015/8/3.
 * @author daikui
 */
class ScriptEnginesTest extends FunSuite {
  test("testScala") {
    // it can't be use SimpleBindings, too slow
    val scala = ScriptEngines.getScala
    val bindings = scala.createBindings()
    bindings.put("x", "bindings")
    scala.eval("println(x)", bindings)
    val bindings1 = scala.createBindings()
    bindings1.put("x", "bindings1")
    scala.eval("println(x)", bindings1)
    scala.eval("println(x)", bindings)
    val watch = Stopwatch.createStarted()
    for (i ← 1 to 10000) {
      scala.eval("java.time.LocalDateTime.now()")
    }
    println(watch.elapsed(TimeUnit.MILLISECONDS))
  }
}
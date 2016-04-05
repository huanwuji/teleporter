package teleporter.task.tests

import java.io.FileReader
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import teleporter.integration.format.DataConvert
import teleporter.integration.script.ScriptEngines

/**
 * Author: kui.dai
 * Date: 2015/11/27.
 */
object HotLoader extends App {
  val dataConvert = ScriptEngines.getScala.eval(
    new FileReader("D:\\git\\etl\\teleporter\\teleporter-task-tests\\src\\main\\java\\teleporter\\task\\tests\\Formatter.func")
  ).asInstanceOf[DataConvert[Map[String, Any], String]]
  var i = 0
  val tick = Stopwatch.createStarted()
  while (i < 1) {
    println(dataConvert(Map("test" → "test1")))
    i += 1
  }
  println(tick.elapsed(TimeUnit.MILLISECONDS))
}
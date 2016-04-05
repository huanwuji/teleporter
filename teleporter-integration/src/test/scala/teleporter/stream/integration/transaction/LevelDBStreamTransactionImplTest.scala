package teleporter.stream.integration.transaction

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import org.scalatest.FunSuite

/**
 * date 2015/8/3.
 * @author daikui
 */
class LevelDBStreamTransactionImplTest extends FunSuite {
  test("LocalDateTime") {
    val temporalAccessor = DateTimeFormatter.ISO_INSTANT.parse("2015-08-07T18:13:00Z")
    val seconds = temporalAccessor.getLong(ChronoField.INSTANT_SECONDS)
    println(seconds)
  }
  test("symbol") {
    //    val map = Uri("http://localhost:8080?name=11&name2=2").query.toMap[Symbol, String]
    //    println(map)
  }
}
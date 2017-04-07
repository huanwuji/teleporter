package teleporter.integration.utils

import org.apache.commons.lang.StringEscapeUtils
import org.scalatest.FunSuite

/**
  * Created by huanwuji 
  * date 2017/2/24.
  */
class CsvUtilsTest extends FunSuite {
  test("csv unescape") {
    val csv = StringEscapeUtils.unescapeCsv("""""cc""""")
    println(csv)
  }
}

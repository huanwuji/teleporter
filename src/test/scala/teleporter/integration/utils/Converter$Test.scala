package teleporter.integration.utils

import java.sql.Timestamp

import org.scalatest.FunSuite

/**
 * Author: kui.dai
 * Date: 2016/4/22.
 */
class Converter$Test extends FunSuite with Convert {

  import MapBean._

  test("convert") {
    println(to[Timestamp]("2016-04-22 11:11:11"))
    val data = Map("a" → "1",
      "b" → "b",
      "c" → Seq(1, 2),
      "d" → Map("d1" → "d1", "d2" → 2),
      "e" → Seq(Map("e1" → "e1", "e2" → 2), Map("e1" → "e1", "e2" → 2))
    )
    val bean = MapBean(data)
    println(bean.__dict__[Int]("a"))
    println(bean.__dict__[String]("d.d1"))
    println(bean.__dicts__[Int]("c"))
    println(bean.__dict__[MapBean]("d").map(_.__dict__[String]("d1")))
    println(bean.__dicts__[MapBean]("e").head.__dict__[String]("e1"))
  }
}
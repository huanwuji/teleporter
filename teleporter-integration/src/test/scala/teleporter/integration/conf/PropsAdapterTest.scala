package teleporter.integration.conf

import org.scalatest.FunSuite
import teleporter.integration.conf.Conf.Props

/**
 * Author: kui.dai
 * Date: 2015/12/14.
 */
class TreeProps(override val props: Props) extends PropsAdapter

class PropsAdapterTest extends FunSuite {
  test("deep update") {
    val props = new TreeProps(Map(
      "test" → 1,
      "time" → Map(
        "period" → 1,
        "page" →
          Map("pageSize" → "10")
      )
    ))
    val newProps = props.updateProp("period" → "100")
    println(newProps)
  }
}
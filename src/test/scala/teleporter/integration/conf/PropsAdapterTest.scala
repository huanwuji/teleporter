//package teleporter.integration.conf
//
//import org.scalatest.FunSuite
//import teleporter.integration.conf.Conf.Props
//import teleporter.integration.utils.MapBean
//
///**
// * Author: kui.dai
// * Date: 2015/12/14.
// */
//class TreeProps(override val underlying: Props) extends MapBean
//
//class PropsAdapterTest extends FunSuite {
//  test("deep update") {
//    val props = new TreeProps(Map(
//      "test" → 1,
//      "time" → Map(
//        "period" → 1,
//        "page" →
//          Map("pageSize" → "10")
//      )
//    ))
//    val newProps = props ++(Seq("time", "page"), "period" → "100")
//    println(newProps)
//  }
//}
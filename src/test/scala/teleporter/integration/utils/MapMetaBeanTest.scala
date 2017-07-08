package teleporter.integration.utils

import org.scalatest.FunSuite

/**
  * Created by huanwuji on 2017/4/10.
  */
class TestMapMetaBean(val underlying: Map[String, Any]) extends MapMetaBean {
  val man = "man"
  val bbb = "bb"
}

class MapMetaBeanTest extends FunSuite {
  test("set") {
    val metaBean = new TestMapMetaBean(Map("aaa" → 1))
    println(metaBean ++ (t ⇒ Seq(t.man → "aaa", t.bbb → "bb")))
  }
}

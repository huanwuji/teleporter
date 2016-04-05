package teleporter.task.tests

/**
 * Author: kui.dai
 * Date: 2015/11/27.
 */
object HotLoader extends App {

  import teleporter.integration.component._
  import teleporter.integration.component.jdbc.Upsert
  import teleporter.integration.conf.PropsSupport
  import PropsSupport._
  import akka.stream.ClosedShape
  import akka.stream.scaladsl.{RunnableGraph, _}
  import teleporter.integration.component.jdbc.SqlSupport._
  import teleporter.integration.core.StreamManager._
  import teleporter.integration.core.TeleporterMessage

  val streamDef: StreamInvoke = (stream, center) ⇒ {
    import center.materializer
    val props = stream.props
    RunnableGraph.fromGraph(
      GraphDSL.create() {
        implicit b ⇒
          import GraphDSL.Implicits._
          val merge = b.add(Merge[TeleporterJdbcRecord](2))
          val orderItemJdbc = Flow[TeleporterJdbcMessage].map(x ⇒ TeleporterMessage[JdbcRecord](id = x.id, sourceRef = x.sourceRef, data = Seq(Upsert(update("plt_jd_order_item","order_item_id", x.data), insert("plt_jd_order_item", x.data)))))
          val wareJdbc = Flow[TeleporterJdbcMessage].map(x ⇒ TeleporterMessage[JdbcRecord](id = x.id, sourceRef = x.sourceRef, data = Seq(Upsert(update("plt_jd_ware","ware_id", x.data), insert("plt_jd_ware", x.data)))))
          center.source[TeleporterJdbcMessage](getStringOpt(props,"source:plt_jd_order_item").get) ~> orderItemJdbc ~> merge
          center.source[TeleporterJdbcMessage](getStringOpt(props,"source:plt_jd_ware").get) ~> wareJdbc ~> merge
          merge ~> center.sink(getStringOpt(props, "sink").get)
          ClosedShape
      }).run()
  }
  streamDef
}
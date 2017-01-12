import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

object Server extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val serverSource = Http().bind(interface = "0.0.0.0", port = 8084)

  println("Start")
  val requestHandler: HttpRequest => HttpResponse = {
    case r: HttpRequest =>
      println(r)
      HttpResponse(200, entity =
        """
          |<?xml version="1.0" encoding="utf-8"?>
          |<response>
          |  <flag>success</flag>
          |  <code>响应码</code>
          |  <message>响应信息</message>
          |  <orderProcess>
          |    <orderCode>单据号, string (50) , 必填</orderCode>
          |    <orderId>仓储系统单据号, string (50) ，条件必填 </orderId>
          |    <orderType>CNJG</orderType>
          |    <warehouseCode>仓库编码, string (50)  </warehouseCode>
          |    <processes>
          |      <process>
          |        <processStatus>OTHER</processStatus>
          |        <operatorCode>该状态操作员编码, string (50) </operatorCode>
          |        <operatorName>该状态操作员姓名, string (50) </operatorName>
          |        <operateTime>2016-08-29 00:00:00</operateTime>
          |        <operateInfo>操作内容, string (200) </operateInfo>
          |        <remark>备注, string (500) </remark>
          |      </process>
          |    </processes>
          |    <extendProps>
          |      <key1>value1</key1>
          |      <key2>value2</key2></extendProps>
          |  </orderProcess>
          |</response>
        """.stripMargin)
  }

  val bindingFuture: Future[Http.ServerBinding] =
    serverSource.to(Sink.foreach { connection =>
      println("Accepted new connection from " + connection.remoteAddress)

      connection handleWithSyncHandler requestHandler
      // this is equivalent to
      // connection handleWith { Flow[HttpRequest] map requestHandler }
    }).run()
}
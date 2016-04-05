//package teleporter.task.tests
//
//import java.io.File
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import akka.actor.ActorSystem
//import akka.stream.io.SynchronousFileSink
//import akka.stream.scaladsl._
//import akka.stream.{ActorMaterializer, ActorMaterializerSettings, ClosedShape, Supervision}
//import akka.util.ByteString
//import com.fasterxml.jackson.annotation.JsonInclude
//import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.commons.io.IOUtils
//import teleporter.integration.component.Control.CompleteThenStop
//import teleporter.integration.component._
//import teleporter.integration.core.TeleporterCenter
//
//
///**
// * Created by joker on 15/10/9.
// */
//object Test extends App with LazyLogging {
//  val decider: Supervision.Decider = {
//    case e: Exception => logger.error(e.getLocalizedMessage, e); Supervision.Resume
//  }
//  implicit val system = ActorSystem()
//  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
//
//  import system.dispatcher
//
//  val center = TeleporterCenter()
//  //  val path = config.getString("teleporter.path")
//  // implicit val client= center.addressing[Connection]("").get
//  // val sink1= center.sink("kafka_test_sink")
//  //  val sink2= center.sink("kafka_test_sink1")
//  //  val sink3= center.sink("kafka_test_sink2")
//  //  val unstable_log = SynchronousFileSink(new File(s"${path}unstable_log.log"), append = true)
//  //  val exist_log = SynchronousFileSink(new File(s"${path}exist_log.log"), true)
//  //  val new_log = SynchronousFileSink(new File(s"${path}new_log.log"), true)
//  //  var num = 1
//  val sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm")
//  flowGraphRun()
//  Thread.sleep(30 * 1000)
//  system.actorSelection("/user/jokertest") ! CompleteThenStop
//  Thread.sleep(30 * 1000)
//  flowGraphRun()
//
//  //  object TradeContab {
//  //
//  //    def tradeMonitor() = {
//  //      val crontab = system.actorOf(CronTab.props, "crontab")
//  //
//  ////      val someOtherActor = system.actorOf(Props(new Actor {
//  ////        override def receive: Receive = {
//  ////          case "woo" ⇒ println("this code is executed")
//  ////        }
//  ////      }))
//  //
//  //      // send woo to someOtherActor once every minute
//  //      crontab ! CronTab.Schedule(system.actorSelection("/user/jokertest") , CompleteThenStop, CronExpression("* * * * *"))
//  //    }
//  //
//  //  }
//  def flowGraphRun()(implicit system: ActorSystem, materializer: ActorMaterializer): Unit = {
//    RunnableGraph.fromGraph(
//      FlowGraph.create() {
//        implicit b ⇒
//          import FlowGraph.Implicits._
//          val date: String = sdf.format(new Date())
//          val bcast = b.add(Broadcast[TeleporterKafkaMessage](3))
//          center.source[TeleporterKafkaMessage]("jokertest") ~> bcast.in
//
//          bcast.out(0).filter(
//            msg => {
//              IOUtils.toString(msg.data.value(), "utf-8").indexOf("") > 0
//            }
//          ).map(msg => {
//            TradeFlow.orderProcess(msg)
//          }) ~> SynchronousFileSink(new File("d://test/joker/joker/data/exist1.log"), true)
//
//          bcast.out(1).filter(msg => {
//            // HbaseUtil.insert(HbaseTmp("","","",""))>1
//            1 == 1
//          }).map(msg => {
//            TradeFlow.orderProcess(msg)
//          }) ~> SynchronousFileSink(new File("d://test/joker/joker/data/exist.log"), true)
//
//          bcast.out(2).filter(msg => {
//            // HbaseUtil.insert(HbaseTmp("","","",""))==1
//            2 == 2
//          }).map(msg => {
//            TradeFlow.orderProcess(msg)
//          }) ~> SynchronousFileSink(new File("d://test/joker/joker/data/new_log.log"), true)
//          ClosedShape
//      }).run()
//  }
//}
//
//object TradeFlow {
//
//
//  def orderProcess(msg: TeleporterKafkaMessage): ByteString = {
//
//    val message: String = ByteString(msg.data.value()).utf8String
//
//    val mapper = new ObjectMapper() with ScalaObjectMapper
//    mapper.registerModule(DefaultScalaModule)
//    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
//    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
//    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
//    val myMap = mapper.readValue[Map[String,Any]](message).get("payload").get.toString
//    val yourMap = mapper.readValue[Trade](myMap)
//    println(yourMap.toString())
//    ByteString(yourMap.toString())
//  }
//
//
//}
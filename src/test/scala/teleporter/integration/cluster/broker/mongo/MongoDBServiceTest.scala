package teleporter.integration.cluster.broker.mongo

import org.json4s._
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.FunSuite
import teleporter.integration.utils.Jackson

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by huanwuji on 2016/12/1.
  */
class MongoDBServiceTest extends FunSuite {
  val mongoClient = MongoClient("mongodb://172.18.21.213:27017")
  val database: MongoDatabase = mongoClient.getDatabase("teleporter")


  test("mongo atomic save") {
    val mongoDBService = MongoDBService(database.getCollection("config_copy"))
    println(mongoDBService.atomicPut("/stream/test/hadoop_es_test/hadoop2es",
      "{\"id\":447,\"key\":\"hadoop2es\",\"cron\":\"\",\"status\":\"NORMAL\",\"extraKeys\":{},\"errorRules\":[\"regex => stop()\"],\"arguments\":{},\"template\":\"//package teleporter.stream.integration.transaction\\n\\nimport akka.Done\\nimport akka.stream.scaladsl.Keep\\nimport akka.stream.{KillSwitch, KillSwitches}\\nimport teleporter.integration.component.hdfs.Hdfs\\nimport teleporter.integration.core.Streams._\\nimport teleporter.integration.core.{SourceAck, TeleporterCenter}\\n\\nimport scala.concurrent.Future\\n\\n/**\\n  * Created by huanwuji on 2016/10/20.\\n  */\\nobject Hadoop2Elasticsearch extends StreamLogic {\\n  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {\\n    import center.{materializer, self}\\n    Hdfs.sourceAck(\\\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\\\")\\n      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\\n      .map { m ⇒ println(m.data.utf8String); m }\\n      .to(SourceAck.confirmSink()).run()\\n    //    Hdfs.source(\\\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\\\")\\n    //      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\\n    //      .map { m ⇒ println(m.data.utf8String); m }\\n    //      .to(Sink.ignore).run()\\n  }\\n}\"}",
      //      """{"errorRules":["regex => stop()"],"extraKeys":{},"cron":"","key":"hadoop2es","id":447,"status":"NORMAL","template":"//package teleporter.stream.integration.transaction\n\nimport akka.Done\nimport akka.stream.scaladsl.Keep\nimport akka.stream.{KillSwitch, KillSwitches}\nimport teleporter.integration.component.hdfs.Hdfs\nimport teleporter.integration.core.Streams._\nimport teleporter.integration.core.{SourceAck, TeleporterCenter}\n\nimport scala.concurrent.Future\n\n/**\n  * Created by huanwuji on 2016/10/20.\n  */\nobject Hadoop2Elasticsearch extends StreamLogic {\n  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {\n    import center.{materializer, self}\n    Hdfs.sourceAck(\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\")\n      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\n      .map { m ⇒ println(m.data.utf8String); m }\n      .to(SourceAck.confirmSink()).run()\n    //    Hdfs.source(\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\")\n    //      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\n    //      .map { m ⇒ println(m.data.utf8String); m }\n    //      .to(Sink.ignore).run()\n  }\n}","arguments":{}}""",
      """{"errorRules":["regex => stop()"],"extraKeys":{},"cron":"","key":"hadoop2es","id":447,"status":"COMPLETE","template":"//package teleporter.stream.integration.transaction\n\nimport akka.Done\nimport akka.stream.scaladsl.Keep\nimport akka.stream.{KillSwitch, KillSwitches}\nimport teleporter.integration.component.hdfs.Hdfs\nimport teleporter.integration.core.Streams._\nimport teleporter.integration.core.{SourceAck, TeleporterCenter}\n\nimport scala.concurrent.Future\n\n/**\n  * Created by huanwuji on 2016/10/20.\n  */\nobject Hadoop2Elasticsearch extends StreamLogic {\n  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {\n    import center.{materializer, self}\n    Hdfs.sourceAck(\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\")\n      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\n      .map { m ⇒ println(m.data.utf8String); m }\n      .to(SourceAck.confirmSink()).run()\n    //    Hdfs.source(\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\")\n    //      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\n    //      .map { m ⇒ println(m.data.utf8String); m }\n    //      .to(Sink.ignore).run()\n  }\n}","arguments":{}}"""))
  }
  test("json serialize") {
    implicit val formats = DefaultFormats
    val json = "{\"id\":447,\"key\":\"hadoop2es\",\"cron\":\"\",\"status\":\"NORMAL\",\"extraKeys\":{},\"errorRules\":[\"regex => stop()\"],\"arguments\":{},\"template\":\"//package teleporter.stream.integration.transaction\\n\\nimport akka.Done\\nimport akka.stream.scaladsl.Keep\\nimport akka.stream.{KillSwitch, KillSwitches}\\nimport teleporter.integration.component.hdfs.Hdfs\\nimport teleporter.integration.core.Streams._\\nimport teleporter.integration.core.{SourceAck, TeleporterCenter}\\n\\nimport scala.concurrent.Future\\n\\n/**\\n  * Created by huanwuji on 2016/10/20.\\n  */\\nobject Hadoop2Elasticsearch extends StreamLogic {\\n  override def apply(key: String, center: TeleporterCenter): (KillSwitch, Future[Done]) = {\\n    import center.{materializer, self}\\n    Hdfs.sourceAck(\\\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\\\")\\n      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\\n      .map { m ⇒ println(m.data.utf8String); m }\\n      .to(SourceAck.confirmSink()).run()\\n    //    Hdfs.source(\\\"/source/test/hadoop_es_test/hadoop2es/hdfs_source\\\")\\n    //      .viaMat(KillSwitches.single)(Keep.right).watchTermination()(Keep.both)\\n    //      .map { m ⇒ println(m.data.utf8String); m }\\n    //      .to(Sink.ignore).run()\\n  }\\n}\"}"
    val map = Jackson.mapper.readValue[mutable.LinkedHashMap[String, Any]](json)
    val map1 = map.toMap
    //    val map1 = Map(1 → 2)
    //    val map2 = immutable.Map(2 → 3)
    println(Jackson.mapper.writeValueAsString(map1))
  }
}

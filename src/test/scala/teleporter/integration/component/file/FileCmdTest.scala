package teleporter.integration.component.file

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import teleporter.integration.component.file.FileCmd.Scan

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by huanwuji on 2017/4/5.
  */
class FileCmdTest extends FunSuite {
  implicit val system = ActorSystem("instance", ConfigFactory.load("instance"))
  implicit val mater = ActorMaterializer()
  test("testParse") {
    val aa = Scan.parser.parse(Seq("--offset=11", " ./test.cc"), Scan())
    val fu = FileCmd.exec("scan --offset 11 /tmp/hdfs/test.csv | st '\r\n' | grep 50820008").runForeach(s ⇒ println(s.utf8String))
    Await.result(fu, 1.minute)
  }
  test("command split") {
    val array = """afdk bbb -t "dee\"dddd"  'aa' c\'cc""".split("(?<!\\\\)[\"']").toSeq
    val builder = Seq.newBuilder[String]
    for (i ← array.indices) {
      if (i % 2 == 0) {
        builder ++= array(i).split("\\s+")
      } else {
        builder += array(i)
      }
    }
    println(builder.result())
  }
}

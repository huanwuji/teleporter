package teleporter.integration.component

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.commons.csv.CSVFormat
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2017/2/24.
  */
class CSV$Test extends FunSuite {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()
  val text: String =
    """
      |aaaaa,"bb
      |b,
      |bb",ccccc!
      |aaaa1,bbbbb,ccccc!
      |aaaa2,bbbbb,ccccc!
      |aaaa3,bbbbb,ccccc!
      |aaaa4,bbbbb,ccccc!
      |aaaa5,bbbbb,ccccc!
    """.stripMargin
  test("csv test") {
    val fu = Source(text.grouped(10).toIndexedSeq).map(ByteString(_))
      .via(CSV.flow(CSVFormat.DEFAULT.withRecordSeparator('\n'), 5))
      .runForeach(println(_))
    Await.result(fu, 1.minute)
  }
}
package teleporter.integration.component

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, TeleporterAttributes}
import teleporter.integration.component.jdbc.SqlSupport
import teleporter.integration.supervision.DecideRule

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Created by huanwuji 
  * date 2017/1/17.
  */
class TestSink() extends CommonSink[Int, Int, Unit]("test.sink", m ⇒ m) {
  override val initialAttributes: Attributes = super.initialAttributes and TeleporterAttributes.CacheDispatcher

  override def create(): Int = {
    println("create client")
    1
  }

  override def write(client: Int, elem: Int): Unit = {
    println(elem)
    throw new RuntimeException
  }

  override def close(client: Int): Unit = {
    println("Sink will closed")
  }
}

class TestSinkAsync(parallelism: Int, _create: (ExecutionContext) ⇒ Future[Int], _close: (Int, ExecutionContext) ⇒ Future[Done])
  extends CommonSinkAsyncUnordered[Int, Int, Int]("test.sink.async", parallelism, m ⇒ m) with SqlSupport {
  override def create(executionContext: ExecutionContext): Future[Int] = _create(executionContext)

  override def write(client: Int, elem: Int, executionContext: ExecutionContext): Future[Int] = {
    implicit val ec = executionContext
    Future {
      println(elem)
      throw new RuntimeException()
    }(executionContext)
  }

  override def close(client: Int, executionContext: ExecutionContext): Future[Done] = _close(client, executionContext)
}

object CommonSinkTest extends App {
  implicit val system = ActorSystem()
  implicit val mater = ActorMaterializer()

  testSinkAsync()

  def testSinkAsync()(implicit system: ActorSystem, mater: ActorMaterializer): Unit = {
    Source.repeat(1)
      .map(_ ⇒ Random.nextInt())
      .via(Flow.fromGraph(new TestSinkAsync(
        parallelism = 2,
        _create = _ ⇒ {
          println("create client")
          Future.successful(1)
        },
        _close = (c, _) ⇒ {
          println(s"$c client close")
          Future.successful(Done)
        }
      )))
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(Seq(DecideRule(".*", "INFO", "resume(delay=100.millis, retries=2, next=stop)")))))
      .to(Sink.ignore).run()
  }

  def testSink()(implicit system: ActorSystem, mater: ActorMaterializer): Unit = {
    Source.tick(1.second, 1.second, 1)
      .map(_ ⇒ Random.nextInt())
      .via(Flow.fromGraph(new TestSink()))
      .addAttributes(Attributes(TeleporterAttributes.SupervisionStrategy(Seq(DecideRule(".*", "INFO", "retry(delay = 2.second, retries=4, next=stop)")))))
      .to(Sink.ignore).run()
  }
}
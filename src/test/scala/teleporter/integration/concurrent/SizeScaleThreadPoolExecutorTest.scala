package teleporter.integration.concurrent

import java.util.concurrent.TimeUnit

import com.google.common.collect.Queues
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by huanwuji 
  * date 2017/2/6.
  */
class SizeScaleThreadPoolExecutorTest extends FunSuite with Matchers {
  test("size scale thread pool executor") {
    val threshold = 2
    val tasks = 16
    val threadPool = new SizeScaleThreadPoolExecutor(2, 10, threshold, 1, TimeUnit.MILLISECONDS,
      Queues.newLinkedBlockingDeque(),
      new ThreadFactoryBuilder().setNameFormat("size-scale-pool-%d").build())
    for (i ← 1 to tasks) {
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          println(Thread.currentThread().getName)
          Thread.sleep(5 * 1000)
        }
      })
    }
    Thread.sleep(2 * 1000)
    threadPool.getCorePoolSize should be((tasks - 2) / threshold)
    Thread.sleep(15 * 1000)
    threadPool.getCorePoolSize should be(2)
  }
}

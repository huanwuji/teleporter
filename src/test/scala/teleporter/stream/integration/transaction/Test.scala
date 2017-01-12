package teleporter.stream.integration.transaction

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

/**
  * Created by huanwuji on 2016/10/28.
  */
object Test extends App {
  val pool = new ThreadPoolExecutor(2, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](1))
  for (i ← 1 to 10) {
    pool.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          println(s"${Thread.currentThread().getName}: $i")
          Thread.sleep(2000)
        }
      }
    })
  }
}
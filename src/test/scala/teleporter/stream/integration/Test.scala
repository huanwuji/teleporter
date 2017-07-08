import java.util.concurrent._

object Test extends App {
  val jddjExecutor = new ThreadPoolExecutor(
    5, 10, 2, TimeUnit.MINUTES,
    new LinkedBlockingQueue[Runnable]())
  for (i ← 1 to 50) {
    jddjExecutor.submit(new FutureTask[Object](new Callable[Object] {
      override def call(): Object = {
        Thread.sleep(5 * 1000)
        println("Was executor")
        null
      }
    }))
  }
  Thread.sleep(10 * 1000)
  println("queue size" + jddjExecutor.getQueue.size())
}
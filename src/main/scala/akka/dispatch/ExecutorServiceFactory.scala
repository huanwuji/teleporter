package akka.dispatch

import java.util.concurrent._

import teleporter.integration.concurrent.SizeScaleThreadPoolExecutor

import scala.concurrent.duration.Duration

/**
  * Created by huanwuji 
  * date 2016/12/16.
  */
object TeleporterExecutorServiceFactory

class CacheThreadPoolFactoryProvider extends ExecutorServiceFactoryProvider {

  class ThreadPoolExecutorServiceFactory(val threadFactory: ThreadFactory) extends ExecutorServiceFactory {
    def createExecutorService: ExecutorService = {
      Executors.newCachedThreadPool(threadFactory)
    }
  }

  final def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {
    val tf = threadFactory match {
      case m: MonitorableThreadFactory ⇒
        // add the dispatcher id to the thread names
        m.withName(m.name + "-" + id)
      case other ⇒ other
    }
    new ThreadPoolExecutorServiceFactory(tf)
  }
}

final case class SizeScaleThreadPoolConfig(
                                            allowCorePoolTimeout: Boolean = ThreadPoolConfig.defaultAllowCoreThreadTimeout,
                                            corePoolSize: Int = ThreadPoolConfig.defaultCorePoolSize,
                                            maxPoolSize: Int = ThreadPoolConfig.defaultMaxPoolSize,
                                            sizeThreshold: Int = 1,
                                            threadTimeout: Duration = ThreadPoolConfig.defaultTimeout,
                                            queueFactory: ThreadPoolConfig.QueueFactory = ThreadPoolConfig.linkedBlockingQueue(),
                                            rejectionPolicy: RejectedExecutionHandler = ThreadPoolConfig.defaultRejectionPolicy)
  extends ExecutorServiceFactoryProvider {

  class ThreadPoolExecutorServiceFactory(val threadFactory: ThreadFactory) extends ExecutorServiceFactory {
    def createExecutorService: ExecutorService = {
      val service: ThreadPoolExecutor = new SizeScaleThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        sizeThreshold,
        threadTimeout.length,
        threadTimeout.unit,
        queueFactory(),
        threadFactory,
        rejectionPolicy) with LoadMetrics {
        def atFullThrottle(): Boolean = this.getActiveCount >= this.getPoolSize
      }
      service.allowCoreThreadTimeOut(allowCorePoolTimeout)
      service
    }
  }

  final def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {
    val tf = threadFactory match {
      case m: MonitorableThreadFactory ⇒
        // add the dispatcher id to the thread names
        m.withName(m.name + "-" + id)
      case other ⇒ other
    }
    new ThreadPoolExecutorServiceFactory(tf)
  }
}
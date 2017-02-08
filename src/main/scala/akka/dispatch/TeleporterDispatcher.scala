package akka.dispatch

import java.util.concurrent.ThreadFactory

import com.typesafe.config.Config

/**
  * Created by huanwuji 
  * date 2016/12/16.
  */
object TeleporterDispatcher

class CacheThreadPoolExecutorConfigurator(config: Config, prerequisites: DispatcherPrerequisites) extends ExecutorServiceConfigurator(config, prerequisites) {
  def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
    new CacheThreadPoolFactoryProvider().createExecutorServiceFactory(id, threadFactory)
}

class SizeScaleThreadPoolExecutorConfigurator(config: Config, prerequisites: DispatcherPrerequisites) extends ExecutorServiceConfigurator(config, prerequisites) {

  val sizeScaleThreadPoolConfig: SizeScaleThreadPoolConfig = createThreadPoolConfig(config.getConfig("size-scale-pool-executor"), prerequisites)

  protected def createThreadPoolConfig(config: Config, prerequisites: DispatcherPrerequisites): SizeScaleThreadPoolConfig = {
    import akka.util.Helpers.ConfigOps
    SizeScaleThreadPoolConfig(
      allowCorePoolTimeout = config getBoolean "allow-core-timeout",
      corePoolSize = ThreadPoolConfig.scaledPoolSize(config getInt "core-pool-size-min", config getDouble "core-pool-size-factor", config getInt "core-pool-size-max"),
      maxPoolSize = ThreadPoolConfig.scaledPoolSize(config getInt "max-pool-size-min", config getDouble "max-pool-size-factor", config getInt "max-pool-size-max"),
      sizeThreshold = config getInt "size-threshold",
      threadTimeout = config.getMillisDuration("keep-alive-time"),
      queueFactory = (Some(config getInt "task-queue-size") flatMap {
        case size if size > 0 ⇒
          Some(config getString "task-queue-type") map {
            case "array" ⇒ ThreadPoolConfig.arrayBlockingQueue(size, fair = false) //TODO config fairness?
            case "" | "linked" ⇒ ThreadPoolConfig.linkedBlockingQueue(size)
            case x ⇒ throw new IllegalArgumentException("[%s] is not a valid task-queue-type [array|linked]!" format x)
          }
        case _ ⇒ None
      }).getOrElse(ThreadPoolConfig.linkedBlockingQueue()),
      rejectionPolicy = ThreadPoolConfig.defaultRejectionPolicy
    )
  }

  def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
    sizeScaleThreadPoolConfig.createExecutorServiceFactory(id, threadFactory)
}
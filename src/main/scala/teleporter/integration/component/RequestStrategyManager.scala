package teleporter.integration.component

import akka.stream.actor._
import com.typesafe.scalalogging.LazyLogging

/**
  * Author: kui.dai
  * Date: 2015/12/14.
  */
class RequestStrategyManager(autoStart: Boolean, var autoRequestStrategy: RequestStrategy = null) extends RequestStrategy {
  private val pauseRequestStrategy = ZeroRequestStrategy
  private var _pause: Boolean = !autoStart

  def isPause: Boolean = _pause

  def stop() = _pause = true

  def start() = _pause = false

  override def requestDemand(remainingRequested: Int): Int = {
    val _remainingRequested = if (remainingRequested < 0) 0 else remainingRequested
    if (_pause) {
      pauseRequestStrategy.requestDemand(_remainingRequested)
    } else {
      require(autoRequestStrategy != null, "you must declare it before start")
      autoRequestStrategy.requestDemand(_remainingRequested)
    }
  }

  def autoRequestStrategy(requestStrategy: RequestStrategy): Unit = autoRequestStrategy = requestStrategy
}

object RequestStrategyManager extends LazyLogging {
  def apply(autoStart: Boolean = true): RequestStrategyManager = {
    if (autoStart) {
      apply(autoStart, WatermarkRequestStrategy(32))
    } else {
      apply(autoStart, null)
    }
  }

  def apply(autoStart: Boolean, autoRequestStrategy: RequestStrategy): RequestStrategyManager = {
    new RequestStrategyManager(autoStart, autoRequestStrategy)
  }
}

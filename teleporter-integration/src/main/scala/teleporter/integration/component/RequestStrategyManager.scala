package teleporter.integration.component

import akka.stream.actor._
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.conf.Conf.Props
import teleporter.integration.conf.PropsSupport

/**
 * Author: kui.dai
 * Date: 2015/12/14.
 */
class RequestStrategyManager(requestStrategy: RequestStrategy) extends RequestStrategy {
  val zeroRequestStrategy = ZeroRequestStrategy
  private var _pause: Boolean = false

  def isPause() = _pause

  def pause() = _pause = true

  def resume() = _pause = false

  override def requestDemand(remainingRequested: Int): Int = {
    val _remainingRequested = if (remainingRequested < 0) 0 else remainingRequested
    if (_pause) {
      zeroRequestStrategy.requestDemand(_remainingRequested)
    } else {
      requestStrategy.requestDemand(_remainingRequested)
    }
  }
}

object RequestStrategyManager extends PropsSupport with LazyLogging {
  val WatermarkReg = """watermark\((\d+),(\d+)\)""".r

  def apply(props: Props): RequestStrategyManager = {
    val requestStrategy = getStringOpt(props, "requestStrategy") match {
      case None ⇒ WatermarkRequestStrategy(100)
      case Some(config) ⇒ apply(config)
    }
    new RequestStrategyManager(requestStrategy)
  }

  def apply(config: String): RequestStrategyManager = {
    val requestStrategy = config match {
      case "oneByOne" ⇒ OneByOneRequestStrategy
      case "zero" ⇒ ZeroRequestStrategy
      case WatermarkReg(high, low) ⇒ WatermarkRequestStrategy(high.toInt, low.toInt)
      case _ ⇒
        logger.warn(s"Can't parse strategy $config, will use default")
        WatermarkRequestStrategy(100)
    }
    new RequestStrategyManager(requestStrategy)
  }

  def apply(requestStrategy: RequestStrategy): RequestStrategyManager = {
    new RequestStrategyManager(requestStrategy)
  }
}

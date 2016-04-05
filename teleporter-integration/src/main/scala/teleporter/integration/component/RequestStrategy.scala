package teleporter.integration.component

import akka.stream.actor.{OneByOneRequestStrategy, RequestStrategy, WatermarkRequestStrategy, ZeroRequestStrategy}
import com.typesafe.scalalogging.LazyLogging
import teleporter.integration.conf.Conf.Props
import teleporter.integration.conf.PropsSupport

/**
 * Author: kui.dai
 * Date: 2015/12/14.
 */
object RequestStrategy extends PropsSupport with LazyLogging {
  val WatermarkReg = """watermark\((\d+),(\d+)\)""".r

  def apply(props: Props): RequestStrategy = {
    getStringOpt(props, "requestStrategy") match {
      case None ⇒ WatermarkRequestStrategy(100)
      case Some(config) ⇒ apply(config)
    }
  }

  def apply(config: String): RequestStrategy = {
    config match {
      case "oneByOne" ⇒ OneByOneRequestStrategy
      case "zero" ⇒ ZeroRequestStrategy
      case WatermarkReg(high, low) ⇒ WatermarkRequestStrategy(high.toInt, low.toInt)
      case _ ⇒
        logger.warn(s"Can't parse strategy $config, will use default")
        WatermarkRequestStrategy(100)
    }
  }
}

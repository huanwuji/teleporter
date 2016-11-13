package teleporter.integration.cluster.instance

import teleporter.integration.core.TeleporterCenter

/**
  * Created by kui.dai on 2016/8/9.
  */
object Instance {
  def main(args: Array[String]): Unit = {
    val center = TeleporterCenter()
    center.start()
  }
}
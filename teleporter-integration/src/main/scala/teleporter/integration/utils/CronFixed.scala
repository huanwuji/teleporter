package teleporter.integration.utils

/**
 * Author: kui.dai
 * Date: 2016/3/17.
 */
object CronFixed {
  val split = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)".r

  def fixed(cron: String): String = split.replaceAllIn(cron, "$2 $1 $3 $4 $5")
}

package teleporter.integration.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

/**
  * Author: kui.dai
  * Date: 2015/12/29.
  */
object Dates {
  val DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  implicit def toDate(str: String): Date = Date.from(LocalDateTime.parse(str, Dates.DEFAULT_DATE_FORMATTER).atZone(ZoneId.systemDefault()).toInstant)
}
package teleporter.integration.utils

import com.google.common.base.CaseFormat

/**
  * Created by huanwuji 
  * date 2017/3/8.
  */
object CaseFormats {
  val UNDERSCORE_SNAKE: com.google.common.base.Converter[String, String] = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL)
  val SNAKE_UNDERSCORE: com.google.common.base.Converter[String, String] = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE)
}

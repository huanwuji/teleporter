package teleporter.integration.component.jdbc

/**
  * Created by huanwuji 
  * date 2017/3/15.
  */
object Mysql extends Jdbc {
  override def prepareProperties(bean: JdbcAddressMetaBean): Map[String, Any] = {
    bean.toMap + (
      "driverClassName " → "com.mysql.jdbc.Driver",
      "jdbcUrl" → s"jdbc:mysql://${bean.host}/${bean.database}"
    )
  }
}

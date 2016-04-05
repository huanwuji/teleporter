package teleporter.web

/**
 * Author: kui.dai
 * Date: 2015/10/29.
 */
object JdbcTest extends App {
  //    implicit val actorSystem = ActorSystem()
  //    implicit val materializer = ActorMaterializer()
  //    implicit val dispatcher = actorSystem.dispatcher
  //    val hikariConfig = new HikariConfig("/hikari.properties")
  //    val dataSource = new HikariDataSource(hikariConfig)
  //    val conn = dataSource.getConnection
  //    val ps = conn.prepareStatement("insert into rfm values(?)")
  //    ps.setString(1, "summ_er\uD83C\uDF56")
  //    ps.execute()
  //    ps.close()
  //    val ps1 = conn.prepareStatement("insert into rfm_copy values(?)")
  //    ps1.setString(1, "虽哩辊 里")
  //    ps1.execute()
  //    conn.close()
  new StringBuilder()
  val str = "summ\uD83C\uDF56\uD83C\uDF56\uD83C\uDF56\uD83C\uDF56\u0333"
  //  println(str.length)
  //  println(util.Arrays.toString(str.chars().asLongStream().toArray))
  //  for(i ← 0 until str.length) {
  //    println(str.codePointAt(i))
  //  }

  //  "[\uD83C\uDF00-\uD83D\uDDFF]|[\uD83D\uDE00-\uD83D\uDE4F]|[\uD83D\uDE80-\uD83D\uDEFF]|[\u2600-\u26FF]|[\u2700-\u27BF]"

  val emojis = "([\\uD83C-\\uDBFF\\uDC00-\\uDFFF])".r
  val replace = emojis.replaceAllIn(str, m ⇒ {
    val group1 = m.group(1)
    println(group1)
    val digit = Character.toCodePoint(group1.charAt(0), group1.charAt(1))
    "\\\\u" + Integer.toHexString(digit).toUpperCase
  })

  println(replace)

  //  println(util.Arrays.toString(str))
  //println(utf8mbr2utf8(str))
  //println(utf8mbr2utf8("时加时时器黑暗顺fjdkjf#3"))

  def utf8mbr2utf8(str: String): String = {
    val strings = for (char ← str.toCharArray) yield {
      if (char > 0xFFFF) {
        "\\u" + Integer.toHexString(char).toUpperCase
      } else {
        char
      }
    }
    strings.mkString
  }
}

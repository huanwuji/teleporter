package teleporter.web

/**
 * Author: kui.dai
 * Date: 2015/11/12.
 */
object MongoSupport {
  def seqToJson(seq: Seq[String]): String = "[" + seq.mkString(",") + "]"
}
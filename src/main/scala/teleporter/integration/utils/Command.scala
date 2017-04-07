package teleporter.integration.utils

/**
  * Created by huanwuji on 2017/4/5.
  */
object Command {
  def cmdSplit(s: String): Seq[String] = {
    val builder = Seq.newBuilder[String]
    val arr = s.split("(?<!\\\\)[\"']")
    for (i ← arr.indices) {
      if (i % 2 == 0) {
        builder ++= arr(i).split("\\s+")
      } else {
        builder += arr(i)
      }
    }
    builder.result()
  }
}

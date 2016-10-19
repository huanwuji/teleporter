package teleporter.integration.script

/**
 * Author: kui.dai
 * Date: 2016/2/19.
 */
trait Template

object Template extends Template {
  val expressionParamGroupRegex = "\\{(.+?)\\}".r

  def apply(template: String, params: Map[String, Any]): String = {
    expressionParamGroupRegex.replaceAllIn(template, mh ⇒ params(mh.group(1)).toString)
  }
}

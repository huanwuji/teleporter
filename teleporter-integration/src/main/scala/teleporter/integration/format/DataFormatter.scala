package teleporter.integration.format

/**
 * Author: kui.dai
 * Date: 2015/11/27.
 */
trait DataConvert[I, O] extends (I ⇒ O)

object DataFormatter
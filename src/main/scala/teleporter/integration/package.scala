package teleporter

import teleporter.integration.core.{ClientRef, TeleporterCenter}

/**
  * Author: kui.dai
  * Date: 2015/12/4.
  */
package object integration {
  type ClientApply[A] = ((String, TeleporterCenter) ⇒ ClientRef[A])
}
package teleporter.integration.script

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import teleporter.integration.component._
import teleporter.integration.conf.Conf.Task

/**
 * Created by joker on 15/12/10
 */


object ScalaHotLoader {


  def load[A, B](task: Task): Flow[A, B, _] = {
    val flow = ScriptEngines.getScala.eval(task.props.get("Flow").toString).asInstanceOf[Flow[A, B, _]]
    flow
  }
}

trait FlowBuilder[I, O] {

  def build(): Flow[I, O, Unit]

}


trait DataConvert[I, O] extends (I ⇒ O)


//class FileProcessFlow(script: String) extends FlowBuilder[MessageTmp, ByteString] {
//  override def build(): Flow[MessageTmp, ByteString, Unit] = {
//    val process = ScalaHotLoader.eval(script).asInstanceOf[DataConvert[MessageTmp, ByteString]]
//    val flow = Flow[MessageTmp].map(msg ⇒ {
//      process.apply(msg)
//    })
//    flow
//  }
//
//
//}


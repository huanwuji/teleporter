package teleporter.integration.script

/**
 * Created by joker on 15/12/10.
 */



object ScalaHotLoader {

  def eval(script:String):Unit={
    ScriptEngines.getScala.eval(script)

  }


}

trait FlowProcess[T]{

  def process 
}


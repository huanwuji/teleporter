package teleporter.integration.component.kv

/**
  * Created by huanwuji 
  * date 2016/12/22.
  */
trait KVOperator {
  def apply(key: Array[Byte]): Array[Byte]

  def get(key: Array[Byte]): Option[Array[Byte]]

  def put(key: Array[Byte], value: Array[Byte]): Unit

  def atomicPut(key: Array[Byte], expectValue: Array[Byte], updateValue: Array[Byte]): Boolean

  def remove(key: Array[Byte]): Unit

  def range(key: Array[Byte] = Array.emptyByteArray): Iterator[(Array[Byte], Array[Byte])]
}
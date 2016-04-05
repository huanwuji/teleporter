package teleporter.integration.persistence

import java.io.File
import java.nio.ByteBuffer
import java.util

import org.iq80.leveldb.Options
import org.iq80.leveldb.impl.Iq80DBFactory
import org.scalatest.FunSuite

/**
 * author: huanwuji
 * created: 2015/8/13.
 */
class LevelDB$Test extends FunSuite {
  test("skipList") {
    val db = Iq80DBFactory.factory.open(new File("/tmp/leveldb"), new Options)
    var key = ByteBuffer.allocate(12)
    key.putInt(100)
    key.putLong(1)
    println(util.Arrays.toString(key.array()))
    db.put(key.array(), "aaa".getBytes)
    key = ByteBuffer.allocate(12)
    key.putInt(100)
    key.putLong(2)
    println(util.Arrays.toString(key.array()))
    db.put(key.array(), "aaa".getBytes)
    key = ByteBuffer.allocate(12)
    key.putInt(100)
    key.putLong(3)
    db.put(key.array(), "aaa".getBytes)
    val it = db.iterator()
    key = ByteBuffer.allocate(12)
    key.putInt(100)
    key.putLong(3)
    it.seek(key.array())
    while (it.hasNext) {
      val array = it.next()
      val origin = ByteBuffer.wrap(array.getKey)
      println(s"${origin.getInt}, ${origin.getLong(4)}")
    }
  }
}

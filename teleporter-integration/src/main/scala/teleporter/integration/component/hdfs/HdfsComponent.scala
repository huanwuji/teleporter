package teleporter.integration.component.hdfs

import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import teleporter.integration.conf.{Conf, PropsSupport}
import teleporter.integration.core._

import scala.annotation.tailrec


/**
 * Created by joker on 15/10/9
 */

class HdfsAddressBuilder(override val conf: Conf.Address)(implicit override val center: TeleporterCenter) extends AddressBuilder[FileSystem] with PropsSupport {
  override def build: Address[FileSystem] = {
    val config = HBaseConfiguration.create()
    cmpProps(conf.props).foreach(t2 ⇒ config.set(t2._1, String.valueOf(t2._2)))
    new AutoCloseAddress[FileSystem](conf, FileSystem.get(config))
  }
}


object HdfsUtil extends LazyLogging {
  def loadFile(srcPath: String, dstPath: String, userName: String): Unit = {
    val ugi: UserGroupInformation = UserGroupInformation.createRemoteUser(userName)
    ugi.doAs(new PrivilegedExceptionAction[Void] {

      override def run(): Void = {
        try {
          val config = new HdfsConfiguration()
          val fileSystem: FileSystem = FileSystem.get(config)
          val src: Path = new Path(srcPath)
          val desc: Path = new Path(dstPath)
          fileSystem.copyFromLocalFile(false, true, src, desc)

          if(!fileSystem.exists(desc))
            run()
          else
            null
        }
        catch {
          case ex: Exception =>
            logger.error(ex.getLocalizedMessage, ex)
            null
        }




      }
    })
  }
}








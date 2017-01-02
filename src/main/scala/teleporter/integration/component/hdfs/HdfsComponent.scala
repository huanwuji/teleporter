package teleporter.integration.component.hdfs

import java.net.URI
import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import teleporter.integration.ClientApply
import teleporter.integration.core._


/**
  * Created by joker on 15/10/9
  */
object HdfsMetaBean {
  val FUri = "uri"
  val FConf = "conf"
  val FUser = "user"
}

class HdfsMetaBean(override val underlying: Map[String, Any]) extends AddressMetaBean(underlying) {

  import HdfsMetaBean._

  def uri: String = client[String](FUri)

  def conf: String = client[String](FConf)

  def user: String = client[String](FUser)
}

object HdfsComponent {
  val hdfsApply: ClientApply = (key, center) ⇒ {
    val config = center.context.getContext[AddressContext](key).config.mapTo[HdfsMetaBean]
    val conf = new Configuration(false)
    val fileSystem = if (config.user.isEmpty) {
      FileSystem.get(new URI(config.uri), conf)
    } else {
      FileSystem.get(new URI(config.uri), conf, config.user)
    }
    AutoCloseClientRef[FileSystem](key, fileSystem)
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
          if (!fileSystem.exists(desc))
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
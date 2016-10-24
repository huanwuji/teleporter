package teleporter.integration.component.hdfs

import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import teleporter.integration.ClientApply
import teleporter.integration.core._


/**
  * Created by joker on 15/10/9
  */
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

object HdfsComponent extends SourceMetadata {
  val hdfs: ClientApply = (key, center) ⇒ {
    implicit val clientConfig = center.context.getContext[AddressContext](key).config
    val hdfsConfig = HBaseConfiguration.create()
    lnsClient.toMap.foreach { case (k: String, v: Any) ⇒ hdfsConfig.set(k, String.valueOf(v)) }
    AutoCloseClientRef[FileSystem](key, FileSystem.get(hdfsConfig))
  }
}
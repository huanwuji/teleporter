package teleporter.integration.core

import java.nio.file.{Files, Paths}

import akka.actor.{Actor, ActorRef}
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import org.apache.commons.lang.StringUtils
import org.apache.logging.log4j.scala.Logging
import teleporter.integration.cache.{PersistentCacheInfo, PersistentCacheMonitor}
import teleporter.integration.core.LocalStatus.{Sync, _}
import teleporter.integration.utils.Jackson

import scala.concurrent.duration._

/**
  * Created by huanwuji 
  * date 2016/12/26.
  */
case class InstanceStatus(cachesInfo: Set[PersistentCacheInfo])

class LocalStatusActor(statusFilePath: String)(implicit center: TeleporterCenter) extends Actor with Logging {

  import center.materializer
  import context.dispatcher

  var instanceStatus: InstanceStatus = _

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    val path = Paths.get(statusFilePath)
    if (Files.notExists(path)) Files.createFile(path)
    val json = new String(Files.readAllBytes(path))
    if (StringUtils.isNotEmpty(json)) {
      this.instanceStatus = Jackson.mapper.readValue[InstanceStatus](json)
    } else this.instanceStatus = InstanceStatus(Set.empty)
    val cacheMonitor: ActorRef = context.actorOf(PersistentCacheMonitor.props())
    context.system.scheduler.schedule(10.minutes, 6.hours, cacheMonitor, SyncAll(instanceStatus.cachesInfo.toSeq))
  }

  override def receive: Receive = {
    case StartLocalPersistentCache(path, database, storageType, tableName, expired) ⇒
      val infoOpt = instanceStatus.cachesInfo
        .find(info ⇒ info.path == path && info.database == database && info.storageType == storageType && info.tableName == tableName)
      val cacheInfo = infoOpt match {
        case Some(info) ⇒ info.copy(updated = System.currentTimeMillis(), expired = expired.toMillis)
        case None ⇒
          PersistentCacheInfo(
            path = path,
            database = database,
            storageType = storageType,
            tableName = tableName,
            created = System.currentTimeMillis(),
            updated = System.currentTimeMillis(),
            expired = expired.toMillis,
            maxSize = Long.MaxValue,
            count = 0,
            latestClearTime = -1)
      }
      self ! Sync(cacheInfo)
    case Sync(info) ⇒
      instanceStatus.copy(cachesInfo = instanceStatus.cachesInfo + info)
      self ! Snapshot
    case SyncAll(cachesInfo) ⇒
      cachesInfo.foreach(self ! Sync(_))
    case Snapshot ⇒
      Source(Jackson.toStr(instanceStatus)).map(ByteString(_)).runWith(FileIO.toPath(Paths.get(statusFilePath)))
        .onComplete(r ⇒ logger.info(s"Save instance status, $r"))
  }
}

object LocalStatus {

  sealed trait Action

  case class StartLocalPersistentCache(path: String, database: String, storageType: String, tableName: String, expired: Duration)

  case class Sync(cacheInfo: PersistentCacheInfo)

  case class SyncAll(cachesInfo: Seq[PersistentCacheInfo])

  case object Snapshot

}
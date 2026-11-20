// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  BlockLocation,
  FileStatus,
  FileSystem,
  FSDataInputStream,
  FSDataOutputStream,
  Path
}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.TaskContext

import java.net.URI
import java.io.Closeable
import scala.collection.mutable
import scala.util.{Using, control}

/**
 * Reuses private delegates during driver planning and Spark tasks. Calls outside either lifecycle
 * retain operation/stream ownership; Hadoop's ambient cached clients are never used.
 */
private[kusto] class KustoBlobReadFileSystem extends FileSystem {
  private var uri: URI = _
  private var workingDirectory: Path = _
  private var delegateConf: Configuration = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    uri = new URI(name.getScheme, name.getAuthority, "/", null, null)
    workingDirectory = new Path(uri)
    setConf(new Configuration(conf))
    delegateConf = new Configuration(conf)
    conf
      .getTrimmedStringCollection(KustoBlobReadFileSystem.UnsetKeys)
      .forEach(key => delegateConf.unset(key))
    val implementation = conf.get(KustoBlobReadFileSystem.DelegateImplementation)
    require(
      implementation != null && implementation != getClass.getName,
      "A distinct blob filesystem implementation is required")
    delegateConf.set(s"fs.${name.getScheme}.impl", implementation)
  }

  override def getUri: URI = uri
  override def getScheme: String = uri.getScheme
  override def getWorkingDirectory: Path = workingDirectory
  override def setWorkingDirectory(path: Path): Unit = {
    workingDirectory = makeQualified(path)
  }

  private def newDelegate(): FileSystem =
    FileSystem.newInstance(uri, new Configuration(delegateConf))

  private def scopedDelegate: Option[FileSystem] =
    KustoBlobReadFileSystem.scope(getConf).map(_.delegate(uri, delegateConf, () => newDelegate()))

  private def withDelegate[T](operation: FileSystem => T): T =
    scopedDelegate match {
      case Some(delegate) => operation(delegate)
      case None => Using.resource(newDelegate())(operation)
    }

  override def getFileStatus(path: Path): FileStatus =
    withDelegate(_.getFileStatus(makeQualified(path)))

  override def listStatus(path: Path): Array[FileStatus] =
    withDelegate(_.listStatus(makeQualified(path)))

  override def getFileBlockLocations(
      status: FileStatus,
      start: Long,
      length: Long): Array[BlockLocation] =
    withDelegate(_.getFileBlockLocations(status, start, length))

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val scope = KustoBlobReadFileSystem.scope(getConf)
    val delegate = scope
      .map(_.delegate(uri, delegateConf, () => newDelegate()))
      .getOrElse(newDelegate())
    var stream: FSDataInputStream = null
    try {
      stream = delegate.open(makeQualified(path), bufferSize)
      val owned = new FSDataInputStream(stream) {
        private var closed = false
        override def close(): Unit = synchronized {
          if (!closed) {
            closed = true
            try {
              if (scope.isDefined) super.close()
              else Using.resource(delegate)(_ => super.close())
            } finally scope.foreach(_.forget(this))
          }
        }
      }
      scope.foreach(_.track(owned))
      owned
    } catch {
      case error: Throwable =>
        if (stream != null) KustoBlobReadFileSystem.closeAfterFailure(stream, error)
        if (scope.isEmpty) KustoBlobReadFileSystem.closeAfterFailure(delegate, error)
        throw error
    }
  }

  private def readOnly(): Nothing =
    throw new UnsupportedOperationException("Kusto blob readback is read-only")

  override def create(
      path: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = readOnly()

  override def append(path: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    readOnly()
  override def rename(source: Path, destination: Path): Boolean = readOnly()
  override def delete(path: Path, recursive: Boolean): Boolean = readOnly()
  override def mkdirs(path: Path, permission: FsPermission): Boolean = readOnly()
}

private[kusto] object KustoBlobReadFileSystem {
  val DelegateImplementation = "kusto.blob.readback.fs.impl"
  val ReadId = "kusto.blob.readback.id"
  val UnsetKeys = "kusto.blob.readback.unset"
  private val driverScopes = mutable.Map.empty[String, Scope]
  private val taskScopes = mutable.Map.empty[TaskContext, Scope]

  private class Scope extends AutoCloseable {
    private val delegates =
      mutable.Map.empty[(String, String, String, UserGroupInformation), FileSystem]
    private val streams = mutable.Set.empty[Closeable]
    private var closed = false

    def delegate(uri: URI, conf: Configuration, create: () => FileSystem): FileSystem =
      synchronized {
        require(!closed, "Blob read lifecycle has completed")
        val key = (
          conf.get(ReadId),
          uri.toString,
          conf.get(DelegateImplementation),
          UserGroupInformation.getCurrentUser)
        delegates.getOrElseUpdate(key, create())
      }

    def track(stream: Closeable): Unit = synchronized {
      require(!closed, "Blob read lifecycle has completed")
      streams.add(stream)
    }

    def forget(stream: Closeable): Unit = synchronized { streams.remove(stream) }

    override def close(): Unit = {
      val (ownedDelegates, ownedStreams) = synchronized {
        if (closed) (Vector.empty[FileSystem], Vector.empty[Closeable])
        else {
          closed = true
          val owned = (delegates.values.toVector, streams.toVector)
          delegates.clear()
          streams.clear()
          owned
        }
      }
      // Using closes in reverse order: streams before the clients they depend on.
      Using.Manager { use =>
        ownedDelegates.foreach(use(_))
        ownedStreams.foreach(use(_))
      }.get
    }
  }

  private def closeAfterFailure(resource: AutoCloseable, error: Throwable): Unit =
    try resource.close()
    catch { case control.NonFatal(cleanup) => if (cleanup ne error) error.addSuppressed(cleanup) }

  private def scope(conf: Configuration): Option[Scope] = {
    if (conf.get(ReadId) == null) return None
    Option(TaskContext.get()) match {
      case Some(task) =>
        taskScopes.synchronized {
          if (task.isCompleted()) None
          else {
            taskScopes.get(task).orElse {
              val created = new Scope
              taskScopes.put(task, created)
              try {
                task.addTaskCompletionListener[Unit] { _ =>
                  taskScopes.synchronized { taskScopes.remove(task) }
                  created.close()
                }
                if (task.isCompleted()) None else Some(created)
              } catch {
                case error: Throwable =>
                  taskScopes.remove(task)
                  closeAfterFailure(created, error)
                  throw error
              }
            }
          }
        }
      case None => driverScopes.synchronized { driverScopes.get(conf.get(ReadId)) }
    }
  }

  def withReadScope[T](readId: String)(body: => T): T = {
    val scope = new Scope
    driverScopes.synchronized {
      require(!driverScopes.contains(readId), "Blob read lifecycle is already active")
      driverScopes.put(readId, scope)
    }
    try Using.resource(scope)(_ => body)
    finally driverScopes.synchronized { driverScopes.remove(readId) }
  }
}

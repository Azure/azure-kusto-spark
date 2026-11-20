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

import java.net.URI
import scala.util.Using

/**
 * Read-scoped blob access. Spark closes Parquet streams, not their FileSystems, so metadata
 * operations and input streams own fresh delegates instead of leaking uncached ABFS clients.
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

  private def withDelegate[T](operation: FileSystem => T): T =
    Using.resource(newDelegate())(operation)

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
    val delegate = newDelegate()
    var transferred = false
    try {
      val stream = delegate.open(makeQualified(path), bufferSize)
      val owned = new FSDataInputStream(stream) {
        private var closed = false
        override def close(): Unit = synchronized {
          if (!closed) {
            closed = true
            Using.resource(delegate)(_ => super.close())
          }
        }
      }
      transferred = true
      owned
    } finally {
      if (!transferred) delegate.close()
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
}

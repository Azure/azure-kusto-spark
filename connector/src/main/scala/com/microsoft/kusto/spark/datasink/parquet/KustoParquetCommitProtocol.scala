package com.microsoft.kusto.spark.datasink.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol

import java.util.UUID

class KustoParquetCommitProtocol(jobId: String, path: String) extends FileCommitProtocol with Serializable {
  override def setupJob(jobContext: JobContext): Unit = {}

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {}

  override def abortJob(jobContext: JobContext): Unit = {}

  override def setupTask(taskContext: TaskAttemptContext): Unit = {}

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val uuid = UUID.randomUUID.toString
    val filename = f"part-$split%05d-$uuid$ext"

    val file = dir.map { d =>
      new Path(new Path(path, d), filename).toString
    }.getOrElse {
      new Path(path, filename).toString
    }

    file
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    ""
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    FileCommitProtocol.EmptyTaskCommitMessage
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {}
}

package com.microsoft.kusto.spark.datasink.parquet

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol

class KustoFileCommitProtocol(jobId: String, path: String, dynamicPartitionOverwrite: Boolean)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite){

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    super.newTaskTempFile(taskContext, dir, "kusto-w.snappy.parquet")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    val partitionResult = taskContext.getTaskAttemptID
    val tcm = super.commitTask(taskContext)
    logWarning(s"###########################Commit task with path $path " +
      s"with file ${super.getFilename(taskContext, "kusto-w.snappy.parquet")} ###########################")
    tcm
  }
}

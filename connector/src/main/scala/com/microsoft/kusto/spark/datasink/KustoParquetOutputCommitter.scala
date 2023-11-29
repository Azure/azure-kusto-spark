package com.microsoft.kusto.spark.datasink

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetOutputCommitter
import org.apache.parquet.Log

class KustoParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext) extends ParquetOutputCommitter(outputPath, context) {
  val LOG = new Log(classOf[ParquetOutputCommitter])
  override def commitJob(jobContext: JobContext): Unit = {
    super.commitJob(jobContext)
    LOG.info("-------------------Start Using KustoParquetOutputCommitter to commit parquet files-------------")
    LOG.info(s"Path written was output path: ${outputPath.toString}")
    LOG.info("-------------------End KustoParquetOutputCommitter to commit parquet files-------------")
  }
}

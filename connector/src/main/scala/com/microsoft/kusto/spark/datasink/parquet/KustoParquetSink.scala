package com.microsoft.kusto.spark.datasink.parquet

import org.apache.spark.internal.io._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

class KustoParquetSink(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: org.apache.spark.sql.DataFrame): Unit = {
    val original = data.queryExecution.logical
    println(data.queryExecution)

    println(
      s"""
         | Plan: ${original.getClass}
         | Output: ${original.output.toList}
         | Expressions: ${original.expressions.toList}
    """.stripMargin)

    val modified = {
      val timezone = DateTimeUtils.defaultTimeZone().getID
      val exprs = original.output.map {
        case date if date.dataType == DateType =>
          Alias(Cast(date, TimestampType), date.name)()
        case ts if ts.dataType == TimestampType =>
          Alias(FromUTCTimestamp(ts, Literal(timezone)), ts.name)()
        case other => other
      }
      CollapseProject.apply(Project(exprs, original))
    }

    // Update query execution by resetting the fields
    val qe = data.queryExecution
    updateQueryExecution(qe, modified)

    qe.analyzed.foreachUp {
      case p if p.isStreaming =>
        println(s"[modified] => Streaming source: $p")
        null
      case _ =>
    }

    println(s"[modified] isStreaming: ${qe.analyzed.isStreaming}")
    println(s"[modified] class: ${qe.getClass}")

    println(qe.analyzed.schema)
    println(qe)

    val hadoopConf = data.sparkSession.sessionState.newHadoopConf()

    val committer = FileCommitProtocol.instantiate(
      className = classOf[KustoParquetCommitProtocol].getCanonicalName,
      jobId = batchId.toString,
      outputPath = parameters("path"))

    val filesWritten = FileFormatWriter.write(
      sparkSession = data.sparkSession,
      plan = qe.executedPlan,
      fileFormat = new KustoParquetFormat(),
      committer = committer,
      outputSpec = FileFormatWriter.OutputSpec(parameters("path"), Map.empty, qe.analyzed.output),
      hadoopConf = hadoopConf,
      partitionColumns = Nil,
      bucketSpec = None,
      statsTrackers = Nil,
      options = parameters
    )

    filesWritten.foreach(fileName => println(s"========>$fileName"))

  }

  def updateQueryExecution(qe: QueryExecution, modified: LogicalPlan): Unit = {
    resetLazy(qe, qe.getClass)
    val parentClass = qe.getClass.getSuperclass
    resetLazy(qe, parentClass)
    // update plan
    val planField = parentClass.getDeclaredField("logical")
    planField.setAccessible(true)
    planField.set(qe, modified)
  }

  def resetLazy(obj: Any, clazz: Class[_]): Unit = {
    val bitmap = clazz.getDeclaredField("bitmap$0")
    bitmap.setAccessible(true)
    if (bitmap.getType == classOf[Boolean]) {
      bitmap.setBoolean(obj, false)
    } else if (bitmap.getType == classOf[Byte]) {
      bitmap.setByte(obj, 0)
    } else if (bitmap.getType == classOf[Short]) {
      bitmap.setShort(obj, 0)
    } else {
      // assume it is integer
      bitmap.setInt(obj, 0)
    }
  }
}

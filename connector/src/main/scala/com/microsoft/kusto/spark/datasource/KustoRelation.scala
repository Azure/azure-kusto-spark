//Copyright (c) Microsoft Corporation and contributors. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.microsoft.kusto.spark.datasource

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.{KustoCoordinates, KustoDebugOptions}
import com.microsoft.kusto.spark.datasink.{KustoWriter, WriteOptions}
import com.microsoft.kusto.spark.utils.{
  ExtendedKustoClient,
  KustoClientCache,
  KustoConstants,
  KustoQueryUtils,
  KustoDataSourceUtils => KDSU
}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import java.security.InvalidParameterException
import java.util.Locale
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

private[kusto] case class KustoRelation(
    kustoCoordinates: KustoCoordinates,
    authentication: KustoAuthentication,
    query: String,
    readOptions: KustoReadOptions,
    timeout: FiniteDuration,
    customSchema: Option[String] = None,
    maybeStorageParameters: Option[TransientStorageParameters],
    clientRequestProperties: Option[ClientRequestProperties],
    requestId: String)(@transient val sparkSession: SparkSession)
    extends BaseRelation
    with PrunedFilteredScan
    with Serializable
    with InsertableRelation {

  private val normalizedQuery = KustoQueryUtils.normalizeQuery(query)
  var cachedSchema: KustoSchema = _

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    if (cachedSchema == null) {
      cachedSchema = if (customSchema.isDefined) {
        val schema = StructType.fromDDL(customSchema.get)
        KustoSchema(schema, Set())
      } else {
        getSchema
      }
    }
    cachedSchema.sparkSchema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val kustoClient = KustoClientCache.getClient(
      kustoCoordinates.clusterUrl,
      authentication,
      kustoCoordinates.ingestionUrl,
      kustoCoordinates.clusterAlias)
    val isUserOptionForceSingleMode = readOptions.readMode.contains(ReadMode.ForceSingleMode)
    val (useSingleMode, estimatedRecordCount) = readOptions.readMode match {
      // if the user provides a specific option , this is to be used no matter what
      case Some(_) => (isUserOptionForceSingleMode, -1)
      // If there is no option mentioned , then estimate which option to use
      // Count records and see if we wat a distributed or single mode
      case None =>
        val estimatedRecordCountResult = Try(
          KDSU.estimateRowsCount(
            kustoClient.engineClient,
            query,
            kustoCoordinates.database,
            clientRequestProperties.orNull))
        /*
        Return values of estimate method
        - Non zero integer : In case estimate method returns a value , estimated result
        - Zero : Estimate fails and falls back to count , this gives a 0 result
        - An exception : Happens when estimate is a null/empty and we fallback to count and count fails as well (timeout)
         */
        estimatedRecordCountResult match {
          // if the count is lss than the 5k threshold,use single mode.
          case Success(recordCount) =>
            (recordCount <= KustoConstants.DirectQueryUpperBoundRows, recordCount)
          // A case of query timing out. ForceDistributedMode will be used here
          case Failure(_) => (false, -1)
        }
    }
    // To avoid all the complexity and logically split , extract this to a separate API
    buildScanImpl(
      requiredColumns,
      filters,
      kustoClient,
      isUserOptionForceSingleMode,
      useSingleMode,
      estimatedRecordCount)
  }

  private def buildScanImpl(
      requiredColumns: Array[String],
      filters: Array[Filter],
      kustoClient: ExtendedKustoClient,
      isUserOptionForceSingleMode: Boolean,
      useSingleMode: Boolean,
      estimatedRecordCount: Int) = {
    // Is a 0 only if both estimate and count return 0 in which case it is an empty RDD of rows
    if (estimatedRecordCount == 0) {
      sparkSession.emptyDataFrame.rdd
    } else {
      // Either a case of non-zero records or a case of timed-out.
      if (useSingleMode) {
        // There are less than 5000 (KustoConstants.DirectQueryUpperBoundRows) records, perform a single scan
        val singleBuildScanResult = Try(
          KustoReader.singleBuildScan(
            kustoClient,
            KustoReadRequest(
              sparkSession,
              cachedSchema,
              kustoCoordinates,
              query,
              authentication,
              timeout,
              clientRequestProperties,
              requestId),
            KustoFiltering(requiredColumns, filters)))
        // see if the scan succeeded
        singleBuildScanResult match {
          case Success(rdd) => rdd
          case Failure(exception) =>
            // If the user specified forceSingleMode explicitly and that cannot be honored , throw an exception back
            // Only check is if exception is because of QueryLimits , it will fallback
            val isRowLimitHit = ExceptionUtils
              .getRootCauseStackTrace(exception)
              .contains("Query execution has exceeded the allowed limits")
            if (isUserOptionForceSingleMode && !isRowLimitHit) {
              // Expected behavior for Issue#261
              throw exception
            } else {
              // The case where used did not provide an option and we estimated to be a single scan.
              // Our approximate estimate failed here , fallback to distributed
              KDSU.reportExceptionAndThrow(
                "KustoRelation",
                exception,
                "Failed with Single mode, falling back to Distributed mode,",
                kustoCoordinates.clusterAlias,
                kustoCoordinates.database,
                requestId = requestId,
                shouldNotThrow = true)
              readOptions.partitionOptions.column = Some(getPartitioningColumn)
              readOptions.partitionOptions.mode = Some(getPartitioningMode)
              KustoReader.distributedBuildScan(
                kustoClient,
                KustoReadRequest(
                  sparkSession,
                  cachedSchema,
                  kustoCoordinates,
                  query,
                  authentication,
                  timeout,
                  clientRequestProperties,
                  requestId),
                maybeStorageParameters.getOrElse(kustoClient.getTempBlobsForExport),
                readOptions,
                KustoFiltering(requiredColumns, filters))
            }
        }
      } else {
        // Determined to be distributed mode , through user property or by record count
        readOptions.partitionOptions.column = Some(getPartitioningColumn)
        readOptions.partitionOptions.mode = Some(getPartitioningMode)
        KustoReader.distributedBuildScan(
          kustoClient,
          KustoReadRequest(
            sparkSession,
            cachedSchema,
            kustoCoordinates,
            query,
            authentication,
            timeout,
            clientRequestProperties,
            requestId),
          maybeStorageParameters.getOrElse(kustoClient.getTempBlobsForExport),
          readOptions,
          KustoFiltering(requiredColumns, filters))
      }
    }
  }

  private def getSchema: KustoSchema = {
    if (query.isEmpty) {
      throw new InvalidParameterException("Query is empty")
    }
    val getSchemaQuery =
      if (KustoQueryUtils.isQuery(query)) KustoQueryUtils.getQuerySchemaQuery(normalizedQuery)
      else ""
    if (getSchemaQuery.isEmpty) {
      throw new RuntimeException(
        "Spark connector cannot run Kusto commands. Please provide a valid query")
    }
    KDSU.getSchema(
      kustoCoordinates.database,
      getSchemaQuery,
      KustoClientCache.getClient(
        kustoCoordinates.clusterUrl,
        authentication,
        kustoCoordinates.ingestionUrl,
        kustoCoordinates.clusterAlias),
      clientRequestProperties)
  }

  private def getPartitioningColumn: String = {
    readOptions.partitionOptions.column match {
      case Some(requestedColumn) =>
        if (!schema.fields.exists(p => p.name equals requestedColumn)) {
          throw new InvalidParameterException(
            s"Cannot partition by column '$requestedColumn' since it is not part of the query schema: ${KDSU.NewLine}${schema
                .mkString(", ")}")
        }
        requestedColumn
      case None => schema.head.name
    }
  }

  private def getPartitioningMode: String = {
    readOptions.partitionOptions.mode match {
      case Some(mode) =>
        val normalizedMode = mode.toLowerCase(Locale.ROOT)
        if (!KustoDebugOptions.supportedPartitioningModes.contains(normalizedMode)) {
          throw new InvalidParameterException(
            s"Specified partitioning mode '$mode' : ${KDSU.NewLine}${KustoDebugOptions.supportedPartitioningModes
                .mkString(", ")}")
        }
        normalizedMode
      case None => "hash"
    }
  }

  // Used for cached results
  override def equals(other: Any): Boolean = other match {
    case that: KustoRelation =>
      kustoCoordinates == that.kustoCoordinates && query == that.query && authentication == that.authentication
    case _ => false
  }

  override def hashCode(): Int =
    kustoCoordinates.hashCode() ^ query.hashCode ^ authentication.hashCode()

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    KustoWriter.write(
      None,
      data,
      kustoCoordinates,
      authentication,
      writeOptions = WriteOptions.apply(),
      clientRequestProperties.get)
  }
}

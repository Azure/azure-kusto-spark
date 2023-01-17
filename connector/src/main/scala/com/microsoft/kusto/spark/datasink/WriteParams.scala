package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.ExtendedKustoClient

final case class WriteParams(tableCoordinates: KustoCoordinates,
                             authentication: KustoAuthentication,
                             writeOptions: WriteOptions,
                             crp: ClientRequestProperties,
                             batchIdIfExists: String,
                             tmpTableName: String,
                             tableExists: Boolean)
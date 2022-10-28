package com.microsoft.kusto.spark.datasink

import com.microsoft.azure.kusto.data.ClientRequestProperties
import com.microsoft.kusto.spark.authentication.KustoAuthentication
import com.microsoft.kusto.spark.common.KustoCoordinates
import com.microsoft.kusto.spark.utils.ExtendedKustoClient

final case class TransactionWriteParams(tableCoordinates: KustoCoordinates,
                                        authentication: KustoAuthentication,
                                        writeOptions: WriteOptions,
                                        crp: ClientRequestProperties,
                                        batchIdIfExists: String,
                                        kustoClient: ExtendedKustoClient,
                                        tmpTableName: String,
                                        tableExists: Boolean)

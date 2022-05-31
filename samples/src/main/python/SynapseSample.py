# Optional:
sc._jvm.com.microsoft.kusto.spark.utils.KustoDataSourceUtils.setLoggingLevel("all") 

# Azure Synapse is a limitless analytics service that brings together enterprise data warehousing. 
# The synapse-kusto connector encapsulates the kusto-spark connector. The code resides in a private repository and is integrated in azure synapse.
# Read more about this integration https://docs.microsoft.com/azure/synapse-analytics/quickstart-connect-azure-data-explorer

#######################
# BATCH READ EXAMPLE  #
#######################

# Read data from Azure Data Explorer table(s)
# In this example the Kusto Spark connector will determine the optimal path to get data: API for small data sets, Export/Distributed mode for large datasets.

kustoDf  = spark.read \
            .format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("spark.synapse.linkedService", "<link service name>") \
            .option("kustoDatabase", "<Database name>") \
            #.option("authType", "LS") \ # If this is added, it uses the link service credentials. Otherwise, the default is native - using the logged-in user's credentials or the Synapse Workspace managed identity for automated execution.
            .option("kustoQuery", "<KQL Query>") \
            .load()

display(kustoDf)    


#################################################
# BATCH READ EXAMPLE WITH FORCED DISTRIBUTION #
#################################################

# Read the data from the kusto table in forced 'distributed' mode and with advanced options
# Please refer to https://github.com/Azure/azure-kusto-spark/blob/master/connector/src/main/scala/com/microsoft/kusto/spark/datasource/KustoSourceOptions.scala
# to get the string representation of the options - as pyspark does not support calling properties of scala objects.

crp = sc._jvm.com.microsoft.azure.kusto.data.ClientRequestProperties()
crp.setOption("norequesttimeout",True)
crp.toString()

kustoDf  = spark.read \
            .format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("spark.synapse.linkedService", "<link service name>") \
            .option("kustoDatabase", "<Database name>") \
            .option("kustoQuery", "<KQL Query>") \
            #.option("authType", "LS") \ # If this is added, it uses the link service credentials. Otherwise, the default is native - using the logged-in user's credentials or the Synapse Workspace managed identity for automated execution.
            .option("clientRequestPropertiesJson", crp.toString()) \
            .option("readMode", 'ForceDistributedMode') \
            .load()

display(kustoDf)    


#######################
# BATCH WRITE EXAMPLE #
#######################

# Write data to an Azure Data Explorer table

df.write \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("spark.synapse.linkedService", "<link service name>") \
    .option("kustoDatabase", "<Database name>") \
    .option("kustoTable", "<Table name>") \
    #.option("authType", "LS") \ # If this is added, it uses the link service credentials. Otherwise, the default is native - using the logged-in user's credentials or the Synapse Workspace managed identity for automated execution.
    .mode("Append") \
    .save()


#######################################################
# BATCH WRITE EXAMPLE WITH ADVANCED INGESTION OPTIONS #
#######################################################

# Please refer to https://github.com/Azure/azure-kusto-spark/blob/master/connector/src/main/scala/com/microsoft/kusto/spark/datasink/KustoSinkOptions.scala
# to get the string representation of the options you need

extentsCreationTime = sc._jvm.org.joda.time.DateTime.now().plusDays(1)
csvMap = "[{\"Name\":\"ColA\",\"Ordinal\":0},{\"Name\":\"ColB\",\"Ordinal\":1}]"
# Alternatively use an existing csv mapping configured on the table and pass it as the last parameter of SparkIngestionProperties or use none


sp = sc._jvm.com.microsoft.kusto.spark.datasink.SparkIngestionProperties(
        False, ["dropByTags"], ["ingestByTags"], ["tags"], ["ingestIfNotExistsTags"], extentsCreationTime, csvMap, None)
# Class fields: SparkIngestionProperties(flushImmediately: Boolean,
#                                        dropByTags: util.ArrayList[String],
#                                        ingestByTags: util.ArrayList[String],
#                                        additionalTags: util.ArrayList[String],
#                                        ingestIfNotExists: util.ArrayList[String],
#                                        creationTime: DateTime,
#                                        csvMapping: String,
#                                        csvMappingNameReference: String)
#
# More info on Ingestion Properties: https://docs.microsoft.com/azure/data-explorer/ingestion-properties


df.write \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("spark.synapse.linkedService", "<link service name>") \
    .option("kustoDatabase", "<Database name>") \
    .option("kustoTable", "<Table name>") \
    #.option("authType", "LS") \ # If this is added, it uses the link service credentials. Otherwise, the default is native - using the logged-in user's credentials or the Synapse Workspace managed identity for automated execution.
     .option("sparkIngestionPropertiesJson", sp.toString()) \
    .option("tableCreateOptions","CreateIfNotExist") \
    .mode("Append") \
    .save()



############################################################
# STREAMING SPARK DATAFRAME TO A AZURE DATA EXPLORER TABLE #
############################################################

# Write a streaming Spark DataFrame to a Azure Data Explorer table


dfStream = spark \
  .readStream \
  .schema([customSchema]) \
  .csv([filename]) \

spark.conf.set("spark.sql.streaming.checkpointLocation", "/localWriteCheckpointFolder")
spark.conf.set("spark.sql.codegen.wholeStage", "false")

kustoQ = dfStream \
    .writeStream \
    .format("com.microsoft.kusto.spark.synapse.datasink.KustoSynapseSinkProvider") \
    .option("spark.synapse.linkedService", "<link service name>") \
    .option("kustoDatabase", "<Database name>") \
    .option("kustoTable", "<Table name>") \
    .trigger(once = True)
    
kustoQ.start().awaitTermination(60*8)

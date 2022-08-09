from pyspark.sql import SparkSession

# COMMAND ----------

# Optional:
sc._jvm.com.microsoft.kusto.spark.utils.KustoDataSourceUtils.setLoggingLevel("all")
# COMMAND ----------

pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
kustoOptions = {"kustoCluster":"<cluster-name>", "kustoDatabase" : "<database-name>", "kustoTable" : "<table-name>", "kustoAadAppId":"<AAD-app id>" ,
 "kustoAadAppSecret":"<AAD-app key>", "kustoAadAuthorityID":"<AAD authentication authority>"} # This can replace the above distributed mode options
# Create a DataFrame for ingestion
df = spark.createDataFrame([("row-"+str(i),i)for i in range(1000)],["name", "value"])

# COMMAND ----------

#######################
# BATCH SINK EXAMPLE  #
#######################

# Write data to a Kusto table
df.write. \
  format("com.microsoft.kusto.spark.datasource"). \
  option("kustoCluster",kustoOptions["kustoCluster"]). \
  option("kustoDatabase",kustoOptions["kustoDatabase"]). \
  option("kustoTable", kustoOptions["kustoTable"]). \
  option("kustoAadAppId",kustoOptions["kustoAadAppId"]). \
  option("kustoAadAppSecret",kustoOptions["kustoAadAppSecret"]). \
  option("kustoAadAuthorityID",kustoOptions["kustoAadAuthorityID"]). \
  mode("Append"). \
  save()

# COMMAND ----------

# Read the data from the kusto table with default reading mode
kustoDf  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
            option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
            option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
            load()

# Read the data from the kusto table in forced 'distributed' mode and with advanced options
# Please refer to https://github.com/Azure/azure-kusto-spark/blob/master/connector/src/main/scala/com/microsoft/kusto/spark/datasource/KustoSourceOptions.scala
# to get the string representation of the options - as pyspark does not support calling properties of scala objects.
# ClientRequestProperties are used in every command executed on the service (schema inference, export command or query. in older versions it is used just the for the export command)

crp = sc._jvm.com.microsoft.azure.kusto.data.ClientRequestProperties()
crp.setOption("norequesttimeout",True)
crp.toString()

# Use customSchema to enforce the schema and remove the initial command to the service for schema inference. 
# The provided Schema must be a subset of the query result schema.
ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(customSchema.json()).toDDL()

kustoDf  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
            option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
            option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
            option("clientRequestPropertiesJson", crp.toString()). \
            option("readMode", 'ForceDistributedMode'). \
            option("customSchema", ddl). \
            load()


kustoDf.show()

# COMMAND ----------

# Writing with advanced options
# Please refer to https://github.com/Azure/azure-kusto-spark/blob/master/connector/src/main/scala/com/microsoft/kusto/spark/datasink/KustoSinkOptions.scala
# to get the string representation of the options you need
extentsCreationTime = sc._jvm.org.joda.time.DateTime.now().minusDays(1)
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

df.write. \
  format("com.microsoft.kusto.spark.datasource"). \
  option("kustoCluster",kustoOptions["kustoCluster"]). \
  option("kustoDatabase",kustoOptions["kustoDatabase"]). \
  option("kustoTable", kustoOptions["kustoTable"]). \
  option("kustoAadAppId",kustoOptions["kustoAadAppId"]). \
  option("kustoAadAppSecret",kustoOptions["kustoAadAppSecret"]). \
  option("kustoAadAuthorityID",kustoOptions["kustoAadAuthorityID"]). \
  option("sparkIngestionPropertiesJson", sp.toString()). \
  option("tableCreateOptions","CreateIfNotExist"). \
  mode("Append"). \
  save()

# COMMAND ----------


##########################
# STREAMING SINK EXAMPLE #
##########################

filename = "file:///dbfs/csvData/"

from pyspark.sql.types import *

customSchema = StructType([
    StructField("colA", StringType(), True),
    StructField("colB", IntegerType(), True)
])

csvDf = spark \
  .readStream \
  .schema(customSchema) \
  .csv(filename) \

# COMMAND ----------

spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/temp/checkpoint")

# Write to a Kusto table from a streaming source
kustoQ = csvDf.writeStream. \
  format("com.microsoft.kusto.spark.datasink.KustoSinkProvider"). \
  option("kustoCluster",kustoOptions["kustoCluster"]). \
  option("kustoDatabase",kustoOptions["kustoDatabase"]). \
  option("kustoTable", kustoOptions["kustoTable"]). \
  option("kustoAadAppId",kustoOptions["kustoAadAppId"]). \
  option("kustoAadAppSecret",kustoOptions["kustoAadAppSecret"]). \
  option("kustoAadAuthorityID",kustoOptions["kustoAadAuthorityID"]). \
  trigger(once = True)

kustoQ.start().awaitTermination(60*8)

#########################
# Device Authentication #
#########################

# Device authentication for databricks (Scala users can just discard any authentication parameters and get the same result)
# Acquire a token with device authentication and pass the token to the connector, this token will expire in one hour but
# it should be enough for reading as the call to the service is done at the start of the flow. Write commands should be done
# in an hour.
# Prints done inside the JVM are not shown in the notebooks, therefore the user has to print himself the device code.

deviceAuth = sc._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(
               "https://{clusterAlias}.kusto.windows.net".format(clusterAlias=kustoOptions["kustoCluster"]),
               kustoOptions["kustoAadAuthorityID"])
deviceCodeMessage = deviceAuth.getDeviceCodeMessage()
print(deviceCodeMessage)
token = deviceAuth.acquireToken()

df  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("accessToken", token). \
            load()

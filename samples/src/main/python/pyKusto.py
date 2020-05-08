from pyspark.sql import SparkSession

# COMMAND ----------

# Optional:
sc._jvm.com.microsoft.kusto.spark.utils.KustoDataSourceUtils.setLoggingLevel("all")
# COMMAND ----------

pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
kustoOptions = {"kustoCluster":"<cluster-name>", "kustoDatabase" : "<database-name>", "kustoTable" : "<table-name>", "kustoAADClientID":"<AAD-app id>" ,
 "kustoClientAADClientPassword":"<AAD-app key>", "kustoAADAuthorityID":"<AAD authentication authority>",
 "blobStorageAccountName":"<Storage-Account-Name>","blobStorageAccountKey":"<Storage-Account-Key>", "blobContainer":"<Container-Name>", # For distributed read
 "blobStorageSasUrl":"<blob-Storage-Full-Sas-Url>"} # This can replace the above distributed mode options
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
  option("kustoAADClientID",kustoOptions["kustoAADClientID"]). \
  option("kustoClientAADClientPassword",kustoOptions["kustoClientAADClientPassword"]). \
  option("kustoAADAuthorityID",kustoOptions["kustoAADAuthorityID"]). \
  mode("Append"). \
  save()

# COMMAND ----------

# Read the data from the kusto table with default reading mode
kustoDf  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("kustoAADClientID", kustoOptions["kustoAADClientID"]). \
            option("kustoClientAADClientPassword", kustoOptions["kustoClientAADClientPassword"]). \
            option("kustoAADAuthorityID", kustoOptions["kustoAADAuthorityID"]). \
            load()

# Read the data from the kusto table in forced 'distributed' mode and with advanced options
# Please refer to https://github.com/Azure/azure-kusto-spark/blob/master/connector/src/main/scala/com/microsoft/kusto/spark/datasource/KustoSourceOptions.scala
# to get the string representation of the options you need as pyspark does not support usage of scala objects.

crp = sc._jvm.com.microsoft.azure.kusto.data.ClientRequestProperties()
crp.setOption("norequesttimeout",True)
crp.toString()

kustoDf  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("kustoAADClientID", kustoOptions["kustoAADClientID"]). \
            option("kustoClientAADClientPassword", kustoOptions["kustoClientAADClientPassword"]). \
            option("kustoAADAuthorityID", kustoOptions["kustoAADAuthorityID"]). \
            option("clientRequestPropertiesJson", crp.toString()). \
            option("readMode", 'ForceDistributedMode'). \
            load()

kustoDf  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("kustoAADClientID", kustoOptions["kustoAADClientID"]). \
            option("kustoClientAADClientPassword", kustoOptions["kustoClientAADClientPassword"]). \
            option("kustoAADAuthorityID", kustoOptions["kustoAADAuthorityID"]). \
            option("blobStorageAccountName",kustoOptions["blobStorageAccountName"]). \
            option("blobStorageAccountKey",kustoOptions["blobStorageAccountKey"]). \
            option("blobContainer",kustoOptions["blobContainer"]). \
            option("blobStorageSasUrl",kustoOptions["blobStorageSasUrl"]). \ # Second option for scaling
            option("clientRequestPropertiesJson", crp.toString()). \
            load()


kustoDf.show()

# COMMAND ----------

# Writing with advanced options
# Please refer to https://github.com/Azure/azure-kusto-spark/blob/master/connector/src/main/scala/com/microsoft/kusto/spark/datasink/KustoSinkOptions.scala
# to get the string representation of the options you need
time = sc._jvm.org.joda.time.DateTime.now().minusDays(1)
csvMap = "[{\"Name\":\"ColA\",\"Ordinal\":0},{\"Name\":\"ColB\",\"Ordinal\":1}]"

sp = sc._jvm.com.microsoft.kusto.spark.datasink.SparkIngestionProperties(False, ["1"], ["2"], ["3"], ["4"], time, csvMap, None)

df.write. \
  format("com.microsoft.kusto.spark.datasource"). \
  option("kustoCluster",kustoOptions["kustoCluster"]). \
  option("kustoDatabase",kustoOptions["kustoDatabase"]). \
  option("kustoTable", kustoOptions["kustoTable"]). \
  option("kustoAADClientID",kustoOptions["kustoAADClientID"]). \
  option("kustoClientAADClientPassword",kustoOptions["kustoClientAADClientPassword"]). \
  option("kustoAADAuthorityID",kustoOptions["kustoAADAuthorityID"]). \
  option("kustoAADAuthorityID",kustoOptions["kustoAADAuthorityID"]). \
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
spark.conf.set("spark.sql.codegen.wholeStage", "false")

# Write to a Kusto table from a streaming source
kustoQ = csvDf.writeStream. \
  format("com.microsoft.kusto.spark.datasink.KustoSinkProvider"). \
  option("kustoCluster",kustoOptions["kustoCluster"]). \
  option("kustoDatabase",kustoOptions["kustoDatabase"]). \
  option("kustoTable", kustoOptions["kustoTable"]). \
  option("kustoAADClientID",kustoOptions["kustoAADClientID"]). \
  option("kustoClientAADClientPassword",kustoOptions["kustoClientAADClientPassword"]). \
  option("kustoAADAuthorityID",kustoOptions["kustoAADAuthorityID"]). \
  trigger(once = True)

kustoQ.start().awaitTermination(60*8)

#########################
# Device Authentication #
#########################

# Device authentication for databricks
# Acquire a token with device authentication and pass the token to the connector
# Prints done inside the JVM are not shown in the notebooks, therefore the user has to print himself the device code.

deviceAuth = sc._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(
               "https://{clusterAlias}.kusto.windows.net".format(clusterAlias=kustoOptions["kustoCluster"]),
               "common")
try:
  deviceCodeMessage = deviceAuth.getDeviceCodeMessage()
  print(deviceCodeMessage)
except Exception as e:
  print(e)
  deviceAuth.refreshDeviceCode() # Every 15 minutes a device code should be reacquired
token = deviceAuth.acquireToken()

df  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster", kustoOptions["kustoCluster"]). \
            option("kustoDatabase", kustoOptions["kustoDatabase"]). \
            option("kustoQuery", kustoOptions["kustoTable"]). \
            option("accessToken", token). \
            load()

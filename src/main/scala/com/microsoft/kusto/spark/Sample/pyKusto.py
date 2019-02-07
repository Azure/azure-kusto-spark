from pyspark.sql import SparkSession

# COMMAND ----------

# Optional:
sc._jvm.com.microsoft.kusto.spark.utils.KustoDataSourceUtils.setLoggingLevel("all")
# COMMAND ----------

pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
kustoOptions = {"kustoCluster":"<cluster-name>", "kustoDatabase" : "<database-name>", "kustoTable" : "<table-name>", "kustoAADClientID":"<AAD-app id>" ,"kustoClientAADClientPassword":"<AAD-app key>", "kustoAADAuthorityID":"<AAD authentication authority>"}
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
  save()

# COMMAND ----------

# Read the data from the kusto table
kustoDf  = pyKusto.read. \
            format("com.microsoft.kusto.spark.datasource"). \
            option("kustoCluster",kustoOptions["kustoCluster"]). \
            option("kustoDatabase",kustoOptions["kustoDatabase"]). \
            option("kustoTable", kustoOptions["kustoTable"]). \
            option("kustoAADClientID",kustoOptions["kustoAADClientID"]). \
            option("kustoClientAADClientPassword",kustoOptions["kustoClientAADClientPassword"]). \
            option("kustoAADAuthorityID",kustoOptions["kustoAADAuthorityID"]). \
            load()

kustoDf.show()

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
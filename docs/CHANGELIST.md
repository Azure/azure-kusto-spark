# Version 1.0.0-Beta-01 (baseline):
- Added a data sink connector, supporting standard 'write' operations ('writeStream' interface is also supported)
- Added a basic data source connector, supporting standard 'read' operations
  Note: in this version, a single partition is forced

# Version 1.0.0-Beta-01 update 1:
- Fixed DataTypes support, including DateTime and decimal
- Fixed streaming sink when working with multiple batches. Handle empty batches
- Added 'KUSTO_WRITE_RESULT_LIMIT' option. When writing to Kusto, limits the number of rows read back as BaseRelation
- Adjusted to Spark 2.4. This is optimized for Azure DataBricks default. In order to use with Spark 2.3, pom.xml
  file must be adjusted: spark.version (to 2.3.x) and json4s-jackson_2.11 (to 3.2.11)
- KustoOptions.KUSTO_TABLE is no longer used in reading using kusto source

# 1.0.0-Beta-02:

### Read Connector 
* Added 'scale' reading mode to allow reading large data sets. This is the default mode 
for reading from Kusto, and it requires the user to provide transient blob storage
  > NOTE: this is an interface-breaking change. Reading small data sets directly (as in previous version) is also supported,
  but it must be explicitly specified by setting 'KUSTO_READ_MODE' option to 'lean'
* Added column pruning and filter push-down support when reading from Kusto

  For details, refer to [KustoSource.md](KustoSource.md) document

### Write Connector
* Support writing large data sets. Partitions that exceed Kusto ingest policy guidelines are split into several smaller
  ingestion operations
  
  For details, refer to [KustoSink.md](KustoSink.md) document
  
### Authentication
* Support Key-vault based authentication, when authentication parameters are stored in KeyVault

  For details, refer to [Authentication.md](Authentication.md) document

### Examples
* Added Python sample code for reference: [pyKusto.py](../samples/src/main/scala/pyKusto.py)
* Updated existing references. 
  In particular, the reference based on Databricks notebook: [KustoConnectorDemo](../samples/src/main/scala/KustoConnectorDemo.scala)

### Packaging
* Organized samples and connector as separate modules to reduce dependencies

# Known Issues and Tips
1. When running with spark 'wholestage codegen' enabled, a mismatch between schema 'nullable' definition and actual data containing
null values can lead to a NullPointerException to be thrown by org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter.
If you encounter this error and cannot identify and fix the mismatch, consider disabling 'wholestage codegen' by setting (databricks):

   `spark.conf.set("spark.sql.codegen.wholeStage","false")`

2. When writing to Kusto, [entity naming rules](https://docs.microsoft.com/en-us/azure/kusto/query/schema-entities/entity-names)
  must be followed to avoid collisions with Kusto reserved keywords.
  For details, refer to [these](https://docs.microsoft.com/en-us/azure/kusto/query/schema-entities/entity-names#naming-your-entities-to-avoid-collisions-with-kusto-language-keywords)
  guidelines.   

# 1.0.0-Beta-03:
* Add heuristics to automatically decide whether to use lean (direct query) 
or scale (via transient blob storage) read mode.
'KUSTO_READ_MODE' option is no longer supported (there is a debug option for testing purposes)
* Remove dependency on Jackson-core library
* Deprecate 'KUSTO_BLOB_SET_FS_CONFIG' option. Instead, cache relevant configuration and set if needed
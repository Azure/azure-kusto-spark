# Spark-Kusto DataTypes mapping

When writing to or reading from a Kusto table, the connector converts types from the original DataFrame type to Kusto type
, and vice versa. Below is the mappings of these conversions.

#####Spark DataTypes mapping to Kusto type

|Spark data type | Kusto data type
|---|---
|StringType |string
|IntegerType |int
|LongType |long
|BooleanType |bool
|ShortType |int
|DoubleType |real
|ByteType |int
|FloatType |real
|DecimalType |decimal
|TimestampType |datetime
|DateType |datetime
|StructType |dynamic
|MapType |dynamic
|ArrayType |dynamic

#####Kusto DataTypes mapping to Spark DataTypes

|Kusto data type |Spark data type  
|---|---
|string |String 
|int |IntegerType
|long |LongType 
|bool |BooleanType 
|int |ShortType 
|real |DoubleType
|int |Byte
|real |FloatType 
|decimal |DecimalType 
|datetime |TimestampType
|timespan |StringType
|guid |String
|dynamic |StringType

##Notes


Kusto **datetime** data type is always read in '%Y-%m-%d %H:%M:%s' format , while **timespan** format is '%H:%M:%s'. On the other
hand spark **DateType** is of format '%Y-%m-%d' and **TimestampType** is of format '%Y-%m-%d %H:%M:%s'. This is why Kusto 'timespan' 
type is translated into a string by the connector and **we recommend using only datetime and TimestampType**. 

Kusto **decimal** type corresponds to .Net 'SqlDecimal' type, with a total size of 34 bit for precision and scale params.
    
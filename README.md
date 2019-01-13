<p align="center">
  <img src="kusto_spark.png" alt="Kusto + Apache Spark Connector" width="270"/>
</p>

# Kusto Connector for Apache Spark
 
This library contains the source code for Kusto Data Source and Data Sink Connector for Apache Spark.

Kusto (A.K.A. [Azure Data Explorer](https://azure.microsoft.com/en-us/services/data-explorer/)) is a lightning-fast indexing and querying service. 

[Spark](https://spark.apache.org/) is a unified analytics engine for large-scale data processing.

Making Kusto and Spark work together enables our users to build fast and scalable applications, targeting a variety of Machine Learning, Extract-Transform-Load, Log Analytics and other data driven scenarios. 
 
## About This Release

This is a beta release, serving as the baseline version of Kusto connector for Spark. It exposes Kusto as a valid Data Store 
for standard Spark source and sink operations such as write, read and writeStream.
It can be used for experimentation, and for applications where the size of data read from Kusto is fairly small.

Future releases will extend Kusto connector capabilities in the following areas:

* Allow large-scale data transport to and from Kusto
* Add capabilities: will be specified in the version [CHANGELIST](docs/CHANGELIST) 
* Improve fault tolerance and resilience 

## Usage

### Linking 

For Scala/Java applications using Maven project definitions, link your application with the artifact below. 

```
groupId = com.microsoft.azure
artifactId = spark-kusto-connector
version = 1.0.0-Beta-01 
```

**In Maven**:
 ```
   <dependency>
     <groupId>com.microsoft.azure</groupId>
     <artifactId>spark-kusto-connector</artifactId>
     <version>1.0.0-Beta-01</version>
   </dependency>
```

## Build Prerequisites

In order to use the connector, you need to have:

- Java 1.8 SDK installed
- [Maven 3.x](https://maven.apache.org/download.cgi) installed
- Spark version 2.3.2 or higher 

## Build Commands
   
```
// Builds jar and runs all tests
mvn clean package

// Builds jar, runs all tests, and installs jar to your local maven repository
mvn clean install
```

## Pre-Compiled Libraries
In order to facilitate ramp-up on platforms such as Azure Databricks, pre-compiled libraries
can be found [here](lib) 

	
## Documentation
Detailed documentation can be found [here](docs).
Usage examples can be found [here](src/main/scala/com/microsoft/kusto/spark/Sample)
	
## Contributing 

Contributions and suggestions to this product will be possible, and most welcome, 
starting with next release
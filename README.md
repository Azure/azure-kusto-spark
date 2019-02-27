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
- Spark version 2.4.0 or higher

    >**Note**: 2.3.x versions are also supported, but require some changes in pom.xml dependencies. 
      For details, refer to [CHANGELIST](docs/CHANGELIST)

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

	
# Documentation

Detailed documentation can be found [here](docs).

# Samples

Usage examples can be found [here](samples/src/main/scala)

# Looking for SDKs for other languages/platforms?

- [Node](https://github.com/azure/azure-kusto-node)
- [Python](https://github.com/azure/azure-kusto-python)
- [.NET](https://docs.microsoft.com/en-us/azure/kusto/api/netfx/about-the-sdk)
- [Java](https://github.com/azure/azure-kusto-java)

# Need Support?

- **Have a feature request for SDKs?** Please post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://docs.microsoft.com/en-us/azure/azure-supportability/how-to-create-azure-support-request) with guaranteed response time.  Consider submitting a ticket and get assistance from Microsoft support team
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Azure/azure-kusto-spark/issues/new).

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.it

import com.microsoft.kusto.spark.KustoTestUtils.getSystemTestOptions
import com.microsoft.kusto.spark.datasink.KustoSinkOptions
import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.utility.{DockerImageName, MountableFile}

import java.util

class E2ETestWithContainers extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val network = Network.newNetwork
  private val sparkContainer: GenericContainer[?] =
    new GenericContainer(DockerImageName.parse("bitnami/spark:3.1.2")).withNetwork(network)
  private lazy val kustoTestConnectionOptions = getSystemTestOptions
  private val targetFolderName = "/opt/bitnami/spark/examples/jars/"
  private val jarFileBaseName = s"kusto-spark_3.0_2.12-${KustoDataSourceUtils.Version}"
  private val testJarFileToSubmit = s"$jarFileBaseName-tests.jar"
  private val jarFileClasspath = s"$jarFileBaseName.jar"

  override def beforeAll(): Unit = {
    sparkContainer.start()
    val envVars = new util.HashMap[String, String]()
    envVars.put(KustoSinkOptions.KUSTO_CLUSTER, kustoTestConnectionOptions.cluster)
    envVars.put(KustoSinkOptions.KUSTO_ACCESS_TOKEN, kustoTestConnectionOptions.accessToken)
    envVars.put(KustoSinkOptions.KUSTO_DATABASE, kustoTestConnectionOptions.database)
    envVars.put(KustoSinkOptions.KUSTO_AAD_AUTHORITY_ID, kustoTestConnectionOptions.tenantId)
    sparkContainer.withEnv(envVars)
    sparkContainer.copyFileToContainer(
      MountableFile.forHostPath(s"target/$testJarFileToSubmit"),
      s"$targetFolderName$testJarFileToSubmit")
    sparkContainer.copyFileToContainer(
      MountableFile.forHostPath(s"target/$jarFileClasspath"),
      s"$targetFolderName$jarFileClasspath")
  }

  "Submit a spark job to perform an ingest" should "ingest" in {
    val ivyOpts = "\"-Divy.cache.dir=/tmp -Divy.home=/tmp\""
    val commandToExec =
      s"spark-submit --conf spark.driver.extraJavaOptions=$ivyOpts " +
        s"--packages org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6  --jars $targetFolderName$jarFileClasspath  " +
        s"--class com.microsoft.kusto.spark.it.KustoE2EMain $targetFolderName$testJarFileToSubmit " +
        s"${kustoTestConnectionOptions.cluster}  ${kustoTestConnectionOptions.database}   ${kustoTestConnectionOptions.accessToken} > /tmp/results.txt 2>&1"
    val execResult = sparkContainer.execInContainer("/bin/sh", "-c", commandToExec)
    Thread.sleep(30000)
    execResult.getStdout should include("Pi is roughly")
  }

  override def afterAll(): Unit = {
    sparkContainer.stop()
  }
}

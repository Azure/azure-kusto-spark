// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.it

import com.microsoft.kusto.spark.utils.KustoDataSourceUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.{DockerImageName, MountableFile}

class E2ETestWithContainers extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val network = Network.newNetwork
  private val sparkContainer: GenericContainer[?] =
    new GenericContainer(DockerImageName.parse("bitnami/spark:3.1.2")).withNetwork(network)

  private val targetFolderName = "/opt/bitnami/spark/examples/jars/"
  private val jarFileBaseName = s"kusto-spark_3.0_2.12-${KustoDataSourceUtils.Version}"
  private val testJarFileToSubmit = s"$jarFileBaseName-tests.jar";
  private val jarFileClasspath = s"$jarFileBaseName.jar";

  override def beforeAll(): Unit = {
    sparkContainer.start()
    sparkContainer.copyFileToContainer(
      MountableFile.forHostPath(s"target/$testJarFileToSubmit"),
      s"$targetFolderName$testJarFileToSubmit")
    sparkContainer.copyFileToContainer(
      MountableFile.forHostPath(s"target/$jarFileClasspath"),
      s"$targetFolderName$jarFileClasspath")
  }

  "Submit a spark job to perform an ingest" should "ingest" in {
    val commandToExec = s"spark-submit --jars $targetFolderName$jarFileClasspath  " +
      s"--class com.microsoft.kusto.spark.it.KustoE2EMain $targetFolderName$testJarFileToSubmit"
    val execResult = sparkContainer.execInContainer("/bin/sh", "-c", commandToExec)
    Thread.sleep(10000)
    execResult.getStdout should include("Pi is roughly")
  }

  override def afterAll(): Unit = {
    sparkContainer.stop()
  }
}

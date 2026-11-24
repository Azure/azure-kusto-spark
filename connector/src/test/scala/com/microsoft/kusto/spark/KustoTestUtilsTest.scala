// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark

import com.microsoft.azure.kusto.data.{Client, KustoOperationResult}
import com.microsoft.kusto.spark.utils.CslCommandsGenerator.{
  generateDropTablesCommand,
  generateFindCurrentTempTablesCommand
}
import org.mockito.ArgumentMatchers.{anyString, eq => eqTo}
import org.mockito.Mockito.{mock, verify, verifyNoMoreInteractions, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class KustoTestUtilsTest extends AnyFlatSpec with Matchers {
  private val database = "test-database"
  private val prefix = "KustoBatchSinkE2E_Ingest_DMStorage"
  private val firstTable = prefix + "_11111111_1111_1111_1111_111111111111"
  private val secondTable = prefix + "_22222222_2222_2222_2222_222222222222"

  private class Fixture {
    val client: Client = mock(classOf[Client])
    val tables: mutable.Set[String] = mutable.Set(firstTable, secondTable)
    val drops: mutable.ArrayBuffer[Set[String]] = mutable.ArrayBuffer.empty

    when(client.executeQuery(eqTo(database), anyString())).thenAnswer { invocation =>
      val query = invocation.getArgument[String](1)
      val table = query.stripSuffix(" | count")
      require(tables.contains(table), s"Test table was deleted before validation: $table")
      new KustoOperationResult(
        """{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"Count","DataType":"Int64","ColumnType":"long"}],"Rows":[[1]]}]}""",
        "v1")
    }
    when(client.executeMgmt(eqTo(database), anyString())).thenAnswer { invocation =>
      val command = invocation.getArgument[String](1)
      if (command == generateFindCurrentTempTablesCommand(Array(prefix))) {
        val rows = tables.toSeq.sorted.map(name => s"""["$name"]""").mkString(",")
        new KustoOperationResult(
          s"""{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"TableName","DataType":"String","ColumnType":"string"}],"Rows":[$rows]}]}""",
          "v1")
      } else {
        require(command.startsWith(".drop tables (") && command.endsWith(") ifexists"))
        val dropped =
          command.stripPrefix(".drop tables (").stripSuffix(") ifexists").split(',').toSet
        drops += dropped
        tables --= dropped
        new KustoOperationResult(
          """{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"TableName","DataType":"String","ColumnType":"string"}],"Rows":[]}]}""",
          "v1")
      }
    }
  }

  "validateResultsAndCleanup" should "leave another active run's table intact by default" in {
    val fixture = new Fixture

    KustoTestUtils.validateResultsAndCleanup(
      fixture.client,
      firstTable,
      database,
      expectedNumberOfRows = 1,
      timeoutMs = 10,
      tableCleanupPrefix = prefix)

    fixture.tables.toSet shouldBe Set(secondTable)
    fixture.drops.toSeq shouldBe Seq(Set(firstTable))

    KustoTestUtils.validateResultsAndCleanup(
      fixture.client,
      secondTable,
      database,
      expectedNumberOfRows = 1,
      timeoutMs = 10,
      tableCleanupPrefix = prefix)

    fixture.tables shouldBe empty
    fixture.drops.toSeq shouldBe Seq(Set(firstTable), Set(secondTable))
    Seq(firstTable, secondTable).foreach { table =>
      verify(fixture.client).executeQuery(database, s"$table | count")
      verify(fixture.client).executeMgmt(database, generateDropTablesCommand(table))
    }
    verifyNoMoreInteractions(fixture.client)
  }

  it should "clean up the exact table without requiring a shared prefix" in {
    val fixture = new Fixture

    KustoTestUtils.validateResultsAndCleanup(
      fixture.client,
      firstTable,
      database,
      expectedNumberOfRows = -1,
      timeoutMs = 0)

    fixture.tables.toSet shouldBe Set(secondTable)
    verify(fixture.client).executeMgmt(database, generateDropTablesCommand(firstTable))
    verifyNoMoreInteractions(fixture.client)
  }

  it should "retain prefix-wide cleanup only when explicitly requested" in {
    val fixture = new Fixture

    KustoTestUtils.validateResultsAndCleanup(
      fixture.client,
      firstTable,
      database,
      expectedNumberOfRows = -1,
      timeoutMs = 0,
      cleanupAllTables = true,
      tableCleanupPrefix = prefix)

    fixture.tables shouldBe empty
    fixture.drops.toSeq shouldBe Seq(Set(firstTable, secondTable))
    verify(fixture.client)
      .executeMgmt(database, generateFindCurrentTempTablesCommand(Array(prefix)))
    verify(fixture.client).executeMgmt(
      database,
      generateDropTablesCommand(Seq(firstTable, secondTable).mkString(",")))
    verifyNoMoreInteractions(fixture.client)
  }
}

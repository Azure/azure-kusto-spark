// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import org.apache.log4j.{AppenderSkeleton, Level, Logger}
import org.apache.log4j.spi.LoggingEvent
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.InputStream
import java.net.URLClassLoader
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue

class KustoConnectorVersionLoggingTest extends AnyFlatSpec with Matchers {
  private val moduleName = "com.microsoft.kusto.spark.utils.KustoDataSourceUtils$"
  private val sdkUtilsName = "com.microsoft.azure.kusto.data.Utils"

  private class CapturingAppender extends AppenderSkeleton {
    val events = new ConcurrentLinkedQueue[(Level, String)]()
    override protected def append(event: LoggingEvent): Unit = {
      events.add((event.getLevel, event.getRenderedMessage))
    }
    override def close(): Unit = ()
    override def requiresLayout(): Boolean = false
  }

  private def withLogging(level: Level)(test: (Logger, CapturingAppender) => Unit): Unit = {
    val logger = Logger.getLogger("KustoConnector")
    val previousLevel = logger.getLevel
    val previousAdditivity = logger.getAdditivity
    val appender = new CapturingAppender()
    try {
      logger.setLevel(level)
      logger.setAdditivity(false)
      logger.addAppender(appender)
      test(logger, appender)
    } finally {
      logger.removeAppender(appender)
      appender.close()
      logger.setLevel(previousLevel)
      logger.setAdditivity(previousAdditivity)
    }
  }

  private def withFreshUtilities(test: ClassLoader => Unit): Unit =
    withFreshUtilities(hideSdkMetadata = false)(test)

  private def withFreshUtilities(hideSdkMetadata: Boolean)(test: ClassLoader => Unit): Unit = {
    val parent = getClass.getClassLoader
    val location =
      Class.forName(moduleName, false, parent).getProtectionDomain.getCodeSource.getLocation
    val sdkLocation =
      Class.forName(sdkUtilsName, false, parent).getProtectionDomain.getCodeSource.getLocation
    // Isolate initialization and optional SDK metadata failure; keep the real logger shared.
    val loader = new URLClassLoader(Array(location, sdkLocation), parent) {
      override protected def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
        val isolatedSdk = hideSdkMetadata &&
          (name == sdkUtilsName || name.startsWith(sdkUtilsName + "$"))
        if (name.startsWith(moduleName) || isolatedSdk) {
          val loaded: Class[_] = Option(findLoadedClass(name)).getOrElse(findClass(name))
          if (resolve) resolveClass(loaded)
          loaded
        } else super.loadClass(name, resolve)
      }

      override def getResourceAsStream(name: String): InputStream =
        if (hideSdkMetadata && name == "app.properties") null
        else super.getResourceAsStream(name)
    }
    try test(loader)
    finally loader.close()
  }

  private def initialize(loader: ClassLoader): AnyRef =
    Class.forName(moduleName, true, loader).getField("MODULE$").get(null)

  private def resourceVersion(loader: ClassLoader, resource: String, key: String): String = {
    val properties = new Properties()
    val input = loader.getResourceAsStream(resource)
    require(input != null, s"Packaged version properties are missing: $resource")
    try properties.load(input)
    finally input.close()
    val version = properties.getProperty(key)
    version should not be null
    version should not be empty
    version should not include "${"
    version
  }

  private def packagedVersion(loader: ClassLoader): String =
    resourceVersion(loader, "spark.kusto.properties", "application.version")

  private def packagedSdkVersion(loader: ClassLoader): String =
    resourceVersion(
      loader,
      "META-INF/maven/com.microsoft.azure.kusto/kusto-data/pom.properties",
      "version")

  private def banner(version: String, sdkVersion: String): String =
    s"KustoDataSourceUtils$$: Kusto.Spark.Connector:$version|Kusto.Java.Client:$sdkVersion"

  "Connector version logging" should "log searchable connector and SDK versions once at initialization" in {
    withLogging(Level.INFO) { (logger, appender) =>
      withFreshUtilities { loader =>
        val expectedVersion = packagedVersion(loader)
        val expectedSdkVersion = packagedSdkVersion(loader)
        Class.forName(moduleName, false, loader)
        appender.events shouldBe empty

        val utilities = initialize(loader)
        utilities.getClass.getMethod("Version").invoke(utilities) shouldBe expectedVersion
        appender.events.size() shouldBe 1
        appender.events
          .peek() shouldBe ((Level.INFO, banner(expectedVersion, expectedSdkVersion)))
        appender.events
          .peek()
          ._2
          .stripPrefix("KustoDataSourceUtils$: ")
          .exists(_.isWhitespace) shouldBe false

        initialize(loader) should be theSameInstanceAs utilities
        utilities.getClass
          .getMethod("getDedupTagsPrefix", classOf[String], classOf[String])
          .invoke(utilities, "request", "batch") shouldBe "request_batch"
        appender.events.size() shouldBe 1
        logger.getLevel shouldBe Level.INFO
      }
    }
  }

  it should "log independently for each utility class loader" in {
    withLogging(Level.INFO) { (_, appender) =>
      withFreshUtilities { loader =>
        initialize(loader)
        appender.events.poll() shouldBe ((
          Level.INFO,
          banner(packagedVersion(loader), packagedSdkVersion(loader))))
        appender.events shouldBe empty
      }
      withFreshUtilities { loader =>
        initialize(loader)
        appender.events.poll() shouldBe ((
          Level.INFO,
          banner(packagedVersion(loader), packagedSdkVersion(loader))))
        appender.events shouldBe empty
      }
    }
  }

  it should "respect the configured log level without replaying initialization" in {
    withLogging(Level.WARN) { (logger, appender) =>
      withFreshUtilities { loader =>
        val utilities = initialize(loader)
        logger.getLevel shouldBe Level.WARN
        appender.events shouldBe empty

        logger.setLevel(Level.INFO)
        initialize(loader) should be theSameInstanceAs utilities
        appender.events shouldBe empty
      }
    }
  }

  it should "retain the connector version when SDK metadata is unavailable" in {
    withLogging(Level.INFO) { (_, appender) =>
      withFreshUtilities(hideSdkMetadata = true) { loader =>
        val utilities = initialize(loader)

        appender.events.size() shouldBe 1
        appender.events.peek() shouldBe ((Level.INFO, banner(packagedVersion(loader), "unknown")))
        utilities.getClass
          .getMethod("getDedupTagsPrefix", classOf[String], classOf[String])
          .invoke(utilities, "request", "batch") shouldBe "request_batch"
      }
    }
  }
}

package com.microsoft.kusto.spark.authentication

import java.awt.datatransfer.{DataFlavor, StringSelection}
import java.awt.{Desktop, Toolkit}
import java.net.URI
import java.util.concurrent.{ExecutionException, ExecutorService, Executors}
import java.util.function.Consumer

import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAuthenticationResult, PublicClientApplication}
import javax.naming.ServiceUnavailableException
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._
import scala.util.Try

object DeviceAuthentication {
  // This is the Kusto client id from the java client used for device authentication.
  private val CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

  private def getAadAuthorityUrl(authority: String): String = {
    val aadAuthorityFromEnv = System.getenv("AadAuthorityUri")
    if (aadAuthorityFromEnv == null) {
      String.format("https://login.microsoftonline.com/%s", authority)
    } else {
      String.format("%s%s%s", aadAuthorityFromEnv, if (aadAuthorityFromEnv.endsWith("/")) "" else "/", authority)
    }
  }

  def acquireAccessTokenUsingDeviceCodeFlow(clusterUrl: String, authority: String = "common") : String = {
    val service: ExecutorService = Executors.newSingleThreadExecutor
    try{
      val publicClientApp =  PublicClientApplication.builder(CLIENT_ID)
        .authority(getAadAuthorityUrl(authority))
        .executorService(service)
        .build();

      val result = waitAndAcquireTokenByDeviceCode(publicClientApp, clusterUrl)
      if (result == null) throw new ServiceUnavailableException("authentication result was null")
      result.accessToken()
    }finally {
      service.shutdown()
    }
  }

  @throws[InterruptedException]
  private def waitAndAcquireTokenByDeviceCode(publicClientApp: PublicClientApplication, clusterUrl: String): IAuthenticationResult = {
    var timeout = 15 * 1000
    var result: IAuthenticationResult = null

    // Logging is set to Fatal as root
    val prevLevel = Logger.getLogger(classOf[PublicClientApplication]).getLevel
    Thread.sleep(5000)
    Logger.getLogger(classOf[IAuthenticationResult]).setLevel(Level.FATAL)
    while ( result == null && timeout > 0) {
      try {
        val deviceCodeFlowParams = DeviceCodeFlowParameters.builder(Set(clusterUrl+"/.default").asJava,DeviceCodeConsumer).build()
        result = publicClientApp.acquireToken(deviceCodeFlowParams).get()
      } catch {
        case e: ExecutionException =>
          Thread.sleep(1000)
          timeout -= 1000
      }
    }
    Logger.getLogger(classOf[PublicClientApplication]).setLevel(prevLevel)
    result
  }

  private def DeviceCodeConsumer: Consumer[DeviceCode] = {
    new Consumer[DeviceCode] {
      override def accept(t: DeviceCode): Unit = {
        var text: String = null
        Try {
          val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
          val dataFlavor = DataFlavor.stringFlavor
          if (clipboard.isDataFlavorAvailable(dataFlavor)) {
            text = clipboard.getData(dataFlavor).asInstanceOf[String]
          }
          clipboard.setContents(new StringSelection(t.userCode()), null)
        }

        println(t.message() +
          (if (!Desktop.isDesktopSupported) t
          else " device code is already copied to clipboard - just press ctrl+v in the web"))
        if (Desktop.isDesktopSupported) Desktop.getDesktop.browse(new URI(t.verificationUri()))
        if (text != null) {
          Try {
            val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
            clipboard.setContents(new StringSelection(text), null)
          }
        }
        Thread.sleep(10000)
      }
    }
  }
}
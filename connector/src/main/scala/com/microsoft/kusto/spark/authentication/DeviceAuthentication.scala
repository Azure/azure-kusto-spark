package com.microsoft.kusto.spark.authentication

import java.awt.{Desktop, Toolkit}
import java.awt.datatransfer.{DataFlavor, StringSelection}
import java.net.URI
import java.util.concurrent.{ExecutionException, ExecutorService, Executors}

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult, DeviceCode}
import javax.naming.ServiceUnavailableException
import org.apache.log4j.{Level, Logger}

import scala.util.Try

object DeviceAuthentication {
  // This is the Kusto client id from the java client used for device authentication.
  private val CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

  def acquireDeviceCode(clusterUrl: String, authority: String = "common"): DeviceCode = {
    var aadAuthorityUri = ""
    val aadAuthorityFromEnv = System.getenv("AadAuthorityUri")
    if (aadAuthorityFromEnv == null) {
      aadAuthorityUri = String.format("https://login.microsoftonline.com/%s", authority)
    } else {
      aadAuthorityUri = String.format("%s%s%s", aadAuthorityFromEnv, if (aadAuthorityFromEnv.endsWith("/")) "" else "/", authority)
    }

    val service = Executors.newSingleThreadExecutor
    val context: AuthenticationContext =  new AuthenticationContext(aadAuthorityUri, true, service)

    context.acquireDeviceCode(CLIENT_ID, clusterUrl, null).get
  }

  def acquireAccessTokenUsingDeviceCodeFlow(clusterUrl: String, authority: String = "common", userDeviceCode: Option[DeviceCode] = None) : String = {
    val deviceCode = if (userDeviceCode.isDefined) userDeviceCode.get else acquireDeviceCode(clusterUrl, authority)
    val aadAuthorityUri = s"https://login.microsoftonline.com/$authority"
    val service: ExecutorService =
      Executors.newSingleThreadExecutor
    val context: AuthenticationContext =  new AuthenticationContext(aadAuthorityUri, true, service)

    var text: String = null
    Try {
      val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
      val dataFlavor = DataFlavor.stringFlavor
      if (clipboard.isDataFlavorAvailable(dataFlavor)) {
        text = clipboard.getData(dataFlavor).asInstanceOf[String]
      }
      clipboard.setContents(new StringSelection(deviceCode.getUserCode), null)
    }

    println(deviceCode.getMessage +
      (if (!Desktop.isDesktopSupported) deviceCode
      else " device code is already copied to clipboard - just press ctrl+v in the web"))
    if (Desktop.isDesktopSupported) Desktop.getDesktop.browse(new URI(deviceCode.getVerificationUrl))
    val result = waitAndAcquireTokenByDeviceCode(deviceCode, context)
    if(text != null) {
      Try {
        val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
        clipboard.setContents(new StringSelection(text), null)
      }
    }
    if (result == null) throw new ServiceUnavailableException("authentication result was null")
    result.getAccessToken
  }

  @throws[InterruptedException]
  private def waitAndAcquireTokenByDeviceCode(deviceCode: DeviceCode, context: AuthenticationContext): AuthenticationResult = {
    var timeout = 15 * 1000
    var result: AuthenticationResult = null

    // Logging is set to Fatal as root
    val prevLevel = Logger.getLogger(classOf[AuthenticationContext]).getLevel
    Thread.sleep(5000)
    Logger.getLogger(classOf[AuthenticationContext]).setLevel(Level.FATAL)
    while ( result == null && timeout > 0) {
      try {
        result = context.acquireTokenByDeviceCode(deviceCode, null).get

      } catch {
        case e: ExecutionException =>
          Thread.sleep(1000)
          timeout -= 1000
      }
    }

    Logger.getLogger(classOf[AuthenticationContext]).setLevel(prevLevel)
    result
  }
}

// This class helps using device authentication in pyspark
class DeviceAuthentication(val clusterUrl: String, val authority: String = "common") {
  import DeviceAuthentication._

  var currentDeviceCode: Option[DeviceCode] = None

  def getDeviceCodeMessage: String = {
    if (currentDeviceCode.isEmpty) {
      refreshDeviceCode()
    }

    currentDeviceCode.get.getMessage
  }

  def acquireToken(): String = {
    if (currentDeviceCode.isEmpty) {
      refreshDeviceCode()
    }

    acquireAccessTokenUsingDeviceCodeFlow(clusterUrl, authority, currentDeviceCode)
  }

  def refreshDeviceCode(): Unit = {
    currentDeviceCode = Some(acquireDeviceCode(clusterUrl, authority))
  }
}

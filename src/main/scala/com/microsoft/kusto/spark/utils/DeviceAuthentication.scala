package com.microsoft.kusto.spark.utils

import java.awt.Desktop
import java.net.URI
import java.util.concurrent.{ExecutionException, ExecutorService, Executors, Future}

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult, DeviceCode}
import javax.naming.ServiceUnavailableException
import org.apache.log4j.{Level, Logger}
import java.awt.Toolkit
import java.awt.datatransfer.StringSelection
import java.awt.datatransfer.DataFlavor


object DeviceAuthentication {
  private val CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

  def acquireAccessTokenUsingDeviceCodeFlow(clusterUrl: String, authority: String = "common") : String =
  {
    val aadAuthorityUri = s"https://login.microsoftonline.com/$authority"
    val service: ExecutorService =
    Executors.newSingleThreadExecutor
    val context: AuthenticationContext =  new AuthenticationContext(aadAuthorityUri, true, service)

    val future: Future[DeviceCode] = context.acquireDeviceCode(CLIENT_ID, clusterUrl, null)
    val deviceCode: DeviceCode = future.get
    val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
    val dataFlavor = DataFlavor.stringFlavor
    var text: String = null
    if (clipboard.isDataFlavorAvailable(dataFlavor)) {
      text = clipboard.getData(dataFlavor).asInstanceOf[String]
    }

    clipboard.setContents(new StringSelection(deviceCode.getUserCode), null)
    println(deviceCode.getMessage + " device code is already copied to clipboard - just press ctrl+v in the web")
    if (Desktop.isDesktopSupported) Desktop.getDesktop.browse(new URI(deviceCode.getVerificationUrl))
    var result = waitAndAcquireTokenByDeviceCode(deviceCode, context)
    clipboard.setContents(new StringSelection(text), null)

    if (result == null) throw new ServiceUnavailableException("authentication result was null")
    result.getAccessToken
  }

  @throws[InterruptedException]
  private def waitAndAcquireTokenByDeviceCode(deviceCode: DeviceCode, context: AuthenticationContext): AuthenticationResult = {
    var timeout = 15 * 1000
    var result: AuthenticationResult = null

    val prevLevel = Logger.getLogger(classOf[AuthenticationContext]).getLevel
    Logger.getLogger(classOf[AuthenticationContext]).setLevel(Level.FATAL)
    Thread.sleep(5000)
    while ( result == null && timeout > 0) {
      try {
        val futureResult = context.acquireTokenByDeviceCode(deviceCode, null)
        result = futureResult.get()
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

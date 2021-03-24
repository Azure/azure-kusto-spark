package com.microsoft.kusto.spark.authentication

import java.awt.{Desktop, Toolkit}
import java.awt.datatransfer.{DataFlavor, StringSelection}
import java.net.{MalformedURLException, URI, URISyntaxException}
import java.util
import java.util.{HashSet, Set}
import java.util.concurrent.{CompletableFuture, ExecutionException, ExecutorService, Executors, TimeUnit, TimeoutException}
import java.util.function.Consumer

import com.microsoft.aad.adal4j.{AuthenticationContext, AuthenticationResult}
import com.microsoft.aad.msal4j.{DeviceCode, DeviceCodeFlowParameters, IAccount, IAuthenticationResult, PublicClientApplication, SilentParameters}
import com.microsoft.azure.kusto.data.auth
import com.microsoft.azure.kusto.data.exceptions.DataClientException
import javax.naming.ServiceUnavailableException
import org.apache.log4j.{Level, Logger}

import scala.util.Try

object DeviceAuthentication {
  // This is the Kusto client id from the java client used for device authentication.

  def getAuthoirtyUrl(clusterUrl: String, authority: String = "common") = {
    var aadAuthorityUri = ""
    val aadAuthorityFromEnv = System.getenv("AadAuthorityUri")
    if (aadAuthorityFromEnv == null) {
      String.format("https://login.microsoftonline.com/%s", authority)
    } else {
      String.format("%s%s%s", aadAuthorityFromEnv, if (aadAuthorityFromEnv.endsWith("/")) "" else "/", authority)
    }
  }

  //  def acquireAccessTokenUsingDeviceCodeFlow(clusterUrl: String, authority: String = "common", userDeviceCode: Option[DeviceCode] = None) : String = {
  //    val deviceCode = if (userDeviceCode.isDefined) userDeviceCode.get else getAuthoirtyUrl(clusterUrl, authority)
  //    val aadAuthorityUri = s"https://login.microsoftonline.com/$authority"
  //    val service: ExecutorService =
  //      Executors.newSingleThreadExecutor
  //    val context: AuthenticationContext =  new AuthenticationContext(aadAuthorityUri, true, service)
  //
  //    var text: String = null
  //    Try {
  //      val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
  //      val dataFlavor = DataFlavor.stringFlavor
  //      if (clipboard.isDataFlavorAvailable(dataFlavor)) {
  //        text = clipboard.getData(dataFlavor).asInstanceOf[String]
  //      }
  //      clipboard.setContents(new StringSelection(deviceCode.getUserCode), null)
  //    }
  //
  //    println(deviceCode.getMessage +
  //      (if (!Desktop.isDesktopSupported) deviceCode
  //      else " device code is already copied to clipboard - just press ctrl+v in the web"))
  //    if (Desktop.isDesktopSupported) Desktop.getDesktop.browse(new URI(deviceCode.getVerificationUrl))
  //    val result = waitAndAcquireTokenByDeviceCode(deviceCode, context)
  //    if(text != null) {
  //      Try {
  //        val clipboard = Toolkit.getDefaultToolkit.getSystemClipboard
  //        clipboard.setContents(new StringSelection(text), null)
  //      }
  //    }
  //    if (result == null) throw new ServiceUnavailableException("authentication result was null")
  //    result.getAccessToken
  //  }

  //  @throws[InterruptedException]
  //  private def waitAndAcquireTokenByDeviceCode(deviceCode: DeviceCode, context: AuthenticationContext): AuthenticationResult = {
  //    var timeout = 15 * 1000
  //    var result: AuthenticationResult = null
  //
  //    // Logging is set to Fatal as root
  //    val prevLevel = Logger.getLogger(classOf[AuthenticationContext]).getLevel
  //    Thread.sleep(5000)
  //    Logger.getLogger(classOf[AuthenticationContext]).setLevel(Level.FATAL)
  //    while ( result == null && timeout > 0) {
  //      try {
  //        result = context.acquireTokenByDeviceCode(deviceCode, null).get
  //
  //      } catch {
  //        case e: ExecutionException =>
  //          Thread.sleep(1000)
  //          timeout -= 1000
  //      }
  //    }
  //
  //    Logger.getLogger(classOf[AuthenticationContext]).setLevel(prevLevel)
  //    result
  //  }
  //}
}

class DeviceAuthTokenProvider (val cluster: String, val authority:String) extends auth.DeviceAuthTokenProvider(cluster, authority) {
  override def acquireNewAccessToken(): IAuthenticationResult = {
    val deviceCodeConsumer: Consumer[DeviceCode] = (deviceCode: DeviceCode) => {
      def foo(deviceCode: DeviceCode) = {
        System.out.println(deviceCode.message)
      }

      foo(deviceCode)
    }

    val deviceCodeFlowParams: DeviceCodeFlowParameters = DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build
    clientApplication.acquireToken(deviceCodeFlowParams).join
  }
  def getDeviceCodeMessage: String = {""}
}

//class DeviceAuthTokenProvider(clusterUrl:String, authorityId:String, consumer: Option[Consumer[DeviceCode]]) {
//  private val CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
//  var authorityUrl: String = DeviceAuthentication.getAuthoirtyUrl(authorityId)
//  var clientApplication: PublicClientApplication = try {
//    PublicClientApplication.builder(CLIENT_ID).authority(authorityUrl).build()
//  } catch {
//    case e: MalformedURLException =>
//      throw new URISyntaxException(authorityUrl, "Error acquiring ApplicationAccessToken due to invalid Authority URL")
//  }
//
//  val scope: String = String.format("%s/%s", clusterUrl, ".default")
//  val scopes = new util.HashSet[String]
//  scopes.add(scope)
//
//  def auth(): Unit = {
//    val deviceCodeFlowParams = DeviceCodeFlowParameters.builder(scopes, if (consumer.isDefined) consumer.get else deviceCodeConsumer).build()
//    clientApplication.acquireToken(deviceCodeFlowParams).join()
//  }
//
//  private def deviceCodeConsumer: Consumer[DeviceCode] = {
//    new Consumer[DeviceCode] {
//      override def accept(t: DeviceCode): Unit = {
//        //print deviceCode.message()
//      }
//    }
//  }
//
//  // This class helps using device authentication in pyspark
//  class DeviceAuthentication(val clusterUrl: String, val authority: String = "common") {
//
//    import DeviceAuthentication._
//
//    var deviceCode: Option[DeviceCode] = None
//    private val consumer = new Consumer[DeviceCode] {
//      override def accept(code: DeviceCode): Unit = {
//        deviceCode = Some(code)
//      }
//    }
//    val provider = new DeviceAuthTokenProvider(clusterUrl, authority, Some(consumer))
//    var currentDeviceCode: Option[DeviceCode] = None
//    protected val TIMEOUT_MS: Int = 20 * 1000
//
//    //    def acquireAccessToken: String = {
//    //      var accessTokenResult = acquireAccessTokenSilently
//    ////      if (accessTokenResult == null) accessTokenResult = acquireNewAccessToken
//    //      accessTokenResult.accessToken
//    //    }
//
//    //    def acquireAccessTokenSilently: IAuthenticationResult = {
//    //      try {
//    //
//    //        val accounts = clientApplication.getAccounts
//    //        clientApplication.acquireTokenSilently(getSilentParameters(accounts.join)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
//    //      }
//    //      catch {
//    //        case e: MalformedURLException =>
//    //          throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e)
//    //        case e@(_: TimeoutException | _: ExecutionException) =>
//    //          null // Legitimate outcome, in which case a new token will be acquired
//    //
//    //        case e: InterruptedException =>
//    //          Thread.currentThread.interrupt()
//    //          null
//    //      }
//    //    }
//
//    //    private def getSilentParameters(accountSet: util.Set[IAccount]): SilentParameters = {
//    //      val account = getAccount(accountSet)
//    //      if (account != null) return SilentParameters.builder(scopes).account(account).authorityUrl(aadAuthorityUrl).build
//    //      SilentParameters.builder(scopes).authorityUrl(aadAuthorityUrl).build
//    //    }
//    def getDeviceCodeMessage: String = {
//      if (currentDeviceCode.isEmpty) {
//        refreshDeviceCode()
//      }
//
//      currentDeviceCode.get.message()
//    }
//
//    def acquireToken(): String = {
//      if (currentDeviceCode.isEmpty || currentDeviceCode.get.expiresIn() <= 0) {
//        refreshDeviceCode()
//      }
//      ""
//      //      acquireAccessTokenUsingDeviceCodeFlow(clusterUrl, authority, currentDeviceCode)
//      //      provider.
//    }
//
//    def refreshDeviceCode(): Unit = {
//      //      currentDeviceCode = Some(deviceCode)
//    }
//  }
//
//}


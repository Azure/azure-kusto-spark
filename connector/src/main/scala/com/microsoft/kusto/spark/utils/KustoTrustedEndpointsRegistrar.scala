// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.kusto.spark.utils

import com.microsoft.azure.kusto.data.auth.endpoints.{KustoTrustedEndpoints, MatchRule}
import org.apache.log4j.Logger

import java.util.concurrent.atomic.AtomicBoolean

private[kusto] object KustoTrustedEndpointsRegistrar {

  private val logger = Logger.getLogger("KustoConnector")
  private val registered = new AtomicBoolean(false)

  // Sovereign cloud rules for Bleu (France), Delos (Germany), and Gov SG (Singapore).
  // These are not included in WellKnownKustoEndpoints.json bundled with SDK v5.1.1.
  val SovereignCloudRules: java.util.List[MatchRule] = java.util.Arrays.asList(
    // Bleu (France)
    new MatchRule(".kusto.sovcloud-api.fr", false),
    new MatchRule(".kustomfa.sovcloud-api.fr", false),
    new MatchRule("adx.applicationinsights.azure.fr", true),
    new MatchRule("adx.loganalytics.azure.fr", true),
    new MatchRule("adx.monitor.azure.fr", true),
    // Delos (Germany)
    new MatchRule(".kusto.sovcloud-api.de", false),
    new MatchRule(".kustomfa.sovcloud-api.de", false),
    new MatchRule("adx.applicationinsights.azure.de", true),
    new MatchRule("adx.loganalytics.azure.de", true),
    new MatchRule("adx.monitor.azure.de", true),
    // Gov SG (Singapore)
    new MatchRule(".kusto.sovcloud-api.sg", false),
    new MatchRule(".kustomfa.sovcloud-api.sg", false),
    new MatchRule("adx.applicationinsights.azure.sg", true),
    new MatchRule("adx.loganalytics.azure.sg", true),
    new MatchRule("adx.monitor.azure.sg", true))

  def ensureRegistered(): Unit = {
    if (registered.compareAndSet(false, true)) {
      try {
        KustoTrustedEndpoints.addTrustedHosts(SovereignCloudRules, false)
        val codeSource = classOf[KustoTrustedEndpoints].getProtectionDomain.getCodeSource
        logger.info(
          s"Registered ${SovereignCloudRules.size()} sovereign-cloud trusted-host rules. " +
            s"KustoTrustedEndpoints loaded from: $codeSource")
      } catch {
        case e: Exception =>
          registered.set(false)
          logger.error("Failed to register sovereign-cloud trusted-host rules", e)
      }
    }
  }
}

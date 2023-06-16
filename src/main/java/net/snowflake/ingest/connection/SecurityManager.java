/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.ingest.utils.ThreadFactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages creating and automatically renewing the token
 *
 * @author obabarinsa
 * @since 1.8
 */
abstract class SecurityManager implements AutoCloseable {
  // the logger for SecurityManager
  protected static final Logger LOGGER = LoggerFactory.getLogger(SecurityManager.class);

  protected final String account;

  // Fingerprint of public key sent from client in jwt payload
  protected String publicKeyFingerPrint;

  // the name of the user who will be loading the files
  protected final String user;

  // Did we fail to refresh our token at some point?
  protected final AtomicBoolean refreshFailed;

  // Thread factory for daemon threads so that application can shutdown
  final ThreadFactory tf = ThreadFactoryUtil.poolThreadFactory(getClass().getSimpleName(), true);

  // the thread we use for refresh tokens
  protected ScheduledExecutorService tokenRefresher = Executors.newScheduledThreadPool(1, tf);

  // Reference to the Telemetry Service in the client
  protected final TelemetryService telemetryService;

  /**
   * Creates a SecurityManager entity for a given account
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   */
  SecurityManager(String accountName, String username, TelemetryService telemetryService) {
    // if any of our arguments are null, throw an exception
    if (accountName == null || username == null) {
      throw new IllegalArgumentException();
    }
    account = parseAccount(accountName);
    user = username.toUpperCase();

    // we haven't yet failed to regenerate our token
    this.refreshFailed = new AtomicBoolean();

    this.telemetryService = telemetryService;
  }

  private String parseAccount(final String accountName) {
    String parseAccount = null;
    if (accountName.contains(".")) {
      final String[] accountParts = accountName.split("[.]");
      if (accountParts.length > 1) {
        parseAccount = accountParts[0];
      }
    } else {
      parseAccount = accountName;
    }
    return parseAccount.toUpperCase();
  }

  /**
   * getToken - returns we've most recently generated
   *
   * @return the string version of a valid token
   * @throws SecurityException if we failed to regenerate or refresh a token since the last call
   */
  abstract String getToken();

  /** getTokenType - returns the token type, either "KEYPAIR_JWT" or "OAUTH" */
  abstract String getTokenType();

  /* Only used in testing at the moment */
  final String getAccount() {
    return this.account;
  }

  /** Only called by SecurityManagerTest */
  final String getPublicKeyFingerPrint() {
    return publicKeyFingerPrint;
  }

  public abstract void close();
}

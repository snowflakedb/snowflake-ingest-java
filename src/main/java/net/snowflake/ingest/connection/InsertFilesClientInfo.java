/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.ingest.connection;

import java.nio.charset.StandardCharsets;

/**
 * Just a wrapper class which is serialised into REST request for insertFiles.
 *
 * <p>This is an optional field which can be passed with a required field "files" in the request
 * body.
 *
 * <p>Here is how the new request could look like
 *
 * <pre>
 * {
 *   "files":[
 *     {
 *       "path":"file1.csv.gz"
 *     },
 *     {
 *       "path":"file2.csv.gz"
 *     }
 *    ],
 *   "clientInfo": {
 *     "clientSequencer": 1,
 *     "offsetToken": "2"
 *    }
 * }
 * </pre>
 */
public class InsertFilesClientInfo {
  //  FDB constant
  public static final int FDB_MAX_VALUE_SIZE = 100000;

  // client sequencer which the caller thinks it currently has
  private final long clientSequencer;

  // offsetToken to atomically commit with files.
  private final String offsetToken;

  /** Constructor with both fields as required. */
  public InsertFilesClientInfo(long clientSequencer, String offsetToken) {
    if (clientSequencer < 0) {
      throw new IllegalArgumentException("ClientSequencer should be non negative.");
    }
    if (offsetToken == null || offsetToken.isEmpty()) {
      throw new IllegalArgumentException("OffsetToken can not be null or empty.");
    }
    /* Keeping some buffer */
    if (offsetToken.getBytes(StandardCharsets.UTF_8).length > FDB_MAX_VALUE_SIZE - 1000) {
      throw new IllegalArgumentException("OffsetToken size too large.");
    }
    this.clientSequencer = clientSequencer;
    this.offsetToken = offsetToken;
  }

  /** Gets client Sequencer associated with this clientInfo record. */
  public long getClientSequencer() {
    return clientSequencer;
  }

  /** Gets offsetToken associated with this clientInfo record. */
  public String getOffsetToken() {
    return offsetToken;
  }

  /** Only printing clientSequencer */
  @Override
  public String toString() {
    return "InsertFilesClientInfo{" + "clientSequencer=" + clientSequencer + '}';
  }
}

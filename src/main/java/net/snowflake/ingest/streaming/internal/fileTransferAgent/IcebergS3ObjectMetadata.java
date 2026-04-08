/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import java.util.TreeMap;

/**
 * S3 implementation of platform independent StorageObjectMetadata interface. Uses plain Map-based
 * storage (AWS SDK v2 does not have an ObjectMetadata class; metadata is passed directly on the
 * PutObjectRequest builder).
 *
 * <p>It only supports a limited set of metadata properties currently used by the JDBC client
 *
 * @author lgiakoumakis
 */
public class IcebergS3ObjectMetadata implements StorageObjectMetadata {
  private long contentLength;
  private final Map<String, String> userMetadata;
  private String contentEncoding;

  // SSE algorithm to apply (e.g. "AES256", "aws:kms")
  private String sseAlgorithm;

  IcebergS3ObjectMetadata() {
    userMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  }

  @Override
  public Map<String, String> getUserMetadata() {
    return StorageClientUtil.createCaseInsensitiveMap(userMetadata);
  }

  /**
   * Returns the raw (mutable) user metadata map. Used internally by IcebergS3Client to build the
   * PutObjectRequest.
   */
  Map<String, String> getRawUserMetadata() {
    return userMetadata;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  @Override
  public void addUserMetadata(String key, String value) {
    userMetadata.put(key, value);
  }

  @Override
  public void setContentEncoding(String encoding) {
    this.contentEncoding = encoding;
  }

  @Override
  public String getContentEncoding() {
    return contentEncoding;
  }

  /** Sets the server-side encryption algorithm (e.g. "AES256", "aws:kms"). */
  void setSSEAlgorithm(String algorithm) {
    this.sseAlgorithm = algorithm;
  }

  /** Returns the server-side encryption algorithm, or null if not set. */
  String getSSEAlgorithm() {
    return sseAlgorithm;
  }
}

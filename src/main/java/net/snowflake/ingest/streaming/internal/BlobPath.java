/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/**
 * Class to manage blob path strings that might have an embedded security token if its a presigned
 * url
 */
public class BlobPath {
  public final String blobPath;
  public final Boolean hasToken;
  public final String fileName;

  private BlobPath(String fileName, String blobPath, Boolean hasToken) {
    this.blobPath = blobPath;
    this.hasToken = hasToken;
    this.fileName = fileName;
  }

  public static BlobPath fileNameWithoutToken(String fileName) {
    return new BlobPath(fileName, fileName, false);
  }

  public static BlobPath presignedUrlWithToken(String fileName, String url) {
    return new BlobPath(fileName, url, true);
  }
}

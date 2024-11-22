/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata;

/**
 * s3 implementation of platform independent StorageObjectMetadata interface, wraps an S3
 * ObjectMetadata class
 *
 * <p>It only supports a limited set of metadata properties currently used by the JDBC client
 *
 * @author lgiakoumakis
 */
public class IcebergS3ObjectMetadata implements StorageObjectMetadata {
  private ObjectMetadata objectMetadata;

  IcebergS3ObjectMetadata() {
    objectMetadata = new ObjectMetadata();
  }

  // Construct from an AWS S3 ObjectMetadata object
  IcebergS3ObjectMetadata(ObjectMetadata meta) {
    objectMetadata = meta;
  }

  @Override
  public Map<String, String> getUserMetadata() {
    return SnowflakeUtil.createCaseInsensitiveMap(objectMetadata.getUserMetadata());
  }

  @Override
  public long getContentLength() {
    return objectMetadata.getContentLength();
  }

  @Override
  public void setContentLength(long contentLength) {
    objectMetadata.setContentLength(contentLength);
  }

  @Override
  public void addUserMetadata(String key, String value) {
    objectMetadata.addUserMetadata(key, value);
  }

  @Override
  public void setContentEncoding(String encoding) {
    objectMetadata.setContentEncoding(encoding);
  }

  @Override
  public String getContentEncoding() {
    return objectMetadata.getContentEncoding();
  }

  /** @return Returns the encapsulated AWS S3 metadata object */
  ObjectMetadata getS3ObjectMetadata() {
    return objectMetadata;
  }
}

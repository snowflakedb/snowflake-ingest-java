/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/** Class used to deserialized volume information response by server */
class FileLocationInfo {
  /** The stage type */
  @JsonProperty("locationType")
  private String locationType;

  /** The container or bucket */
  @JsonProperty("location")
  private String location;

  /** The path of the target file */
  @JsonProperty("path")
  private String path;

  /** The credentials required for the stage */
  @JsonProperty("creds")
  private Map<String, String> credentials;

  /** AWS/S3/GCS region (S3/GCS only) */
  @JsonProperty("region")
  private String region;

  /** The Azure Storage endpoint (Azure only) */
  @JsonProperty("endPoint")
  private String endPoint;

  /** The Azure Storage account (Azure only) */
  @JsonProperty("storageAccount")
  private String storageAccount;

  /** GCS gives us back a presigned URL instead of a cred */
  @JsonProperty("presignedUrl")
  private String presignedUrl;

  /** Whether to encrypt/decrypt files on the stage */
  @JsonProperty("isClientSideEncrypted")
  private boolean isClientSideEncrypted;

  /** Whether to use s3 regional URL (AWS Only) */
  @JsonProperty("useS3RegionalUrl")
  private boolean useS3RegionalUrl;

  /** A unique id for volume assigned by server */
  @JsonProperty("volumeHash")
  private String volumeHash;

  String getLocationType() {
    return locationType;
  }

  void setLocationType(String locationType) {
    this.locationType = locationType;
  }

  String getLocation() {
    return location;
  }

  void setLocation(String location) {
    this.location = location;
  }

  String getPath() {
    return path;
  }

  void setPath(String path) {
    this.path = path;
  }

  Map<String, String> getCredentials() {
    return credentials;
  }

  void setCredentials(Map<String, String> credentials) {
    this.credentials = credentials;
  }

  String getRegion() {
    return region;
  }

  void setRegion(String region) {
    this.region = region;
  }

  String getEndPoint() {
    return endPoint;
  }

  void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  String getStorageAccount() {
    return storageAccount;
  }

  void setStorageAccount(String storageAccount) {
    this.storageAccount = storageAccount;
  }

  String getPresignedUrl() {
    return presignedUrl;
  }

  void setPresignedUrl(String presignedUrl) {
    this.presignedUrl = presignedUrl;
  }

  boolean getIsClientSideEncrypted() {
    return this.isClientSideEncrypted;
  }

  void setIsClientSideEncrypted(boolean isClientSideEncrypted) {
    this.isClientSideEncrypted = isClientSideEncrypted;
  }

  boolean getUseS3RegionalUrl() {
    return this.useS3RegionalUrl;
  }

  void setUseS3RegionalUrl(boolean useS3RegionalUrl) {
    this.useS3RegionalUrl = useS3RegionalUrl;
  }

  String getVolumeHash() {
    return this.volumeHash;
  }

  void setVolumeHash(String volumeHash) {
    this.volumeHash = volumeHash;
  }
}

/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/** Class used to deserialized volume information response by server */
class FileLocationInfo {
  @JsonProperty("locationType")
  private String locationType; // The stage type

  @JsonProperty("location")
  private String location; // The container or bucket

  @JsonProperty("path")
  private String path; // path of the target file

  @JsonProperty("creds")
  private Map<String, String> credentials; // the credentials required for the  stage

  @JsonProperty("region")
  private String region; // AWS/S3/GCS region (S3/GCS only)

  @JsonProperty("endPoint")
  private String endPoint; // The Azure Storage endpoint (Azure only)

  @JsonProperty("storageAccount")
  private String storageAccount; // The Azure Storage account (Azure only)

  @JsonProperty("presignedUrl")
  private String presignedUrl; // GCS gives us back a presigned URL instead of a cred

  @JsonProperty("isClientSideEncrypted")
  private boolean isClientSideEncrypted; // whether to encrypt/decrypt files on the stage

  @JsonProperty("useS3RegionalUrl")
  private boolean useS3RegionalUrl; // whether to use s3 regional URL (AWS Only)

  @JsonProperty("volumeHash")
  private String volumeHash; // a unique id for volume assigned by server

  String getLocationType() {
    return locationType;
  }

  void setLocationType(String locationType) {
    this.locationType = locationType;
  }

  // remove s3:// or gs:// from location
  String getLocation() {
    return location.replaceAll("s3://", "");
  }

  void setLocation(String location) {
    this.location = location.replaceAll("s3://", "");
  }

  // only keeps the last part of the path
  String getPath() {
    return path.replaceAll("s3://", "");
  }

  void setPath(String path) {
    this.path = path.replaceAll("s3://", "");
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

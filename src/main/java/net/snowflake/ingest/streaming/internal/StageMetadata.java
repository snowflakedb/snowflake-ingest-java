package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;

class StageMetadata {
  @JsonProperty("locationType")
  private String stageType; // The stage type

  @JsonProperty("location")
  private String location; // The container or bucket

  @JsonProperty("path")
  private String path; // path of the target file

  @JsonProperty("creds")
  private Map<?, ?> credentials; // the credentials required for the  stage

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

  String getStageType() {
    return stageType;
  }

  void setLocationType(String stageType) {
    this.stageType = stageType;
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

  Map<?, ?> getCredentials() {
    return credentials;
  }

  void setCredentials(Map<?, ?> credentials) {
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

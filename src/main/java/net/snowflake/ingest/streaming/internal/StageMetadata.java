package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class StageMetadata {
  @JsonProperty(value = "locationType", required = true)
  private String stageType; // The stage type

  @JsonProperty(value = "location", required = true)
  private String location; // The container or bucket

  @JsonProperty(value = "creds", required = true)
  private Map<?, ?> credentials; // the credentials required for the  stage

  @JsonProperty(value = "region")
  private String region; // AWS/S3/GCS region (S3/GCS only)

  @JsonProperty(value = "endPoint")
  private String endPoint; // The Azure Storage endpoint (Azure only)

  @JsonProperty(value = "storageAccount")
  private String storageAccount; // The Azure Storage account (Azure only)

  @JsonProperty(value = "presignedUrl")
  private String presignedUrl; // GCS gives us back a presigned URL instead of a cred

  @JsonProperty(value = "isClientSideEncrypted")
  private boolean isClientSideEncrypted; // whether to encrypt/decrypt files on the stage

  @JsonProperty(value = "useS3RegionalUrl")
  private boolean useS3RegionalUrl; // whether to use s3 regional URL (AWS Only)

  @JsonProperty(value = "volumeHash")
  private String volumeHash; // a unique id for volume assigned by server

  public String getStageType() {
    return stageType;
  }

  public void setLocationType(String stageType) {
    this.stageType = stageType;
  }

  public String getLocation() {
    return location;
  }

  @JsonProperty("location")
  public void setLocation(String location) {
    this.location = location;
  }

  public Map<?, ?> getCredentials() {
    return credentials;
  }

  public void setCredentials(Map<?, ?> credentials) {
    this.credentials = credentials;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  public String getStorageAccount() {
    return storageAccount;
  }

  public void setStorageAccount(String storageAccount) {
    this.storageAccount = storageAccount;
  }

  public String getPresignedUrl() {
    return presignedUrl;
  }

  public void setPresignedUrl(String presignedUrl) {
    this.presignedUrl = presignedUrl;
  }

  public boolean getIsClientSideEncrypted() {
    return this.isClientSideEncrypted;
  }

  public void setIsClientSideEncrypted(boolean isClientSideEncrypted) {
    this.isClientSideEncrypted = isClientSideEncrypted;
  }

  public boolean getUseS3RegionalUrl() {
    return this.useS3RegionalUrl;
  }

  public void setUseS3RegionalUrl(boolean useS3RegionalUrl) {
    this.useS3RegionalUrl = useS3RegionalUrl;
  }

  public String getVolumeHash() {
    return this.volumeHash;
  }

  public void setVolumeHash(String volumeHash) {
    this.volumeHash = volumeHash;
  }
}

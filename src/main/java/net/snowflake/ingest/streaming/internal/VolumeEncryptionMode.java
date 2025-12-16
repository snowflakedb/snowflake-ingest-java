package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum representing the different volume encryption modes supported by Snowflake. These modes
 * determine how data is encrypted when stored in cloud storage.
 */
public enum VolumeEncryptionMode {
  /** AWS S3 server-side encryption with S3-managed keys */
  AWS_SSE_S3("AWS_SSE_S3"),

  /** AWS S3 server-side encryption with AWS KMS-managed keys */
  AWS_SSE_KMS("AWS_SSE_KMS"),

  /** Google Cloud Storage server-side encryption with GCS KMS-managed keys */
  GCS_SSE_KMS("GCS_SSE_KMS"),

  /** No encryption (typically used for Azure as Snowflake doesn't support KMS encryption there) */
  NONE("NONE");

  private final String value;

  VolumeEncryptionMode(String value) {
    this.value = value;
  }

  /**
   * Gets the string value of the encryption mode. This is used for JSON serialization and external
   * APIs.
   *
   * @return the string representation of the encryption mode
   */
  @JsonValue
  public String getValue() {
    return value;
  }

  /**
   * Creates a VolumeEncryptionMode from a string value. Returns null for null input to maintain
   * backward compatibility. This method is used by Jackson for JSON deserialization.
   *
   * @param value the string value
   * @return the corresponding VolumeEncryptionMode, or null if input is null
   * @throws IllegalArgumentException if the value is not recognized
   */
  @JsonCreator
  public static VolumeEncryptionMode fromString(String value) {
    if (value == null) {
      return null;
    }

    for (VolumeEncryptionMode mode : values()) {
      if (mode.value.equals(value)) {
        return mode;
      }
    }

    throw new IllegalArgumentException("Unknown volume encryption mode: " + value);
  }

  @Override
  public String toString() {
    return value;
  }

  /**
   * Checks if this encryption mode requires a KMS key ID.
   *
   * @return true if this mode requires a KMS key ID, false otherwise
   */
  public boolean requiresKmsKeyId() {
    return this == AWS_SSE_KMS || this == GCS_SSE_KMS;
  }

  /**
   * Validates that the given KMS key ID is compatible with this encryption mode.
   *
   * @param encryptionKmsKeyId the KMS key ID to validate
   * @throws IllegalArgumentException if the key ID is invalid for this mode
   */
  public void validateKmsKeyId(String encryptionKmsKeyId) {
    if (requiresKmsKeyId()) {
      if (encryptionKmsKeyId == null || encryptionKmsKeyId.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Encryption KMS key ID is required for volume encryption mode: " + this);
      }
    }
  }
}

package net.snowflake.ingest.streaming.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

/** Unit tests for VolumeEncryptionMode validation */
public class VolumeEncryptionModeTest {

  @Test
  public void testRequiresKmsKeyId() {
    // KMS modes require key ID
    assertThat(VolumeEncryptionMode.AWS_SSE_KMS.requiresKmsKeyId()).isTrue();
    assertThat(VolumeEncryptionMode.GCS_SSE_KMS.requiresKmsKeyId()).isTrue();

    // Non-KMS modes don't require key ID
    assertThat(VolumeEncryptionMode.AWS_SSE_S3.requiresKmsKeyId()).isFalse();
    assertThat(VolumeEncryptionMode.NONE.requiresKmsKeyId()).isFalse();
  }

  @Test
  public void testValidateKmsKeyId_Success() {
    // Valid key ID for KMS modes
    VolumeEncryptionMode.AWS_SSE_KMS.validateKmsKeyId("valid-key-id");
    VolumeEncryptionMode.GCS_SSE_KMS.validateKmsKeyId("valid-key-id");

    // Non-KMS modes should accept null/empty key ID
    VolumeEncryptionMode.AWS_SSE_S3.validateKmsKeyId(null);
    VolumeEncryptionMode.AWS_SSE_S3.validateKmsKeyId("");
    VolumeEncryptionMode.NONE.validateKmsKeyId(null);
    VolumeEncryptionMode.NONE.validateKmsKeyId("");
  }

  @Test
  public void testValidateKmsKeyId_FailsForKmsModeWithNullKey() {
    assertThatThrownBy(() -> VolumeEncryptionMode.AWS_SSE_KMS.validateKmsKeyId(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Encryption KMS key ID is required for volume encryption mode: AWS_SSE_KMS");

    assertThatThrownBy(() -> VolumeEncryptionMode.GCS_SSE_KMS.validateKmsKeyId(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Encryption KMS key ID is required for volume encryption mode: GCS_SSE_KMS");
  }

  @Test
  public void testValidateKmsKeyId_FailsForKmsModeWithEmptyKey() {
    assertThatThrownBy(() -> VolumeEncryptionMode.AWS_SSE_KMS.validateKmsKeyId(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Encryption KMS key ID is required for volume encryption mode: AWS_SSE_KMS");

    assertThatThrownBy(() -> VolumeEncryptionMode.AWS_SSE_KMS.validateKmsKeyId("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Encryption KMS key ID is required for volume encryption mode: AWS_SSE_KMS");
  }

  @Test
  public void testFileLocationInfoValidation_KmsModeWithValidKey() {
    FileLocationInfo info = new FileLocationInfo();

    // Should work in any order now
    info.setVolumeEncryptionMode(VolumeEncryptionMode.AWS_SSE_KMS);
    info.setEncryptionKmsKeyId("valid-key");

    assertThat(info.getVolumeEncryptionMode()).isEqualTo(VolumeEncryptionMode.AWS_SSE_KMS);
    assertThat(info.getEncryptionKmsKeyId()).isEqualTo("valid-key");
  }

  @Test
  public void testFileLocationInfoValidation_NoOrderingDependency() {
    FileLocationInfo info1 = new FileLocationInfo();
    FileLocationInfo info2 = new FileLocationInfo();

    // Order 1: Set mode first, then key
    info1.setVolumeEncryptionMode(VolumeEncryptionMode.AWS_SSE_KMS);
    info1.setEncryptionKmsKeyId("valid-key");

    // Order 2: Set key first, then mode
    info2.setEncryptionKmsKeyId("valid-key");
    info2.setVolumeEncryptionMode(VolumeEncryptionMode.AWS_SSE_KMS);

    // Both should work and be equivalent
    assertThat(info1.getVolumeEncryptionMode()).isEqualTo(info2.getVolumeEncryptionMode());
    assertThat(info1.getEncryptionKmsKeyId()).isEqualTo(info2.getEncryptionKmsKeyId());
  }
}

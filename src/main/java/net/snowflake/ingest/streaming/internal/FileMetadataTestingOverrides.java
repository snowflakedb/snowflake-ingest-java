package net.snowflake.ingest.streaming.internal;

import java.util.Optional;

/**
 * Container for optional overrides of metadata being written to files. Used for testing. In normal
 * operation, the overrides are inactive.
 */
public class FileMetadataTestingOverrides {
  public final Optional<String> customFileId;
  /**
   *
   * <li>Optional.empty() - no override, use the default SDK version.
   * <li>Optional.of(Optional.empty()) - don't send an SDK version.
   * <li>Optional.of("<version>") - send a custom SDK version.
   */
  public final Optional<Optional<String>> customSdkVersion;

  FileMetadataTestingOverrides(
      Optional<String> customFileId, Optional<Optional<String>> customSdkVersion) {
    this.customFileId = customFileId;
    this.customSdkVersion = customSdkVersion;
  }

  public static FileMetadataTestingOverrides none() {
    return new FileMetadataTestingOverrides(Optional.empty(), Optional.empty());
  }

  @Override
  public String toString() {
    return "{customFileId="
        + customFileId.orElse("<unchanged>")
        + ", customSdkVersion="
        + customSdkVersion.map((v) -> v.orElse("<removed>")).orElse("<unchanged>")
        + "}";
  }
}

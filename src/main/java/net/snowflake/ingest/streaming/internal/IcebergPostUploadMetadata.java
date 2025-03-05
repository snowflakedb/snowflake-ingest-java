/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Wrapper class containing etag and the final upload path of the parquet file in the table's
 * external volume.
 */
class IcebergPostUploadMetadata {
  /*
   * With iceberg files, the XP scanner expects the MD5 value to exactly match the etag value in S3.
   * This assumption doesn't hold when multipart upload kicks in, causing scan time failures and
   * table corruption. By plugging in the etag value instead of the md5 value, we can ensure that
   * the XP scanner can successfully scan the file.
   */
  private final String etag;

  /**
   * The updated upload path of the file within the table's external volume. This will be null if
   * {@code fileTransferMetadata} is not refreshed during {@link
   * net.snowflake.ingest.streaming.internal.InternalStage#put}, meaning the final upload path
   * remains the same as the original path. This is necessary for file registration because the
   * final upload path may differ from the original one if it's refreshed after the upload thread
   * has started.
   */
  private final @Nullable BlobPath refreshedPath;

  /**
   * Constructor for IcebergPostUploadMetadata.
   *
   * @param etag The etag of the uploaded file.
   * @param path The updated upload path of the file within the table's external volume.
   */
  IcebergPostUploadMetadata(String etag, @Nullable BlobPath path) {
    this.etag = etag;
    this.refreshedPath = path;
  }

  String getEtag() {
    return etag;
  }

  Optional<BlobPath> getRefreshedPath() {
    return Optional.ofNullable(refreshedPath);
  }

  @Override
  public String toString() {
    return String.format("IcebergPostUploadMetadata(etag=%s, path=%s)", etag, refreshedPath);
  }
}

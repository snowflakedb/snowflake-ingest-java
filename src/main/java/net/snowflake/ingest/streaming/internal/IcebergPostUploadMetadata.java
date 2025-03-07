/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Optional;
import javax.annotation.Nullable;

/** Stores Iceberg post-upload metadata. */
class IcebergPostUploadMetadata {
  /**
   * With iceberg files, the XP scanner expects the MD5 value to exactly match the etag value in S3.
   * This assumption doesn't hold when multipart upload kicks in, causing scan time failures and
   * table corruption. By plugging in the etag value instead of the md5 value, we can ensure that
   * the XP scanner can successfully scan the file.
   */
  private final @Nullable String etag;

  /** The final uploaded blob path of the file within the table's external volume. */
  private final BlobPath blobPath;

  /**
   * Constructor for IcebergPostUploadMetadata.
   *
   * @param etag The etag of the uploaded file.
   * @param blobPath The updated blob path of the file within the table's external volume.
   */
  IcebergPostUploadMetadata(@Nullable String etag, BlobPath blobPath) {
    this.etag = etag;
    this.blobPath = blobPath;
  }

  Optional<String> getEtag() {
    return Optional.ofNullable(etag);
  }

  BlobPath getBlobPath() {
    return blobPath;
  }

  @Override
  public String toString() {
    return String.format("IcebergPostUploadMetadata(etag=%s, blobPath=%s)", etag, blobPath);
  }
}

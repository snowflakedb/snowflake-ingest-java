/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/**
 * Class to maintain the upload-path (relative to the location for which we have authorized access)
 * and the file registration path (relative to the volume location).
 *
 * <p>In the case of FDN tables, these values are identical as we get access to the account's
 * streaming_ingest volume.
 *
 * <p>In the case of Iceberg tables, these values are different since we scope the token down to a
 * per-session subpath under the external volume's location, whereas the file registration still
 * needs to happen relative to the ext vol.
 */
class BlobPath {
  public final String uploadPath;
  public final String fileRegistrationPath;

  public BlobPath(String uploadPath, String fileRegistrationPath) {
    this.uploadPath = uploadPath;
    this.fileRegistrationPath = fileRegistrationPath;
  }
}

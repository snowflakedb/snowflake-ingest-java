/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import net.snowflake.ingest.utils.Constants;

/**
 * Same as {@link BlobMetadata} but with {@link Constants.BdecVersion} to send it to server side if
 * the version is greater than {@link Constants.BdecVersion#ONE}.
 */
class BlobMetadataWithBdecVersion extends BlobMetadata {
  BlobMetadataWithBdecVersion(
      String path, String md5, Constants.BdecVersion bdecVersion, List<ChunkMetadata> chunks) {
    super(path, md5, bdecVersion, chunks);
  }

  @JsonProperty("bdec_version")
  byte getVersionByte() {
    return getVersion().toByte();
  }
}

/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Build a single blob file that contains file header plus data. The header will be a
 * variable-length data structure consisting of:
 *
 * <ul>
 *   <li>4 bytes magic word
 *   <li>1 byte file version
 *   <li>8 bytes total blob size in bytes
 *   <li>8 bytes CRC-32C checksum for the data portion
 *   <li>4 bytes chunks metadata length in bytes
 *   <li>variable size chunks metadata in json format
 * </ul>
 *
 * <p>After the metadata, it will be one or more chunks of variable size Arrow data, and each chunk
 * will be encrypted and compressed separately.
 */
public class BlobBuilder {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Gzip compress the given chunk data
   *
   * @param outputStream
   * @return
   * @throws IOException
   */
  public static byte[] compress(ByteArrayOutputStream outputStream) throws IOException {
    // Based on current experiment, compressing twice will give us the best compression
    // ratio and compression time combination
    int uncompressedSize = outputStream.size();
    ByteArrayOutputStream compressedOutputStream = new ByteArrayOutputStream(uncompressedSize);
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(compressedOutputStream, true)) {
      gzipOutputStream.write(outputStream.toByteArray());
    }

    if (!COMPRESS_BLOB_TWICE) {
      return compressedOutputStream.toByteArray();
    }

    int firstCompressedSize = compressedOutputStream.size();
    ByteArrayOutputStream doubleCompressedOutputStream =
        new ByteArrayOutputStream(firstCompressedSize);
    try (GZIPOutputStream doubleGzipOutputStream =
        new GZIPOutputStream(doubleCompressedOutputStream, true)) {
      doubleGzipOutputStream.write(compressedOutputStream.toByteArray());
    }

    return doubleCompressedOutputStream.toByteArray();
  }

  /**
   * Build the blob file
   *
   * @param chunksMetadataList List of chunk metadata
   * @param chunksDataList List of chunk data
   * @param chunksChecksum Checksum for the chunk data portion
   * @param chunksDataSize Total size for the chunk data portion after compression
   * @return The blob file as a byte array
   * @throws JsonProcessingException
   */
  public static byte[] build(
      List<ChunkMetadata> chunksMetadataList,
      List<byte[]> chunksDataList,
      long chunksChecksum,
      long chunksDataSize)
      throws JsonProcessingException {
    byte[] chunkMetadataListInBytes = MAPPER.writeValueAsBytes(chunksMetadataList);

    int metadataSize =
        BLOB_TAG_SIZE_IN_BYTES
            + BLOB_VERSION_SIZE_IN_BYTES
            + BLOB_FILE_SIZE_SIZE_IN_BYTES
            + BLOB_CHECKSUM_SIZE_IN_BYTES
            + BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES
            + chunkMetadataListInBytes.length;

    // Create the blob file and add the metadata
    ByteArrayOutputStream blob = new ByteArrayOutputStream();
    blob.writeBytes(BLOB_EXTENSION_TYPE.getBytes());
    blob.write(BLOB_FORMAT_VERSION);
    blob.writeBytes(Longs.toByteArray(metadataSize + chunksDataSize));
    blob.writeBytes(Longs.toByteArray(chunksChecksum));
    blob.writeBytes(Ints.toByteArray(chunkMetadataListInBytes.length));
    blob.writeBytes(chunkMetadataListInBytes);
    for (byte[] arrowData : chunksDataList) {
      blob.writeBytes(arrowData);
    }

    // We need to update the start offset for the EP and Arrow data in the request since
    // some metadata was added at the beginning
    for (ChunkMetadata chunkMetadata : chunksMetadataList) {
      chunkMetadata.advanceStartOffset(metadataSize);
    }

    return blob.toByteArray();
  }
}

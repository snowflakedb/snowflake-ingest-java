/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_CHECKSUM_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BLOB_FILE_SIZE_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_FORMAT_VERSION;
import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.BLOB_TAG_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_VERSION_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.COMPRESS_BLOB_TWICE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.ingest.utils.Logging;

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
class BlobBuilder {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logging logger = new Logging(BlobBuilder.class);

  /**
   * Gzip compress the given chunk data
   *
   * @param filePath blob file full path
   * @param chunkData uncompressed chunk data
   * @return compressed chunk data
   * @throws IOException
   */
  static byte[] compress(String filePath, ByteArrayOutputStream chunkData) throws IOException {
    int uncompressedSize = chunkData.size();
    ByteArrayOutputStream compressedOutputStream = new ByteArrayOutputStream(uncompressedSize);
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(compressedOutputStream, true)) {
      gzipOutputStream.write(chunkData.toByteArray());
    }
    int firstCompressedSize = compressedOutputStream.size();

    // Based on current experiment, compressing twice will give us the best compression
    // ratio and compression time combination
    int doubleCompressedSize = 0;
    if (COMPRESS_BLOB_TWICE) {
      ByteArrayOutputStream doubleCompressedOutputStream =
          new ByteArrayOutputStream(firstCompressedSize);
      try (GZIPOutputStream doubleGzipOutputStream =
          new GZIPOutputStream(doubleCompressedOutputStream, true)) {
        doubleGzipOutputStream.write(compressedOutputStream.toByteArray());
      }
      doubleCompressedSize = doubleCompressedOutputStream.size();
      compressedOutputStream = doubleCompressedOutputStream;
    }

    logger.logDebug(
        "Finish compressing chunk in blob={}, uncompressedSize={}, firstCompressedSize={},"
            + " doubleCompressedSize={}",
        filePath,
        uncompressedSize,
        firstCompressedSize,
        doubleCompressedSize);

    return compressedOutputStream.toByteArray();
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
  static byte[] build(
      List<ChunkMetadata> chunksMetadataList,
      List<byte[]> chunksDataList,
      long chunksChecksum,
      long chunksDataSize)
      throws IOException {
    byte[] chunkMetadataListInBytes = MAPPER.writeValueAsBytes(chunksMetadataList);

    int metadataSize =
        BLOB_NO_HEADER
            ? 0
            : BLOB_TAG_SIZE_IN_BYTES
                + BLOB_VERSION_SIZE_IN_BYTES
                + BLOB_FILE_SIZE_SIZE_IN_BYTES
                + BLOB_CHECKSUM_SIZE_IN_BYTES
                + BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES
                + chunkMetadataListInBytes.length;

    // Create the blob file and add the metadata
    ByteArrayOutputStream blob = new ByteArrayOutputStream();
    if (!BLOB_NO_HEADER) {
      blob.write(BLOB_EXTENSION_TYPE.getBytes());
      blob.write(BLOB_FORMAT_VERSION);
      blob.write(Longs.toByteArray(metadataSize + chunksDataSize));
      blob.write(Longs.toByteArray(chunksChecksum));
      blob.write(Ints.toByteArray(chunkMetadataListInBytes.length));
      blob.write(chunkMetadataListInBytes);
    }
    for (byte[] arrowData : chunksDataList) {
      blob.write(arrowData);
    }

    // We need to update the start offset for the EP and Arrow data in the request since
    // some metadata was added at the beginning
    for (ChunkMetadata chunkMetadata : chunksMetadataList) {
      chunkMetadata.advanceStartOffset(metadataSize);
    }

    return blob.toByteArray();
  }

  /**
   * Compute the MD5 for a byte array
   *
   * @param data the input byte array
   * @return lower case MD5 for the compressed chunk data
   * @throws NoSuchAlgorithmException
   */
  static String computeMD5(byte[] data) throws NoSuchAlgorithmException {
    // CASEC-2936
    // https://github.com/snowflakedb/snowflake-ingest-java-private/pull/22
    MessageDigest md = // nosem: java.lang.security.audit.crypto.weak-hash.use-of-md5
        MessageDigest.getInstance("MD5");
    md.update(data);
    byte[] digest = md.digest();
    return DatatypeConverter.printHexBinary(digest).toLowerCase();
  }
}

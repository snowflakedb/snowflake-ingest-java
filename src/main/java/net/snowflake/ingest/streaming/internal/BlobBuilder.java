/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_CHECKSUM_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BLOB_FILE_SIZE_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.BLOB_TAG_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_VERSION_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.COMPRESS_BLOB_TWICE;
import static net.snowflake.ingest.utils.Utils.toByteArray;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;

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
   * @param blockSizeToAlignTo block size to align to for encryption
   * @return padded compressed chunk data, aligned to blockSizeToAlignTo, and actual length of
   *     compressed data before padding at the end
   * @throws IOException
   */
  static Pair<byte[], Integer> compress(
      String filePath, ByteArrayOutputStream chunkData, int blockSizeToAlignTo) throws IOException {
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

    int compressedSize = compressedOutputStream.size();
    int paddingSize = blockSizeToAlignTo - compressedSize % blockSizeToAlignTo;
    compressedOutputStream.write(new byte[paddingSize]);
    return new Pair<>(compressedOutputStream.toByteArray(), compressedSize);
  }

  /**
   * Gzip compress the given chunk data if required by the given write mode and pads the compressed
   * data for encryption.
   *
   * @param filePath blob file full path
   * @param chunkData uncompressed chunk data
   * @param blockSizeToAlignTo block size to align to for encryption
   * @param arrowBatchWriteMode Arrow format write mode
   * @return padded compressed chunk data, aligned to blockSizeToAlignTo, and actual length of
   *     compressed data before padding at the end
   * @throws IOException
   */
  static Pair<byte[], Integer> compressIfNeededAndPadChunk(
      String filePath,
      ByteArrayOutputStream chunkData,
      int blockSizeToAlignTo,
      Constants.ArrowBatchWriteMode arrowBatchWriteMode)
      throws IOException {
    // Encryption needs padding to the ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES
    // to align with decryption on the Snowflake query path starting from this chunk offset.
    // The padding does not have arrow data and not compressed.
    // Hence, the actual chunk size is smaller by the padding size.
    // The compression on the Snowflake query path needs the correct size of the compressed
    // data.
    if (arrowBatchWriteMode == Constants.ArrowBatchWriteMode.STREAM) {
      // Stream write mode does not support column level compression.
      // Compress the chunk data and pad it for encryption.
      return BlobBuilder.compress(filePath, chunkData, blockSizeToAlignTo);
    } else {
      int actualSize = chunkData.size();
      int paddingSize = blockSizeToAlignTo - actualSize % blockSizeToAlignTo;
      chunkData.write(new byte[paddingSize]);
      return new Pair<>(chunkData.toByteArray(), actualSize);
    }
  }

  /**
   * Build the blob file
   *
   * @param chunksMetadataList List of chunk metadata
   * @param chunksDataList List of chunk data
   * @param chunksChecksum Checksum for the chunk data portion
   * @param chunksDataSize Total size for the chunk data portion after compression
   * @param bdecVersion BDEC file version
   * @return The blob file as a byte array
   * @throws JsonProcessingException
   */
  static byte[] build(
      List<ChunkMetadata> chunksMetadataList,
      List<byte[]> chunksDataList,
      long chunksChecksum,
      long chunksDataSize,
      Constants.BdecVerion bdecVersion)
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
      blob.write(bdecVersion.toByte());
      blob.write(toByteArray(metadataSize + chunksDataSize));
      blob.write(toByteArray(chunksChecksum));
      blob.write(toByteArray(chunkMetadataListInBytes.length));
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
    return computeMD5(data, data.length);
  }

  /**
   * Compute the MD5 for a byte array
   *
   * @param data the input byte array
   * @param length the number of bytes from the input byte array starting from zero index
   * @return lower case MD5 for the compressed chunk data
   * @throws NoSuchAlgorithmException
   */
  static String computeMD5(byte[] data, int length) throws NoSuchAlgorithmException {
    // CASEC-2936
    // https://github.com/snowflakedb/snowflake-ingest-java-private/pull/22
    MessageDigest md = // nosem: java.lang.security.audit.crypto.weak-hash.use-of-md5
        MessageDigest.getInstance("MD5");
    md.update(data, 0, length);
    byte[] digest = md.digest();
    return DatatypeConverter.printHexBinary(digest).toLowerCase();
  }
}

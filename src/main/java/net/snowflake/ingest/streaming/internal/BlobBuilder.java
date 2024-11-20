/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_CHECKSUM_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BLOB_FILE_SIZE_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.BLOB_TAG_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_VERSION_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Utils.getParquetFooterSize;
import static net.snowflake.ingest.utils.Utils.toByteArray;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Cryptor;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.codec.binary.Hex;

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
 * <p>After the metadata, it will be one or more chunks of variable size Parquet data, and each
 * chunk will be encrypted and compressed separately.
 */
class BlobBuilder {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logging logger = new Logging(BlobBuilder.class);

  /**
   * Builds blob.
   *
   * @param filePath Path of the destination file in cloud storage
   * @param blobData All the data for one blob. Assumes that all ChannelData in the inner List
   *     belongs to the same table. Will error if this is not the case
   * @param bdecVersion version of blob
   * @return {@link Blob} data
   */
  static <T> Blob constructBlobAndMetadata(
      String filePath,
      List<List<ChannelData<T>>> blobData,
      Constants.BdecVersion bdecVersion,
      InternalParameterProvider internalParameterProvider,
      Map<FullyQualifiedTableName, EncryptionKey> encryptionKeysPerTable)
      throws IOException, NoSuchPaddingException, NoSuchAlgorithmException,
          InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException,
          BadPaddingException {
    List<ChunkMetadata> chunksMetadataList = new ArrayList<>();
    List<byte[]> chunksDataList = new ArrayList<>();
    long curDataSize = 0L;
    CRC32 crc = new CRC32();

    // TODO: channels with different schema can't be combined even if they belongs to same table
    for (List<ChannelData<T>> channelsDataPerTable : blobData) {
      ChannelFlushContext firstChannelFlushContext =
          channelsDataPerTable.get(0).getChannelContext();

      final EncryptionKey encryptionKey =
          encryptionKeysPerTable.getOrDefault(
              new FullyQualifiedTableName(
                  firstChannelFlushContext.getDbName(),
                  firstChannelFlushContext.getSchemaName(),
                  firstChannelFlushContext.getTableName()),
              new EncryptionKey(
                  firstChannelFlushContext.getDbName(),
                  firstChannelFlushContext.getSchemaName(),
                  firstChannelFlushContext.getTableName(),
                  firstChannelFlushContext.getEncryptionKey(),
                  firstChannelFlushContext.getEncryptionKeyId()));

      Flusher<T> flusher = channelsDataPerTable.get(0).createFlusher();
      Flusher.SerializationResult serializedChunk =
          flusher.serialize(channelsDataPerTable, filePath, curDataSize);

      if (!serializedChunk.channelsMetadataList.isEmpty()) {
        final byte[] compressedChunkData;
        final int chunkLength;
        final int compressedChunkDataSize;

        if (internalParameterProvider.getEnableChunkEncryption()) {
          Pair<byte[], Integer> paddedChunk =
              padChunk(serializedChunk.chunkData, Constants.ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES);
          byte[] paddedChunkData = paddedChunk.getFirst();
          chunkLength = paddedChunk.getSecond();

          // Encrypt the compressed chunk data, the encryption key is derived using the key from
          // server with the full blob path.
          // We need to maintain IV as a block counter for the whole file, even interleaved,
          // to align with decryption on the Snowflake query path.
          // TODO: address alignment for the header SNOW-557866
          long iv = curDataSize / Constants.ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES;

          compressedChunkData =
              Cryptor.encrypt(paddedChunkData, encryptionKey.getEncryptionKey(), filePath, iv);

          compressedChunkDataSize = compressedChunkData.length;
        } else {
          compressedChunkData = serializedChunk.chunkData.toByteArray();
          chunkLength = compressedChunkData.length;
          compressedChunkDataSize = chunkLength;
        }

        // Compute the md5 of the chunk data
        String md5 = computeMD5(compressedChunkData, chunkLength);

        // Create chunk metadata
        long startOffset = curDataSize;
        ChunkMetadata.Builder chunkMetadataBuilder =
            ChunkMetadata.builder()
                .setOwningTableFromChannelContext(firstChannelFlushContext)
                // The start offset will be updated later in BlobBuilder#build to include the blob
                // header
                .setChunkStartOffset(startOffset)
                // The chunkLength is used because it is the actual data size used for
                // decompression and md5 calculation on server side.
                .setChunkLength(chunkLength)
                .setUncompressedChunkLength((int) serializedChunk.chunkEstimatedUncompressedSize)
                .setChannelList(serializedChunk.channelsMetadataList)
                .setChunkMD5(md5)
                .setEncryptionKeyId(encryptionKey.getEncryptionKeyId())
                .setEpInfo(
                    AbstractRowBuffer.buildEpInfoFromStats(
                        serializedChunk.rowCount,
                        serializedChunk.columnEpStatsMapCombined,
                        internalParameterProvider.setAllDefaultValuesInEp(),
                        internalParameterProvider.isEnableDistinctValuesCount()))
                .setFirstInsertTimeInMs(serializedChunk.chunkMinMaxInsertTimeInMs.getFirst())
                .setLastInsertTimeInMs(serializedChunk.chunkMinMaxInsertTimeInMs.getSecond());

        if (internalParameterProvider.setIcebergSpecificFieldsInEp()) {
          if (internalParameterProvider.getEnableChunkEncryption()) {
            /* metadata size computation only works when encryption and padding is off */
            throw new SFException(
                ErrorCode.INTERNAL_ERROR,
                "Metadata size computation is only supported when encryption is enabled");
          }
          final long metadataSize = getParquetFooterSize(compressedChunkData);
          final long extendedMetadataSize = serializedChunk.extendedMetadataSize;
          chunkMetadataBuilder
              .setMajorVersion(Constants.PARQUET_MAJOR_VERSION)
              .setMinorVersion(Constants.PARQUET_MINOR_VERSION)
              // set createdOn in seconds
              .setCreatedOn(System.currentTimeMillis() / 1000)
              .setMetadataSize(metadataSize)
              .setExtendedMetadataSize(extendedMetadataSize);
        }

        ChunkMetadata chunkMetadata = chunkMetadataBuilder.build();

        // Add chunk metadata and data to the list
        chunksMetadataList.add(chunkMetadata);
        chunksDataList.add(compressedChunkData);
        curDataSize += compressedChunkDataSize;
        crc.update(compressedChunkData, 0, compressedChunkDataSize);

        logger.logInfo(
            "Finish building chunk in blob={}, table={}, rowCount={}, startOffset={},"
                + " estimatedUncompressedSize={}, md5={}, chunkLength={}, compressedSize={},"
                + " encrypt={}, bdecVersion={}",
            filePath,
            firstChannelFlushContext.getFullyQualifiedTableName(),
            serializedChunk.rowCount,
            startOffset,
            serializedChunk.chunkEstimatedUncompressedSize,
            md5,
            chunkLength,
            compressedChunkDataSize,
            internalParameterProvider.getEnableChunkEncryption(),
            bdecVersion);
      }
    }

    // Build blob file bytes
    byte[] blobBytes =
        buildBlob(chunksMetadataList, chunksDataList, crc.getValue(), curDataSize, bdecVersion);
    return new Blob(blobBytes, chunksMetadataList, new BlobStats());
  }

  /**
   * Pad the compressed data for encryption. Encryption needs padding to the
   * ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES to align with decryption on the Snowflake query path
   * starting from this chunk offset.
   *
   * @param chunkData uncompressed chunk data
   * @param blockSizeToAlignTo block size to align to for encryption
   * @return padded compressed chunk data, aligned to blockSizeToAlignTo, and actual length of
   *     compressed data before padding at the end
   * @throws IOException
   */
  static Pair<byte[], Integer> padChunk(ByteArrayOutputStream chunkData, int blockSizeToAlignTo)
      throws IOException {
    int actualSize = chunkData.size();
    int paddingSize = blockSizeToAlignTo - actualSize % blockSizeToAlignTo;
    chunkData.write(new byte[paddingSize]);
    return new Pair<>(chunkData.toByteArray(), actualSize);
  }

  /**
   * Build the blob file bytes
   *
   * @param chunksMetadataList List of chunk metadata
   * @param chunksDataList List of chunk data
   * @param chunksChecksum Checksum for the chunk data portion
   * @param chunksDataSize Total size for the chunk data portion after compression
   * @param bdecVersion BDEC file version
   * @return The blob file as a byte array
   * @throws JsonProcessingException
   */
  static byte[] buildBlob(
      List<ChunkMetadata> chunksMetadataList,
      List<byte[]> chunksDataList,
      long chunksChecksum,
      long chunksDataSize,
      Constants.BdecVersion bdecVersion)
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
    for (byte[] chunkData : chunksDataList) {
      blob.write(chunkData);
    }

    // We need to update the start offset for the EP and Parquet data in the request since
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
    return Hex.encodeHexString(digest);
  }

  /** Blob data to store in a file and register by server side. */
  static class Blob {
    final byte[] blobBytes;
    final List<ChunkMetadata> chunksMetadataList;
    final BlobStats blobStats;

    Blob(byte[] blobBytes, List<ChunkMetadata> chunksMetadataList, BlobStats blobStats) {
      this.blobBytes = blobBytes;
      this.chunksMetadataList = chunksMetadataList;
      this.blobStats = blobStats;
    }
  }
}

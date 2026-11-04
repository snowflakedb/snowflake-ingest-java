/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeStreamingIngestRetryTest {
  private final SnowflakeStreamingIngestClientInternal<?> client =
      Mockito.mock(SnowflakeStreamingIngestClientInternal.class, Mockito.CALLS_REAL_METHODS);

  private static ChunkMetadata chunk(String db, String schema, String table, long rowSequencer) {
    ChannelFlushContext context =
        new ChannelFlushContext("same-channel", db, schema, table, 7L, "key", 1L);
    ChannelMetadata channel =
        ChannelMetadata.builder()
            .setOwningChannelFromContext(context)
            .setRowSequencer(rowSequencer)
            .setOffsetToken("offset-" + rowSequencer)
            .build();
    return ChunkMetadata.builder()
        .setOwningTableFromChannelContext(context)
        .setChunkStartOffset(rowSequencer * 100)
        .setChunkLength(100)
        .setUncompressedChunkLength(200)
        .setChannelList(Collections.singletonList(channel))
        .setChunkMD5("md5-" + rowSequencer)
        .setEncryptionKeyId(1L)
        .setEpInfo(new EpInfo())
        .setFirstInsertTimeInMs(1L)
        .setLastInsertTimeInMs(2L)
        .build();
  }

  private static BlobMetadata blob(String path, ChunkMetadata... chunks) {
    BlobStats stats = new BlobStats();
    stats.setFlushStartMs(1000L);
    return BlobMetadata.createBlobMetadata(
        path,
        "blob-md5",
        Constants.BdecVersion.THREE,
        Arrays.asList(chunks),
        stats,
        chunks.length > 1);
  }

  static ChunkRegisterStatus status(ChunkMetadata chunk, long code) {
    ChunkRegisterStatus status = new ChunkRegisterStatus();
    status.setDBName(chunk.getDBName());
    status.setSchemaName(chunk.getSchemaName());
    status.setTableName(chunk.getTableName());
    List<ChannelRegisterStatus> channels = new ArrayList<>();
    for (ChannelMetadata metadata : chunk.getChannels()) {
      ChannelRegisterStatus channel = new ChannelRegisterStatus();
      channel.setChannelName(metadata.getChannelName());
      channel.setChannelSequencer(metadata.getClientSequencer());
      channel.setStatusCode(code);
      channels.add(channel);
    }
    status.setChannelsStatus(channels);
    return status;
  }

  static RegisterBlobResponse response(List<BlobMetadata> blobs) {
    RegisterBlobResponse response = new RegisterBlobResponse();
    response.setStatusCode(RESPONSE_SUCCESS);
    response.setMessage("success");
    response.setEncryptionKeys(Collections.emptyList());
    List<BlobRegisterStatus> statuses = new ArrayList<>();
    for (BlobMetadata blob : blobs) {
      BlobRegisterStatus blobStatus = new BlobRegisterStatus();
      List<ChunkRegisterStatus> chunks = new ArrayList<>();
      for (ChunkMetadata chunk : blob.getChunks()) chunks.add(status(chunk, RESPONSE_SUCCESS));
      blobStatus.setChunksStatus(chunks);
      statuses.add(blobStatus);
    }
    response.setBlobsStatus(statuses);
    return response;
  }

  private void assertOnlyFailedChunkRetried(ChunkMetadata successful) {
    ChunkMetadata failed = chunk("DB", "SCHEMA", "TABLE", 2L);
    BlobMetadata original = blob("path", successful, failed);
    List<BlobMetadata> blobs = Collections.singletonList(original);
    RegisterBlobResponse response = response(blobs);
    ChunkRegisterStatus failure = response.getBlobsStatus().get(0).getChunksStatus().get(1);
    failure.getChannelsStatus().get(0).setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);
    List<BlobMetadata> retry =
        client.getRetryBlobs(Collections.singleton(failure), response, blobs);
    assertEquals(1, retry.size());
    assertEquals(Collections.singletonList(failed), retry.get(0).getChunks());
    assertSame(failed, retry.get(0).getChunks().get(0));
    assertEquals(original.getPath(), retry.get(0).getPath());
    assertEquals(original.getMD5(), retry.get(0).getMD5());
    assertEquals(original.getVersion(), retry.get(0).getVersion());
    assertSame(original.getBlobStats(), retry.get(0).getBlobStats());
    assertTrue(retry.get(0).getSpansMixedTables());
    assertEquals(Long.valueOf(200), failed.getChunkStartOffset());
  }

  @Test
  public void testSuccessfulOtherTableIsNotRetried() {
    assertOnlyFailedChunkRetried(chunk("DB", "SCHEMA", "OTHER_TABLE", 1L));
  }

  @Test
  public void testSuccessfulOtherDatabaseIsNotRetried() {
    assertOnlyFailedChunkRetried(chunk("OTHER_DB", "SCHEMA", "TABLE", 1L));
  }

  @Test
  public void testSuccessfulOtherSchemaIsNotRetried() {
    assertOnlyFailedChunkRetried(chunk("DB", "OTHER_SCHEMA", "TABLE", 1L));
  }

  @Test
  public void testSuccessfulEarlierBlobOfSameChannelIsNotRetried() {
    ChunkMetadata successful = chunk("DB", "SCHEMA", "TABLE", 1L);
    ChunkMetadata failed = chunk("DB", "SCHEMA", "TABLE", 2L);
    List<BlobMetadata> blobs = Arrays.asList(blob("earlier", successful), blob("later", failed));
    RegisterBlobResponse response = response(blobs);
    ChunkRegisterStatus failure = response.getBlobsStatus().get(1).getChunksStatus().get(0);
    failure.getChannelsStatus().get(0).setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);
    List<BlobMetadata> retry =
        client.getRetryBlobs(Collections.singleton(failure), response, blobs);
    assertEquals(1, retry.size());
    assertEquals("later", retry.get(0).getPath());
    assertEquals(Collections.singletonList(failed), retry.get(0).getChunks());
  }

  @Test
  public void testRepeatedChannelInSameBlobSelectsOnlyItsFailedChunk() {
    assertOnlyFailedChunkRetried(chunk("DB", "SCHEMA", "TABLE", 1L));
  }

  @Test
  public void testOnlyAlreadyClassifiedRetryableChunksAreSelected() {
    List<BlobMetadata> blobs =
        Collections.singletonList(
            blob("path", chunk("DB", "SCHEMA", "TABLE", 1L), chunk("DB", "SCHEMA", "TABLE", 2L)));
    RegisterBlobResponse response = response(blobs);
    List<ChunkRegisterStatus> statuses = response.getBlobsStatus().get(0).getChunksStatus();
    statuses
        .get(0)
        .getChannelsStatus()
        .get(0)
        .setStatusCode(Constants.RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL);
    statuses
        .get(1)
        .getChannelsStatus()
        .get(0)
        .setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);
    // Queue-full retries have been exhausted; the generic retry is still eligible.
    List<BlobMetadata> retry =
        client.getRetryBlobs(Collections.singleton(statuses.get(1)), response, blobs);
    assertEquals(
        Collections.singletonList(blobs.get(0).getChunks().get(1)), retry.get(0).getChunks());
  }

  @Test
  public void testAllSuccessHasNoRetry() {
    List<BlobMetadata> blobs =
        Collections.singletonList(blob("path", chunk("DB", "SCHEMA", "TABLE", 1L)));
    assertTrue(client.getRetryBlobs(Collections.emptySet(), response(blobs), blobs).isEmpty());
  }

  @Test
  public void testSelectedMultiChannelChunkIsRetainedWhole() {
    ChunkMetadata first = chunk("DB", "SCHEMA", "TABLE", 1L);
    ChannelMetadata second =
        ChannelMetadata.builder()
            .setOwningChannelFromContext(
                new ChannelFlushContext("other-channel", "DB", "SCHEMA", "TABLE", 8L, "key", 1L))
            .setRowSequencer(1L)
            .build();
    ChunkMetadata multi =
        ChunkMetadata.builder()
            .setOwningTableFromChannelContext(
                new ChannelFlushContext("same-channel", "DB", "SCHEMA", "TABLE", 7L, "key", 1L))
            .setChunkStartOffset(0L)
            .setChunkLength(100)
            .setUncompressedChunkLength(200)
            .setChannelList(Arrays.asList(first.getChannels().get(0), second))
            .setChunkMD5("multi")
            .setEncryptionKeyId(1L)
            .setEpInfo(new EpInfo())
            .setFirstInsertTimeInMs(1L)
            .setLastInsertTimeInMs(2L)
            .build();
    List<BlobMetadata> blobs = Collections.singletonList(blob("path", multi));
    RegisterBlobResponse response = response(blobs);
    ChunkRegisterStatus failure = response.getBlobsStatus().get(0).getChunksStatus().get(0);
    for (ChannelRegisterStatus channel : failure.getChannelsStatus())
      channel.setStatusCode(Constants.RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL);
    List<BlobMetadata> retry =
        client.getRetryBlobs(Collections.singleton(failure), response, blobs);
    assertSame(multi, retry.get(0).getChunks().get(0));
    assertEquals(2, retry.get(0).getChunks().get(0).getChannels().size());
  }

  private void assertMalformedResponse(RegisterBlobResponse response, List<BlobMetadata> blobs) {
    try {
      client.getRetryBlobs(Collections.emptySet(), response, blobs);
      fail("Malformed registration response accepted");
    } catch (SFException expected) {
      assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), expected.getVendorCode());
    }
  }

  @Test
  public void testMismatchedBlobCountRejected() {
    List<BlobMetadata> blobs =
        Collections.singletonList(blob("path", chunk("DB", "SCHEMA", "TABLE", 1L)));
    assertMalformedResponse(response(Collections.emptyList()), blobs);
  }

  @Test
  public void testMismatchedChunkCountRejected() {
    List<BlobMetadata> blobs =
        Collections.singletonList(blob("path", chunk("DB", "SCHEMA", "TABLE", 1L)));
    RegisterBlobResponse response = response(blobs);
    response.getBlobsStatus().get(0).setChunksStatus(Collections.emptyList());
    assertMalformedResponse(response, blobs);
  }

  @Test
  public void testMismatchedTableRejected() {
    List<BlobMetadata> blobs =
        Collections.singletonList(blob("path", chunk("DB", "SCHEMA", "TABLE", 1L)));
    RegisterBlobResponse response = response(blobs);
    response.getBlobsStatus().get(0).getChunksStatus().get(0).setTableName("WRONG_TABLE");
    assertMalformedResponse(response, blobs);
  }

  @Test
  public void testMismatchedChannelCountRejected() {
    List<BlobMetadata> blobs =
        Collections.singletonList(blob("path", chunk("DB", "SCHEMA", "TABLE", 1L)));
    RegisterBlobResponse response = response(blobs);
    response
        .getBlobsStatus()
        .get(0)
        .getChunksStatus()
        .get(0)
        .setChannelsStatus(Collections.emptyList());
    assertMalformedResponse(response, blobs);
  }
}

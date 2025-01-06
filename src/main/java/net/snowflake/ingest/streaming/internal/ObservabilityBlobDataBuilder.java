package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.ObservabilityClusteringKey;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.ParameterProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.snowflake.ingest.utils.Constants.MAX_BLOB_SIZE_IN_BYTES;

/**
 * Responsible for accepting data from channels and collating into collections that will be used to build the actual blobs
 * <p>
 * A chunk is represented as a list of channel data from a single table
 * A blob is represented as a list of chunks that must share the same schema (but not necessarily the same table)
 * <p>
 * This class returns a list of blobs
 */
class ObservabilityBlobDataBuilder {
  private static final Logging logger = new Logging(BlobDataBuilder.class);
  private final List<List<List<ChannelData<ParquetChunkData>>>> allBlobs;
  private final ParameterProvider parameterProvider;
  private final String clientName;
  private List<List<ChannelData<ParquetChunkData>>> currentBlob;
  private ChannelData<ParquetChunkData> prevChannelData = null;
  private ObservabilityClusteringKey prevClusteringKey = null;
  private float totalCurrentBlobSizeInBytes = 0F;
  private float totalBufferSizeInBytes = 0F;

  public ObservabilityBlobDataBuilder(String clientName, ParameterProvider parameterProvider) {
    this.clientName = clientName;
    this.parameterProvider = parameterProvider;
    this.currentBlob = new ArrayList<>();
    this.allBlobs = new ArrayList<>();
  }

  public List<List<List<ChannelData<ParquetChunkData>>>> getAllBlobData() {
    addCurrentBlob();
    return allBlobs;
  }

  public void appendDataForTable(Collection<? extends SnowflakeStreamingIngestChannelFlushable<ParquetChunkData>> tableChannels) {
    List<ChannelData<ParquetChunkData>> chunk = getChunkForTable(tableChannels);
    appendChunk(chunk);
  }

  private List<ChannelData<ParquetChunkData>> getChunkForTable(Collection<? extends SnowflakeStreamingIngestChannelFlushable<ParquetChunkData>> tableChannels) {
    List<ChannelData<ParquetChunkData>> channelsDataPerTable = Collections.synchronizedList(new ArrayList<>());
    // Use parallel stream since getData could be the performance bottleneck when we have a
    // high number of channels
    tableChannels.parallelStream()
        .forEach(
            channel -> {
              if (channel.isValid()) {
                channelsDataPerTable.addAll(
                    channel.getData().stream().filter(Objects::nonNull).collect(Collectors.toList())
                );
              }
            });
    return channelsDataPerTable;
  }

  private void appendChunk(List<ChannelData<ParquetChunkData>> chunkData) {
    if (chunkData.isEmpty()) {
      return;
    }

    if (currentBlob.size() >= parameterProvider.getMaxChunksInBlob()) {
      // Create a new blob if the current one already contains max allowed number of chunks
      logger.logInfo(
          "Max allowed number of chunks in the current blob reached. chunkCount={}"
              + " maxChunkCount={}",
          currentBlob.size(),
          parameterProvider.getMaxChunksInBlob());

      addCurrentBlob();
    }

    int i, start = 0;
    for (i = 0; i < chunkData.size(); i++) {
      ChannelData<ParquetChunkData> channelData = chunkData.get(i);
      ObservabilityClusteringKey currentClusteringKey = createFromChannelData(channelData);

      if (prevChannelData != null && shouldStopProcessing(
          totalCurrentBlobSizeInBytes,
          totalBufferSizeInBytes,
          parameterProvider.getMaxChunkSizeInBytes(),
          currentClusteringKey,
          prevClusteringKey,
          channelData,
          prevChannelData)) {
        logger.logInfo(
            "Creation of another blob is needed because of blob/chunk size limit or"
                + " different encryption ids or different schema, client={}, table={},"
                + " blobSize={}, chunkSize={}, nextChannelSize={}, encryptionId1={},"
                + " encryptionId2={}, schema1={}, schema2={}",
            clientName,
            channelData.getChannelContext().getTableName(),
            totalCurrentBlobSizeInBytes,
            totalBufferSizeInBytes,
            channelData.getBufferSize(),
            channelData.getChannelContext().getEncryptionKeyId(),
            prevChannelData.getChannelContext().getEncryptionKeyId(),
            channelData.getColumnEps().keySet(),
            prevChannelData.getColumnEps().keySet());

        if (i != start) {
          currentBlob.add(chunkData.subList(start, i));
          start = i;
        }

        addCurrentBlob();
      }

      totalCurrentBlobSizeInBytes += channelData.getBufferSize();
      totalBufferSizeInBytes += channelData.getBufferSize();
      prevChannelData = channelData;
      prevClusteringKey = currentClusteringKey;
    }

    if (i != start) {
      currentBlob.add(chunkData.subList(start, i));
    }
  }

  private ObservabilityClusteringKey createFromChannelData(ChannelData<ParquetChunkData> channelData) {
    if (channelData instanceof ObservabilityChannelData) {
      return ((ObservabilityChannelData)channelData).getClusteringKey();
    }
    return null;
  }

  private void addCurrentBlob() {
    if (!currentBlob.isEmpty()) {
      allBlobs.add(currentBlob);
      currentBlob = new ArrayList<>();
    }
    totalBufferSizeInBytes = 0;
    totalCurrentBlobSizeInBytes = 0;
  }

  /**
   * Check whether we should stop merging more channels into the same chunk, we need to stop in a
   * few cases:
   *
   * <p>When the blob size is larger than a certain threshold
   *
   * <p>When the chunk size is larger than a certain threshold
   *
   * <p>When the schemas are not the same
   */
  private static boolean shouldStopProcessing(
      float totalBufferSizeInBytes,
      float totalBufferSizePerTableInBytes,
      float maxChunkSizeInBytes,
      ObservabilityClusteringKey currentKey,
      ObservabilityClusteringKey prevKey,
      ChannelData<ParquetChunkData> current,
      ChannelData<ParquetChunkData> prev) {

    return totalBufferSizeInBytes + current.getBufferSize() > MAX_BLOB_SIZE_IN_BYTES
        || totalBufferSizePerTableInBytes + current.getBufferSize() > maxChunkSizeInBytes
        || !Objects.equals(
        current.getChannelContext().getEncryptionKeyId(),
        prev.getChannelContext().getEncryptionKeyId())
        || !current.getColumnEps().keySet().equals(prev.getColumnEps().keySet());
  }
}

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.ObservabilityClusteringKey;

public class ObservabilityChannelData extends ChannelData<ParquetChunkData> {
  private ObservabilityClusteringKey clusteringKey;

  public ObservabilityClusteringKey getClusteringKey() {
    return clusteringKey;
  }

  public void setClusteringKey(ObservabilityClusteringKey clusteringKey) {
    this.clusteringKey = clusteringKey;
  }
}

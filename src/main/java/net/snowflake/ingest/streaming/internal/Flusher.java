package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Flusher {
    SerializationResult serialize(List<ChannelData> channelsDataPerTable, ByteArrayOutputStream chunkData, String filePath) throws IOException;

    class SerializationResult {
        final List<ChannelMetadata> channelsMetadataList;
        final Map<String, RowBufferStats> columnEpStatsMapCombined;
        final long rowCount;

        public SerializationResult(List<ChannelMetadata> channelsMetadataList, Map<String, RowBufferStats> columnEpStatsMapCombined, long rowCount) {
            this.channelsMetadataList = channelsMetadataList;
            this.columnEpStatsMapCombined = columnEpStatsMapCombined;
            this.rowCount = rowCount;
        }
    }
}

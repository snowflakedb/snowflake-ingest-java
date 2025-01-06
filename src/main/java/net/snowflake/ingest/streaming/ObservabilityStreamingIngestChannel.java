package net.snowflake.ingest.streaming;

import net.snowflake.ingest.streaming.ObservabilityClusteringKey;

import javax.annotation.Nullable;
import java.util.Map;

public interface ObservabilityStreamingIngestChannel {
    InsertValidationResponse insertRows(
            ObservabilityClusteringKey clusteringKey,
            Iterable<Map<String, Object>> rows,
            @Nullable String startOffsetToken,
            @Nullable String endOffsetToken);

    InsertValidationResponse insertRows(
            ObservabilityClusteringKey clusteringKey,
            Iterable<Map<String, Object>> rows,
            @Nullable String offsetToken);
}

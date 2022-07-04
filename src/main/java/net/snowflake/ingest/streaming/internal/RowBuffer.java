package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.InsertValidationResponse;

import java.util.List;
import java.util.Map;

public interface RowBuffer<T> {
    void setupSchema(List<ColumnMetadata> columns);
    InsertValidationResponse insertRows(Iterable<Map<String, Object>> rows, String offsetToken);
    ChannelData<T> flush();
    void close(String name);
    float getSize();
}

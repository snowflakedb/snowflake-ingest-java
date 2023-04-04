package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.*;

public class ParquetWriterTest {
    @Test
    public void test() throws IOException {
        ParquetRowBuffer buffer = new ParquetRowBuffer(
                OpenChannelRequest.OnErrorOption.ABORT,
                ZoneId.systemDefault(),
                new RootAllocator(),
                "parquet_test_channel",
                m -> {},
                new ChannelRuntimeState(0, 0, true),
                false,
                ParquetRowBuffer.BufferingType.PARQUET_BUFFERS);

        ColumnMetadata testCol =
                ColumnMetadataBuilder.newBuilder()
                        .logicalType("TEXT")
                        .physicalType("LOB")
                        .nullable(true)
                        .length(56)
                        .build();

        buffer.setupSchema(Collections.singletonList(testCol));

        for (int val = 0; val < 1000; val++) {
            Map<String, Object> row = new HashMap<>();
            row.put("c1", Integer.toString(val));
            InsertValidationResponse response = buffer.insertRows(Collections.singletonList(row), Integer.toString(val));
            if (response.hasErrors()) {
                throw response.getInsertErrors().get(0).getException();
            }
        }

        ChannelData<ParquetChunkData> data = buffer.flush("filePath");
        Flusher.SerializationResult result = buffer.createFlusher().serialize(Collections.singletonList(data), "filePath");

    }

}

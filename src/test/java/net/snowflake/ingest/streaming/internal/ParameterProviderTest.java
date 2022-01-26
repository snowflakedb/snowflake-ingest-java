package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ParameterProviderTest {

  @Test
  public void withValuesSet() {
    ParameterProvider parameterProvider = new ParameterProvider();

    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(parameterProvider.MAX_CHUNK_SIZE_IN_BYTES_MAP_KEY, 1l);
    parameterMap.put(parameterProvider.MAX_BLOB_SIZE_IN_BYTES_MAP_KEY, 2l);
    parameterMap.put(parameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY, 3l);
    parameterMap.put(parameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY, 4l);
    parameterMap.put(parameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY, 6l);
    parameterMap.put(parameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_MAP_KEY, 7l);
    parameterProvider.setParameterMap(parameterMap);

    Assert.assertEquals(1, parameterProvider.getMaxChunkSizeInBytes());
    Assert.assertEquals(2, parameterProvider.getMaxBlobSizeInBytes());
    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(7, parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withDefaultValues() {
    ParameterProvider parameterProvider = new ParameterProvider();

    Assert.assertEquals(
        parameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT,
        parameterProvider.getMaxChunkSizeInBytes());
    Assert.assertEquals(
        parameterProvider.MAX_BLOB_SIZE_IN_BYTES_DEFAULT,
        parameterProvider.getMaxBlobSizeInBytes());
    Assert.assertEquals(
        parameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(
        parameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(
        parameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT,
        parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        parameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }
}

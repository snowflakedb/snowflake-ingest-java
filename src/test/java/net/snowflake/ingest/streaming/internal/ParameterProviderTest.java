package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.utils.ParameterProvider;
import org.junit.Assert;
import org.junit.Test;

public class ParameterProviderTest {

  @Test
  public void withValuesSet() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, 3L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS, 7L);
    parameterMap.put(ParameterProvider.IO_TIME_CPU_RATIO, 10);
    parameterMap.put(ParameterProvider.BLOB_UPLOAD_MAX_RETRY_COUNT, 100);
    parameterMap.put(ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES, 1000L);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);

    Assert.assertEquals(3L, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4L, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(7L, parameterProvider.getInsertThrottleIntervalInMs());
    Assert.assertEquals(10, parameterProvider.getIOTimeCpuRatio());
    Assert.assertEquals(100, parameterProvider.getBlobUploadMaxRetryCount());
    Assert.assertEquals(1000L, parameterProvider.getMaxMemoryLimitInBytes());
  }

  @Test
  public void withNullProps() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, 3L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, null);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withNullParameterMap() {
    Properties props = new Properties();
    props.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, 3L);
    props.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    props.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    ParameterProvider parameterProvider = new ParameterProvider(null, props);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withNullInputs() {
    ParameterProvider parameterProvider = new ParameterProvider(null, null);

    Assert.assertEquals(
        ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(
        ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT,
        parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withDefaultValues() {
    ParameterProvider parameterProvider = new ParameterProvider();

    Assert.assertEquals(
        ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(
        ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT,
        parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
    Assert.assertEquals(
        ParameterProvider.IO_TIME_CPU_RATIO_DEFAULT, parameterProvider.getIOTimeCpuRatio());
    Assert.assertEquals(
        ParameterProvider.BLOB_UPLOAD_MAX_RETRY_COUNT_DEFAULT,
        parameterProvider.getBlobUploadMaxRetryCount());
    Assert.assertEquals(
        ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT,
        parameterProvider.getMaxMemoryLimitInBytes());
  }
}

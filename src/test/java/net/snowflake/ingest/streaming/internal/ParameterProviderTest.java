package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class ParameterProviderTest {

  private Map<String, Object> getStartingParameterMap() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, 3L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS, 7L);
    parameterMap.put(ParameterProvider.IO_TIME_CPU_RATIO, 10);
    parameterMap.put(ParameterProvider.BLOB_UPLOAD_MAX_RETRY_COUNT, 100);
    parameterMap.put(ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES, 1000L);
    parameterMap.put(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, 1000000L);
    return parameterMap;
  }

  @Test
  public void withValuesSet() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, false);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);

    Assert.assertEquals(3L, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4L, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(1024, parameterProvider.getInsertThrottleThresholdInBytes());
    Assert.assertEquals(7L, parameterProvider.getInsertThrottleIntervalInMs());
    Assert.assertEquals(10, parameterProvider.getIOTimeCpuRatio());
    Assert.assertEquals(100, parameterProvider.getBlobUploadMaxRetryCount());
    Assert.assertEquals(1000L, parameterProvider.getMaxMemoryLimitInBytes());
    Assert.assertEquals(1000000L, parameterProvider.getMaxChannelSizeInBytes());
  }

  @Test
  public void withNullProps() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, 3L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, false);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, null);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(1024, parameterProvider.getInsertThrottleThresholdInBytes());
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
    props.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
    props.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, false);
    ParameterProvider parameterProvider = new ParameterProvider(null, props);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(1024, parameterProvider.getInsertThrottleThresholdInBytes());
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
        ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES_DEFAULT,
        parameterProvider.getInsertThrottleThresholdInBytes());
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
        ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES_DEFAULT,
        parameterProvider.getInsertThrottleThresholdInBytes());
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
    Assert.assertEquals(
        ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES_DEFAULT,
        parameterProvider.getMaxChannelSizeInBytes());
  }

  @Test
  public void testMaxClientLagEnabled() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "2 second");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    Assert.assertEquals(2000, parameterProvider.getBufferFlushIntervalInMs());
    // call again to trigger caching logic
    Assert.assertEquals(2000, parameterProvider.getBufferFlushIntervalInMs());
  }

  @Test
  public void testMaxClientLagEnabledPluralTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "2 seconds");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    Assert.assertEquals(2000, parameterProvider.getBufferFlushIntervalInMs());
  }

  @Test
  public void testMaxClientLagEnabledMinuteTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "1 minute");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    Assert.assertEquals(60000, parameterProvider.getBufferFlushIntervalInMs());
  }

  @Test
  public void testMaxClientLagEnabledMinuteTimeUnitPluralTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "2 minutes");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    Assert.assertEquals(120000, parameterProvider.getBufferFlushIntervalInMs());
  }

  @Test
  public void testMaxClientLagEnabledDefaultValue() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    Assert.assertEquals(1000, parameterProvider.getBufferFlushIntervalInMs());
  }

  @Test
  public void testMaxClientLagEnabledMissingUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "1");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    try {
      parameterProvider.getBufferFlushIntervalInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse"));
    }
  }

  @Test
  public void testMaxClientLagEnabledMissingUnitTimeUnitSupplied() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, " year");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    try {
      parameterProvider.getBufferFlushIntervalInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse"));
    }
  }

  @Test
  public void testMaxClientLagEnabledInvalidTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "1 year");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    try {
      parameterProvider.getBufferFlushIntervalInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Invalid time unit"));
    }
  }

  @Test
  public void testMaxClientLagEnabledInvalidUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "banana minute");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    try {
      parameterProvider.getBufferFlushIntervalInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse"));
    }
  }

  @Test
  public void testMaxClientLagEnabledThresholdBelow() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "0 second");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    try {
      parameterProvider.getBufferFlushIntervalInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Lag falls outside"));
    }
  }

  @Test
  public void testMaxClientLagEnabledThresholdAbove() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG_ENABLED, true);
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "11 minutes");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    try {
      parameterProvider.getBufferFlushIntervalInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Lag falls outside"));
    }
  }

  @Test
  public void testMaxChunksInBlobAndRegistrationRequest() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put("max_chunks_in_blob", 1);
    parameterMap.put("max_chunks_in_registration_request", 2);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);
    Assert.assertEquals(1, parameterProvider.getMaxChunksInBlob());
    Assert.assertEquals(2, parameterProvider.getMaxChunksInRegistrationRequest());
  }

  @Test
  public void testValidationMaxChunksInBlobAndRegistrationRequest() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put("max_chunks_in_blob", 2);
    parameterMap.put("max_chunks_in_registration_request", 1);
    try {
      new ParameterProvider(parameterMap, prop);
      Assert.fail("Should not have succeeded");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CONFIG_PARAMETER.getMessageCode(), e.getVendorCode());
    }
  }
}

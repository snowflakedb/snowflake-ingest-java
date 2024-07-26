/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.ParameterProvider.MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST_ICEBERG_MODE_DEFAULT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class ParameterProviderTest {

  private Map<String, Object> getStartingParameterMap() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, 1000L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS, 7L);
    parameterMap.put(ParameterProvider.IO_TIME_CPU_RATIO, 10);
    parameterMap.put(ParameterProvider.BLOB_UPLOAD_MAX_RETRY_COUNT, 100);
    parameterMap.put(ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES, 1000L);
    parameterMap.put(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, 1000000L);
    parameterMap.put(ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM, "gzip");
    return parameterMap;
  }

  @Test
  public void withValuesSet() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);

    Assert.assertEquals(1000L, parameterProvider.getCachedMaxClientLagInMs());
    Assert.assertEquals(4L, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(1024, parameterProvider.getInsertThrottleThresholdInBytes());
    Assert.assertEquals(7L, parameterProvider.getInsertThrottleIntervalInMs());
    Assert.assertEquals(10, parameterProvider.getIOTimeCpuRatio());
    Assert.assertEquals(100, parameterProvider.getBlobUploadMaxRetryCount());
    Assert.assertEquals(1000L, parameterProvider.getMaxMemoryLimitInBytes());
    Assert.assertEquals(1000000L, parameterProvider.getMaxChannelSizeInBytes());
    Assert.assertEquals(
        Constants.BdecParquetCompression.GZIP,
        parameterProvider.getBdecParquetCompressionAlgorithm());
  }

  @Test
  public void withNullProps() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, 3000L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, null, false);

    Assert.assertEquals(3000, parameterProvider.getCachedMaxClientLagInMs());
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
    props.put(ParameterProvider.MAX_CLIENT_LAG, 3000L);
    props.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 4L);
    props.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 6);
    props.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
    ParameterProvider parameterProvider = new ParameterProvider(null, props, false);

    Assert.assertEquals(3000, parameterProvider.getCachedMaxClientLagInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(1024, parameterProvider.getInsertThrottleThresholdInBytes());
    Assert.assertEquals(
        ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withNullInputs() {
    ParameterProvider parameterProvider = new ParameterProvider(null, null, false);

    Assert.assertEquals(
        ParameterProvider.MAX_CLIENT_LAG_DEFAULT, parameterProvider.getCachedMaxClientLagInMs());
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
    ParameterProvider parameterProvider = new ParameterProvider(false);

    Assert.assertEquals(
        ParameterProvider.MAX_CLIENT_LAG_DEFAULT, parameterProvider.getCachedMaxClientLagInMs());
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
    Assert.assertEquals(
        ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM_DEFAULT,
        parameterProvider.getBdecParquetCompressionAlgorithm());
  }

  @Test
  public void withIcebergDefaultValues() {
    ParameterProvider parameterProvider = new ParameterProvider(true);

    Assert.assertEquals(
        ParameterProvider.MAX_CLIENT_LAG_ICEBERG_MODE_DEFAULT,
        parameterProvider.getCachedMaxClientLagInMs());
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
    Assert.assertEquals(
        ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM_DEFAULT,
        parameterProvider.getBdecParquetCompressionAlgorithm());
    Assert.assertEquals(
        MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST_ICEBERG_MODE_DEFAULT,
        parameterProvider.getMaxChunksInBlobAndRegistrationRequest());
  }

  @Test
  public void testMaxClientLagEnabled() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "2 second");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(2000, parameterProvider.getCachedMaxClientLagInMs());
    // call again to trigger caching logic
    Assert.assertEquals(2000, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledPluralTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "2 seconds");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(2000, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledMinuteTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "1 minute");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(60000, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledMinuteTimeUnitPluralTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "2 minutes");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(120000, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledDefaultValue() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(
        ParameterProvider.MAX_CLIENT_LAG_DEFAULT, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledDefaultUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "3000");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(3000, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledLongInput() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, 3000L);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(3000, parameterProvider.getCachedMaxClientLagInMs());
  }

  @Test
  public void testMaxClientLagEnabledMissingUnitTimeUnitSupplied() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, " year");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getCachedMaxClientLagInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse"));
    }
  }

  @Test
  public void testMaxClientLagEnabledInvalidTimeUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "1 year");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getCachedMaxClientLagInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Invalid time unit"));
    }
  }

  @Test
  public void testMaxClientLagEnabledInvalidUnit() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "banana minute");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getCachedMaxClientLagInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse"));
    }
  }

  @Test
  public void testMaxClientLagEnabledThresholdBelow() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "0 second");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getCachedMaxClientLagInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Lag falls outside"));
    }
  }

  @Test
  public void testMaxClientLagEnabledThresholdAbove() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "11 minutes");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getCachedMaxClientLagInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Lag falls outside"));
    }
  }

  @Test
  public void testMaxClientLagEnableEmptyInput() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getCachedMaxClientLagInMs();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getCause().getClass(), NumberFormatException.class);
    }
  }

  @Test
  public void testMaxChunksInBlobAndRegistrationRequest() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put("max_chunks_in_blob_and_registration_request", 1);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    Assert.assertEquals(1, parameterProvider.getMaxChunksInBlobAndRegistrationRequest());

    parameterProvider = new ParameterProvider(parameterMap, prop, true);
    Assert.assertEquals(1, parameterProvider.getMaxChunksInBlobAndRegistrationRequest());

    SFException e =
        Assert.assertThrows(
            SFException.class,
            () -> {
              parameterMap.put("max_chunks_in_blob_and_registration_request", 100);
              new ParameterProvider(parameterMap, prop, true);
            });
    Assert.assertEquals(e.getVendorCode(), ErrorCode.INVALID_CONFIG_PARAMETER.getMessageCode());
  }

  @Test
  public void testValidCompressionAlgorithmsAndWithUppercaseLowerCase() {
    List<String> gzipValues = Arrays.asList("GZIP", "gzip", "Gzip", "gZip");
    gzipValues.forEach(
        v -> {
          Properties prop = new Properties();
          Map<String, Object> parameterMap = getStartingParameterMap();
          parameterMap.put(ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM, v);
          ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
          Assert.assertEquals(
              Constants.BdecParquetCompression.GZIP,
              parameterProvider.getBdecParquetCompressionAlgorithm());
        });
    List<String> zstdValues = Arrays.asList("ZSTD", "zstd", "Zstd", "zStd");
    zstdValues.forEach(
        v -> {
          Properties prop = new Properties();
          Map<String, Object> parameterMap = getStartingParameterMap();
          parameterMap.put(ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM, v);
          ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
          Assert.assertEquals(
              Constants.BdecParquetCompression.ZSTD,
              parameterProvider.getBdecParquetCompressionAlgorithm());
        });
  }

  @Test
  public void testInvalidCompressionAlgorithm() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = getStartingParameterMap();
    parameterMap.put(ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM, "invalid_comp");
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop, false);
    try {
      parameterProvider.getBdecParquetCompressionAlgorithm();
      Assert.fail("Should not have succeeded");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(
          "Unsupported BDEC_PARQUET_COMPRESSION_ALGORITHM = 'invalid_comp', allowed values are"
              + " [GZIP, ZSTD]",
          e.getMessage());
    }
  }
}

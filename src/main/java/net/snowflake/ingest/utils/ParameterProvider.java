package net.snowflake.ingest.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Utility class to provide configurable constants */
public class ParameterProvider {
  public static final String BUFFER_FLUSH_INTERVAL_IN_MILLIS =
      "STREAMING_INGEST_CLIENT_SDK_BUFFER_FLUSH_INTERVAL_IN_MILLIS".toLowerCase();
  public static final String BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS =
      "STREAMING_INGEST_CLIENT_SDK_BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS".toLowerCase();
  public static final String INSERT_THROTTLE_INTERVAL_IN_MILLIS =
      "STREAMING_INGEST_CLIENT_SDK_INSERT_THROTTLE_INTERVAL_IN_MILLIS".toLowerCase();
  public static final String INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE =
      "STREAMING_INGEST_CLIENT_SDK_INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE".toLowerCase();
  public static final String INSERT_THROTTLE_THRESHOLD_IN_BYTES =
      "STREAMING_INGEST_CLIENT_SDK_INSERT_THROTTLE_THRESHOLD_IN_BYTES".toLowerCase();
  public static final String ENABLE_SNOWPIPE_STREAMING_METRICS =
      "ENABLE_SNOWPIPE_STREAMING_JMX_METRICS".toLowerCase();
  public static final String BLOB_FORMAT_VERSION = "BLOB_FORMAT_VERSION".toLowerCase();
  public static final String IO_TIME_CPU_RATIO = "IO_TIME_CPU_RATIO".toLowerCase();
  public static final String BLOB_UPLOAD_MAX_RETRY_COUNT =
      "BLOB_UPLOAD_MAX_RETRY_COUNT".toLowerCase();
  public static final String MAX_MEMORY_LIMIT_IN_BYTES = "MAX_MEMORY_LIMIT_IN_BYTES".toLowerCase();
  public static final String ENABLE_PARQUET_INTERNAL_BUFFERING =
      "ENABLE_PARQUET_INTERNAL_BUFFERING".toLowerCase();
  // This should not be needed once we have the ability to track size at table/chunk level
  public static final String MAX_CHANNEL_SIZE_IN_BYTES = "MAX_CHANNEL_SIZE_IN_BYTES".toLowerCase();
  public static final String MAX_CHUNK_SIZE_IN_BYTES = "MAX_CHUNK_SIZE_IN_BYTES".toLowerCase();
  public static final String MAX_ALLOWED_ROW_SIZE_IN_BYTES =
      "MAX_ALLOWED_ROW_SIZE_IN_BYTES".toLowerCase();
  public static final String MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST =
      "MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST".toLowerCase();

  public static final String MAX_CLIENT_LAG = "MAX_CLIENT_LAG".toLowerCase();

  public static final String BDEC_PARQUET_COMPRESSION_ALGORITHM =
      "BDEC_PARQUET_COMPRESSION_ALGORITHM".toLowerCase();

  // Default values
  public static final long BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT = 100;
  public static final long INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT = 1000;
  public static final int INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT = 10;
  public static final int INSERT_THROTTLE_THRESHOLD_IN_BYTES_DEFAULT = 200 * 1024 * 1024; // 200MB
  public static final boolean SNOWPIPE_STREAMING_METRICS_DEFAULT = false;
  public static final Constants.BdecVersion BLOB_FORMAT_VERSION_DEFAULT =
      Constants.BdecVersion.THREE;
  public static final int IO_TIME_CPU_RATIO_DEFAULT = 2;
  public static final int BLOB_UPLOAD_MAX_RETRY_COUNT_DEFAULT = 24;
  public static final long MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT = -1L;
  public static final long MAX_CHANNEL_SIZE_IN_BYTES_DEFAULT = 64 * 1024 * 1024;
  public static final long MAX_CHUNK_SIZE_IN_BYTES_DEFAULT = 256 * 1024 * 1024;

  // Lag related parameters
  public static final long MAX_CLIENT_LAG_DEFAULT = 1000; // 1 second
  static final long MAX_CLIENT_LAG_MS_MIN = TimeUnit.SECONDS.toMillis(1);
  static final long MAX_CLIENT_LAG_MS_MAX = TimeUnit.MINUTES.toMillis(10);

  public static final long MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT = 64 * 1024 * 1024; // 64 MB
  public static final int MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST_DEFAULT = 100;

  public static final Constants.BdecParquetCompression BDEC_PARQUET_COMPRESSION_ALGORITHM_DEFAULT =
      Constants.BdecParquetCompression.GZIP;

  /* Parameter that enables using internal Parquet buffers for buffering of rows before serializing.
  It reduces memory consumption compared to using Java Objects for buffering.*/
  public static final boolean ENABLE_PARQUET_INTERNAL_BUFFERING_DEFAULT = false;

  /** Map of parameter name to parameter value. This will be set by client/configure API Call. */
  private final Map<String, Object> parameterMap = new HashMap<>();

  // Cached buffer flush interval - avoid parsing each time for quick lookup
  private Long cachedBufferFlushIntervalMs = -1L;

  /**
   * Constructor. Takes properties from profile file and properties from client constructor and
   * resolves final parameter value
   *
   * @param parameterOverrides Map of parameter name to value
   * @param props Properties from profile file
   */
  public ParameterProvider(Map<String, Object> parameterOverrides, Properties props) {
    this.setParameterMap(parameterOverrides, props);
  }

  /** Empty constructor for tests */
  public ParameterProvider() {
    this(null, null);
  }

  private void updateValue(
      String key, Object defaultValue, Map<String, Object> parameterOverrides, Properties props) {
    if (parameterOverrides != null && props != null) {
      this.parameterMap.put(
          key, parameterOverrides.getOrDefault(key, props.getOrDefault(key, defaultValue)));
    } else if (parameterOverrides != null) {
      this.parameterMap.put(key, parameterOverrides.getOrDefault(key, defaultValue));
    } else if (props != null) {
      this.parameterMap.put(key, props.getOrDefault(key, defaultValue));
    }
  }

  /**
   * Sets parameter values by first checking 1. parameterOverrides 2. props 3. default value
   *
   * @param parameterOverrides Map<String, Object> of parameter name -> value
   * @param props Properties file provided to client constructor
   */
  private void setParameterMap(Map<String, Object> parameterOverrides, Properties props) {
    // BUFFER_FLUSH_INTERVAL_IN_MILLIS is deprecated and disallowed
    if ((parameterOverrides != null
            && parameterOverrides.containsKey(BUFFER_FLUSH_INTERVAL_IN_MILLIS))
        || (props != null && props.containsKey(BUFFER_FLUSH_INTERVAL_IN_MILLIS))) {
      throw new IllegalArgumentException(
          String.format(
              "%s is deprecated, please use %s instead",
              BUFFER_FLUSH_INTERVAL_IN_MILLIS, MAX_CLIENT_LAG));
    }

    this.updateValue(
        BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS,
        BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        INSERT_THROTTLE_INTERVAL_IN_MILLIS,
        INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE,
        INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        INSERT_THROTTLE_THRESHOLD_IN_BYTES,
        INSERT_THROTTLE_THRESHOLD_IN_BYTES_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        ENABLE_SNOWPIPE_STREAMING_METRICS,
        SNOWPIPE_STREAMING_METRICS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(BLOB_FORMAT_VERSION, BLOB_FORMAT_VERSION_DEFAULT, parameterOverrides, props);
    getBlobFormatVersion(); // to verify parsing the configured value

    this.updateValue(IO_TIME_CPU_RATIO, IO_TIME_CPU_RATIO_DEFAULT, parameterOverrides, props);

    this.updateValue(
        BLOB_UPLOAD_MAX_RETRY_COUNT,
        BLOB_UPLOAD_MAX_RETRY_COUNT_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        MAX_MEMORY_LIMIT_IN_BYTES, MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT, parameterOverrides, props);

    this.updateValue(
        ENABLE_PARQUET_INTERNAL_BUFFERING,
        ENABLE_PARQUET_INTERNAL_BUFFERING_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        MAX_CHANNEL_SIZE_IN_BYTES, MAX_CHANNEL_SIZE_IN_BYTES_DEFAULT, parameterOverrides, props);

    this.updateValue(
        MAX_CHUNK_SIZE_IN_BYTES, MAX_CHUNK_SIZE_IN_BYTES_DEFAULT, parameterOverrides, props);

    this.updateValue(MAX_CLIENT_LAG, MAX_CLIENT_LAG_DEFAULT, parameterOverrides, props);

    this.updateValue(
        MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST,
        MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        BDEC_PARQUET_COMPRESSION_ALGORITHM,
        BDEC_PARQUET_COMPRESSION_ALGORITHM_DEFAULT,
        parameterOverrides,
        props);
  }

  /** @return Longest interval in milliseconds between buffer flushes */
  public long getCachedMaxClientLagInMs() {
    if (cachedBufferFlushIntervalMs != -1L) {
      return cachedBufferFlushIntervalMs;
    }

    cachedBufferFlushIntervalMs = getMaxClientLagInMs();
    return cachedBufferFlushIntervalMs;
  }

  private long getMaxClientLagInMs() {
    Object val = this.parameterMap.getOrDefault(MAX_CLIENT_LAG, MAX_CLIENT_LAG_DEFAULT);
    long computedLag;
    if (val instanceof String) {
      String maxLag = (String) val;
      String[] lagParts = maxLag.split(" ");
      if (lagParts.length > 2) {
        throw new IllegalArgumentException(
            String.format("Failed to parse MAX_CLIENT_LAG = '%s'", maxLag));
      }

      // Compute the actual value
      try {
        computedLag = Long.parseLong(lagParts[0]);
      } catch (Throwable t) {
        throw new IllegalArgumentException(
            String.format("Failed to parse MAX_CLIENT_LAG = '%s'", lagParts[0]), t);
      }

      // Compute the time unit if needed
      if (lagParts.length == 2) {
        switch (lagParts[1].toLowerCase()) {
          case "second":
          case "seconds":
            computedLag = computedLag * TimeUnit.SECONDS.toMillis(1);
            break;
          case "minute":
          case "minutes":
            computedLag = computedLag * TimeUnit.SECONDS.toMillis(60);
            break;
          default:
            throw new IllegalArgumentException(
                String.format("Invalid time unit supplied = '%s", lagParts[1]));
        }
      }
    } else {
      computedLag = (long) val;
    }

    if (!(computedLag >= MAX_CLIENT_LAG_MS_MIN && computedLag <= MAX_CLIENT_LAG_MS_MAX)) {
      throw new IllegalArgumentException(
          String.format(
              "Lag falls outside of allowed time range. Minimum (milliseconds) = %s, Maximum"
                  + " (milliseconds) = %s",
              MAX_CLIENT_LAG_MS_MIN, MAX_CLIENT_LAG_MS_MAX));
    }

    return computedLag;
  }

  /** @return Time in milliseconds between checks to see if the buffer should be flushed */
  public long getBufferFlushCheckIntervalInMs() {
    Object val =
        this.parameterMap.getOrDefault(
            BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT);
    if (val instanceof String) {
      return Long.parseLong(val.toString());
    }
    return (long) val;
  }

  /** @return Duration in milliseconds to delay data insertion to the buffer when throttled */
  public long getInsertThrottleIntervalInMs() {
    Object val =
        this.parameterMap.getOrDefault(
            INSERT_THROTTLE_INTERVAL_IN_MILLIS, INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT);
    if (val instanceof String) {
      return Long.parseLong(val.toString());
    }
    return (long) val;
  }

  /** @return Percent of free total memory at which we throttle row inserts */
  public int getInsertThrottleThresholdInPercentage() {
    Object val =
        this.parameterMap.getOrDefault(
            INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE,
            INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT);
    if (val instanceof String) {
      return Integer.parseInt(val.toString());
    }
    return (int) val;
  }

  /** @return Absolute size in bytes of free total memory at which we throttle row inserts */
  public int getInsertThrottleThresholdInBytes() {
    Object val =
        this.parameterMap.getOrDefault(
            INSERT_THROTTLE_THRESHOLD_IN_BYTES, INSERT_THROTTLE_THRESHOLD_IN_BYTES_DEFAULT);
    if (val instanceof String) {
      return Integer.parseInt(val.toString());
    }
    return (int) val;
  }

  /** @return true if jmx metrics are enabled for a client */
  public boolean hasEnabledSnowpipeStreamingMetrics() {
    Object val =
        this.parameterMap.getOrDefault(
            ENABLE_SNOWPIPE_STREAMING_METRICS, SNOWPIPE_STREAMING_METRICS_DEFAULT);
    if (val instanceof String) {
      return Boolean.parseBoolean(val.toString());
    }
    return (boolean) val;
  }

  /** @return Blob format version */
  public Constants.BdecVersion getBlobFormatVersion() {
    Object val = this.parameterMap.getOrDefault(BLOB_FORMAT_VERSION, BLOB_FORMAT_VERSION_DEFAULT);
    if (val instanceof Constants.BdecVersion) {
      return (Constants.BdecVersion) val;
    }
    if (val instanceof String) {
      try {
        val = Integer.parseInt((String) val);
      } catch (Throwable t) {
        throw new IllegalArgumentException(
            String.format("Failed to parse BLOB_FORMAT_VERSION = '%s'", val), t);
      }
    }
    return Constants.BdecVersion.fromInt((int) val);
  }

  /**
   * @return the IO_TIME/CPU ratio that we will use to determine the number of buildAndUpload
   *     threads
   */
  public int getIOTimeCpuRatio() {
    Object val = this.parameterMap.getOrDefault(IO_TIME_CPU_RATIO, IO_TIME_CPU_RATIO_DEFAULT);
    if (val instanceof String) {
      return Integer.parseInt(val.toString());
    }
    return (int) val;
  }

  /** @return the max retry count when waiting for a blob upload task to finish */
  public int getBlobUploadMaxRetryCount() {
    Object val =
        this.parameterMap.getOrDefault(
            BLOB_UPLOAD_MAX_RETRY_COUNT, BLOB_UPLOAD_MAX_RETRY_COUNT_DEFAULT);
    if (val instanceof String) {
      return Integer.parseInt(val.toString());
    }
    return (int) val;
  }

  /** @return The max memory limit in bytes */
  public long getMaxMemoryLimitInBytes() {
    Object val =
        this.parameterMap.getOrDefault(
            MAX_MEMORY_LIMIT_IN_BYTES, MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT);
    return (val instanceof String) ? Long.parseLong(val.toString()) : (long) val;
  }

  /** @return Return whether memory optimization for Parquet is enabled. */
  public boolean getEnableParquetInternalBuffering() {
    Object val =
        this.parameterMap.getOrDefault(
            ENABLE_PARQUET_INTERNAL_BUFFERING, ENABLE_PARQUET_INTERNAL_BUFFERING_DEFAULT);
    return (val instanceof String) ? Boolean.parseBoolean(val.toString()) : (boolean) val;
  }

  /** @return The max channel size in bytes */
  public long getMaxChannelSizeInBytes() {
    Object val =
        this.parameterMap.getOrDefault(
            MAX_CHANNEL_SIZE_IN_BYTES, MAX_CHANNEL_SIZE_IN_BYTES_DEFAULT);
    return (val instanceof String) ? Long.parseLong(val.toString()) : (long) val;
  }

  /** @return The max chunk size in bytes that could avoid OOM at server side */
  public long getMaxChunkSizeInBytes() {
    Object val =
        this.parameterMap.getOrDefault(MAX_CHUNK_SIZE_IN_BYTES, MAX_CHUNK_SIZE_IN_BYTES_DEFAULT);
    return (val instanceof String) ? Long.parseLong(val.toString()) : (long) val;
  }

  public long getMaxAllowedRowSizeInBytes() {
    Object val =
        this.parameterMap.getOrDefault(
            MAX_ALLOWED_ROW_SIZE_IN_BYTES, MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT);
    return (val instanceof String) ? Long.parseLong(val.toString()) : (long) val;
  }

  /**
   * @return The max number of chunks that can be put into a single BDEC or blob registration
   *     request.
   */
  public int getMaxChunksInBlobAndRegistrationRequest() {
    Object val =
        this.parameterMap.getOrDefault(
            MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST,
            MAX_CHUNKS_IN_BLOB_AND_REGISTRATION_REQUEST_DEFAULT);
    return (val instanceof String) ? Integer.parseInt(val.toString()) : (int) val;
  }

  /** @return BDEC compression algorithm */
  public Constants.BdecParquetCompression getBdecParquetCompressionAlgorithm() {
    Object val =
        this.parameterMap.getOrDefault(
            BDEC_PARQUET_COMPRESSION_ALGORITHM, BDEC_PARQUET_COMPRESSION_ALGORITHM_DEFAULT);
    if (val instanceof Constants.BdecParquetCompression) {
      return (Constants.BdecParquetCompression) val;
    }
    return Constants.BdecParquetCompression.fromName((String) val);
  }

  @Override
  public String toString() {
    return "ParameterProvider{" + "parameterMap=" + parameterMap + '}';
  }
}

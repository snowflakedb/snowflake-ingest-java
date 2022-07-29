package net.snowflake.ingest.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Utility class to provide configurable constants */
public class ParameterProvider {
  public static final String BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY =
      "STREAMING_INGEST_CLIENT_SDK_BUFFER_FLUSH_INTERVAL_IN_MILLIS".toLowerCase();
  public static final String BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY =
      "STREAMING_INGEST_CLIENT_SDK_BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS".toLowerCase();
  public static final String INSERT_THROTTLE_INTERVAL_IN_MILLIS_MAP_KEY =
      "STREAMING_INGEST_CLIENT_SDK_INSERT_THROTTLE_INTERVAL_IN_MILLIS".toLowerCase();
  public static final String INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY =
      "STREAMING_INGEST_CLIENT_SDK_INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE".toLowerCase();
  public static final String ENABLE_SNOWPIPE_STREAMING_METRICS_MAP_KEY =
      "ENABLE_SNOWPIPE_STREAMING_JMX_METRICS".toLowerCase();

  public static final String BLOB_FORMAT_VERSION = "BLOB_FORMAT_VERSION".toLowerCase();

  // Default values
  public static final long BUFFER_FLUSH_INTERVAL_IN_MILLIS_DEFAULT = 1000;
  public static final long BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT = 100;
  public static final long INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT = 500;
  public static final long INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT = 10;
  public static final boolean SNOWPIPE_STREAMING_METRICS_DEFAULT = false;

  public static final Constants.BdecVerion BLOB_FORMAT_VERSION_DEFAULT = Constants.BdecVerion.ONE;

  /** Map of parameter name to parameter value. This will be set by client/configure API Call. */
  private final Map<String, Object> parameterMap = new HashMap<>();

  /**
   * Constructor. Takes properties from profile file and properties from client constructor and
   * resolves final parameter value
   *
   * @param parameterOverrides Map<String, Object> of parameter name -> value
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
    this.updateValue(
        BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY,
        BUFFER_FLUSH_INTERVAL_IN_MILLIS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY,
        BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        INSERT_THROTTLE_INTERVAL_IN_MILLIS_MAP_KEY,
        INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY,
        INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(
        ENABLE_SNOWPIPE_STREAMING_METRICS_MAP_KEY,
        SNOWPIPE_STREAMING_METRICS_DEFAULT,
        parameterOverrides,
        props);

    this.updateValue(BLOB_FORMAT_VERSION, BLOB_FORMAT_VERSION_DEFAULT, parameterOverrides, props);
    getBlobFormatVersion(); // to verify parsing the configured value
  }

  /** @return Longest interval in milliseconds between buffer flushes */
  public long getBufferFlushIntervalInMs() {
    return (long)
        this.parameterMap.getOrDefault(
            BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY, BUFFER_FLUSH_INTERVAL_IN_MILLIS_DEFAULT);
  }

  /** @return Time in milliseconds between checks to see if the buffer should be flushed */
  public long getBufferFlushCheckIntervalInMs() {
    return (long)
        this.parameterMap.getOrDefault(
            BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY,
            BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_DEFAULT);
  }

  /** @return Duration in milliseconds to delay data insertion to the buffer when throttled */
  public long getInsertThrottleIntervalInMs() {
    return (long)
        this.parameterMap.getOrDefault(
            INSERT_THROTTLE_INTERVAL_IN_MILLIS_MAP_KEY, INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT);
  }

  /** @return Percent of free total memory at which we throttle row inserts */
  public int getInsertThrottleThresholdInPercentage() {
    return ((Long)
            this.parameterMap.getOrDefault(
                INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY,
                INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_DEFAULT))
        .intValue();
  }

  /** @return true if jmx metrics are enabled for a client */
  public boolean hasEnabledSnowpipeStreamingMetrics() {
    return (Boolean)
        this.parameterMap.getOrDefault(
            ENABLE_SNOWPIPE_STREAMING_METRICS_MAP_KEY, SNOWPIPE_STREAMING_METRICS_DEFAULT);
  }

  /** @return Blob format version: 1 (arrow stream write mode), 2 (arrow file write mode) etc */
  public Constants.BdecVerion getBlobFormatVersion() {
    Object val = this.parameterMap.getOrDefault(BLOB_FORMAT_VERSION, BLOB_FORMAT_VERSION_DEFAULT);
    if (val instanceof Constants.BdecVerion) {
      return (Constants.BdecVerion) val;
    }
    if (val instanceof String) {
      try {
        val = Integer.parseInt((String) val);
      } catch (Throwable t) {
        throw new IllegalArgumentException(
            String.format("Failed to parse BLOB_FORMAT_VERSION = '%s'", val), t);
      }
    }
    return Constants.BdecVerion.fromInt((int) val);
  }

  @Override
  public String toString() {
    return "ParameterProvider{" + "parameterMap=" + parameterMap + '}';
  }
}

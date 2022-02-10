package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

public class ParameterProviderTest {

  @Test
  public void withValuesSet() {
    Properties prop = new Properties();
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY, 3l);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY, 4l);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY, 6l);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_MAP_KEY, 7l);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, prop);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(7, parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withNullProps() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY, 3l);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY, 4l);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY, 6l);
    ParameterProvider parameterProvider = new ParameterProvider(parameterMap, null);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        parameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withNullParameterMap() {
    Properties props = new Properties();
    props.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS_MAP_KEY, 3l);
    props.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS_MAP_KEY, 4l);
    props.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE_MAP_KEY, 6l);
    ParameterProvider parameterProvider = new ParameterProvider(null, props);

    Assert.assertEquals(3, parameterProvider.getBufferFlushIntervalInMs());
    Assert.assertEquals(4, parameterProvider.getBufferFlushCheckIntervalInMs());
    Assert.assertEquals(6, parameterProvider.getInsertThrottleThresholdInPercentage());
    Assert.assertEquals(
        parameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS_DEFAULT,
        parameterProvider.getInsertThrottleIntervalInMs());
  }

  @Test
  public void withNullInputs() {
    ParameterProvider parameterProvider = new ParameterProvider(null, null);

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

  @Test
  public void withDefaultValues() {
    ParameterProvider parameterProvider = new ParameterProvider();

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

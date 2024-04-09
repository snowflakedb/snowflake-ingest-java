package net.snowflake.ingest.connection;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.mockito.Mockito;

public class TelemetryServiceTest {
  @Test
  public void testReportLatencyInSec() {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    TelemetryService telemetryService =
        Mockito.spy(
            new TelemetryService(httpClient, "testReportLatencyInSec", "snowflake.dev.local:8082"));
    Mockito.doNothing().when(telemetryService).send(Mockito.any(), Mockito.any());
    MetricRegistry metrics = new MetricRegistry();
    Timer flushLatency = metrics.timer(MetricRegistry.name("latency", "flush"));
    Timer buildLatency = metrics.timer(MetricRegistry.name("latency", "build"));
    Timer uploadLatency = metrics.timer(MetricRegistry.name("latency", "upload"));
    Timer registerLatency = metrics.timer(MetricRegistry.name("latency", "register"));

    // Make sure there is no exception thrown
    telemetryService.reportLatencyInSec(buildLatency, uploadLatency, registerLatency, flushLatency);
  }

  @Test
  public void testReportClientFailure() {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    TelemetryService telemetryService =
        Mockito.spy(
            new TelemetryService(
                httpClient, "testReportClientFailure", "snowflake.dev.local:8082"));
    Mockito.doNothing().when(telemetryService).send(Mockito.any(), Mockito.any());

    // Make sure there is no exception thrown
    telemetryService.reportClientFailure("testReportClientFailure", "exception");
  }

  @Test
  public void testReportThroughputBytesPerSecond() {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    TelemetryService telemetryService =
        Mockito.spy(
            new TelemetryService(
                httpClient, "testReportThroughputBytesPerSecond", "snowflake.dev.local:8082"));
    Mockito.doNothing().when(telemetryService).send(Mockito.any(), Mockito.any());
    MetricRegistry metrics = new MetricRegistry();
    Meter uploadThroughput = metrics.meter(MetricRegistry.name("throughput", "upload"));
    Meter inputThroughput = metrics.meter(MetricRegistry.name("throughput", "input"));

    // Make sure there is no exception thrown
    telemetryService.reportThroughputBytesPerSecond(uploadThroughput, inputThroughput);
  }

  @Test
  public void testReportCpuMemoryUsage() {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    TelemetryService telemetryService =
        Mockito.spy(
            new TelemetryService(
                httpClient, "testReportCpuMemoryUsage", "snowflake.dev.local:8082"));
    Mockito.doNothing().when(telemetryService).send(Mockito.any(), Mockito.any());
    MetricRegistry metrics = new MetricRegistry();
    Histogram cpuHistogram = metrics.histogram(MetricRegistry.name("cpu", "usage", "histogram"));

    // Make sure there is no exception thrown
    telemetryService.reportCpuMemoryUsage(cpuHistogram);
  }

  @Test
  public void testReportBatchOffsetMismatch() {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    TelemetryService telemetryService =
        Mockito.spy(
            new TelemetryService(
                httpClient, "testReportClientFailure", "snowflake.dev.local:8082"));
    Mockito.doNothing().when(telemetryService).send(Mockito.any(), Mockito.any());

    // Make sure there is no exception thrown
    telemetryService.reportBatchOffsetMismatch("channel", "0", "1", "2", 1);
  }
}

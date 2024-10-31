package net.snowflake.ingest.connection;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TelemetryServiceTest {
  @Parameters(name = "{index}: {0}")
  public static Object[] icebergStreamingModes() {
    return new Object[] {false};
  }
  ;

  @Parameter public boolean enableIcebergStreaming;

  @Test
  public void testReportLatencyInSec() {
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    TelemetryService telemetryService =
        Mockito.spy(
            new TelemetryService(
                httpClient,
                enableIcebergStreaming,
                "testReportLatencyInSec",
                "snowflake.dev.local:8082"));
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
                httpClient,
                enableIcebergStreaming,
                "testReportClientFailure",
                "snowflake.dev.local:8082"));
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
                httpClient,
                enableIcebergStreaming,
                "testReportThroughputBytesPerSecond",
                "snowflake.dev.local:8082"));
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
                httpClient,
                enableIcebergStreaming,
                "testReportCpuMemoryUsage",
                "snowflake.dev.local:8082"));
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
                httpClient,
                enableIcebergStreaming,
                "testReportClientFailure",
                "snowflake.dev.local:8082"));
    Mockito.doNothing().when(telemetryService).send(Mockito.any(), Mockito.any());

    // Make sure there is no exception thrown
    telemetryService.reportBatchOffsetMismatch("channel", "0", "1", "2", 1);
  }
}

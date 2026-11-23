package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.ingest.utils.OCSPMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that GCS presigned URL uploads return their connection to the shared HTTP connection
 * pool when the storage endpoint rejects the upload, so that later uploads can still get a
 * connection.
 */
public class SnowflakeGCSClientConnectionPoolTest {
  private static final int FAILED_UPLOADS = 3;
  private static final byte[] DATA = "data".getBytes(StandardCharsets.UTF_8);

  private final AtomicInteger statusCode = new AtomicInteger();
  private final AtomicInteger requestCount = new AtomicInteger();
  private HttpServer server;
  private SnowflakeGCSClient gcsClient;
  private boolean telemetryEnabled;
  private int connectionTimeoutMillis;

  @Before
  public void setUp() throws Exception {
    telemetryEnabled = TelemetryService.getInstance().isEnabled();
    TelemetryService.disable();

    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try (InputStream in = exchange.getRequestBody()) {
            while (in.read() != -1) {}
          }
          requestCount.incrementAndGet();
          byte[] body = "body".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(statusCode.get(), body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();

    // A single pooled connection makes one leaked connection block every later upload, and a short
    // connection timeout turns that block into a fast failure.
    System.setProperty(JdbcHttpUtil.JDBC_MAX_CONNECTIONS_PROPERTY, "1");
    System.setProperty(JdbcHttpUtil.JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY, "1");
    connectionTimeoutMillis = (int) JdbcHttpUtil.getConnectionTimeout().toMillis();
    JdbcHttpUtil.setConnectionTimeout(1000);
    JdbcHttpUtil.reset();

    StageInfo stageInfo =
        StageInfo.createStageInfo("GCS", "bucket/path", new HashMap<>(), null, null, null, true);
    gcsClient = SnowflakeGCSClient.createSnowflakeGCSClient(stageInfo, null);
  }

  @After
  public void tearDown() throws Exception {
    server.stop(0);
    System.clearProperty(JdbcHttpUtil.JDBC_MAX_CONNECTIONS_PROPERTY);
    System.clearProperty(JdbcHttpUtil.JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY);
    JdbcHttpUtil.setConnectionTimeout(connectionTimeoutMillis);
    JdbcHttpUtil.reset();
    if (telemetryEnabled) {
      TelemetryService.enable();
    } else {
      TelemetryService.disable();
    }
  }

  @Test
  public void testUploadSucceedsAfterRepeated503Uploads() throws Exception {
    statusCode.set(503);
    for (int i = 0; i < FAILED_UPLOADS; i++) {
      assertThrows(SnowflakeSQLException.class, this::upload);
    }

    statusCode.set(200);
    int requestsBeforeSuccess = requestCount.get();
    upload();
    assertEquals(requestsBeforeSuccess + 1, requestCount.get());
  }

  @Test
  public void testRepeatedSuccessfulUploadsReuseConnection() throws Exception {
    statusCode.set(200);
    for (int i = 0; i < FAILED_UPLOADS; i++) {
      upload();
    }
    assertEquals(FAILED_UPLOADS, requestCount.get());
  }

  private void upload() throws SnowflakeSQLException {
    gcsClient.uploadWithPresignedUrlWithoutConnection(
        1000, // network timeout in ms, bounds retries of a 503
        new HttpClientSettingsKey(OCSPMode.FAIL_OPEN),
        1, // parallelism
        true, // upload from stream
        "bucket/path",
        null, // no source file
        "file.bdec",
        new ByteArrayInputStream(DATA),
        null, // no file backed output stream
        new CommonObjectMetadata(DATA.length, "", new HashMap<>()),
        null, // no stage region
        "http://localhost:" + server.getAddress().getPort() + "/bucket/path/file.bdec?sig=abc",
        "query-id");
  }
}

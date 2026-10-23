package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link RestRequest#executeWithRetries} returns pooled connections when it gives up
 * on a request, using a real connection pool against a local HTTP server.
 */
public class RestRequestConnectionReleaseTest {
  private static final int REQUESTS = 3;

  private final AtomicInteger statusCode = new AtomicInteger();
  private HttpServer server;
  private PoolingHttpClientConnectionManager connectionManager;
  private CloseableHttpClient httpClient;
  private boolean telemetryEnabled;

  @Before
  public void setUp() throws Exception {
    telemetryEnabled = TelemetryService.getInstance().isEnabled();
    TelemetryService.disable();

    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body = "body".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(statusCode.get(), body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();

    // A single slot makes one leaked connection block every later request.
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(1);
    connectionManager.setDefaultMaxPerRoute(1);
    httpClient = HttpClientBuilder.create().setConnectionManager(connectionManager).build();
  }

  @After
  public void tearDown() throws Exception {
    httpClient.close();
    server.stop(0);
    if (telemetryEnabled) {
      TelemetryService.enable();
    } else {
      TelemetryService.disable();
    }
  }

  @Test
  public void testReleasesConnectionWhenRetryDisabledAndResponseIs503() throws Exception {
    statusCode.set(503);

    for (int i = 0; i < REQUESTS; i++) {
      assertThrows(SnowflakeSQLException.class, () -> execute(0, true));
      assertEquals(0, connectionManager.getTotalStats().getLeased());
    }
  }

  @Test
  public void testReleasesConnectionWhenMaxRetriesExhaustedOn503() throws Exception {
    statusCode.set(503);

    for (int i = 0; i < REQUESTS; i++) {
      assertThrows(SnowflakeSQLException.class, () -> execute(1, false));
      assertEquals(0, connectionManager.getTotalStats().getLeased());
    }
  }

  @Test
  public void testKeepsConnectionLeasedOn200UntilCallerConsumesBody() throws Exception {
    statusCode.set(200);

    for (int i = 0; i < REQUESTS; i++) {
      HttpResponseContextDto response = execute(0, true);
      assertEquals(1, connectionManager.getTotalStats().getLeased());
      assertEquals("body", EntityUtils.toString(response.getHttpResponse().getEntity()));
      assertEquals(0, connectionManager.getTotalStats().getLeased());
    }
  }

  private HttpResponseContextDto execute(int maxRetries, boolean noRetry)
      throws SnowflakeSQLException {
    HttpGet request =
        new HttpGet("http://localhost:" + server.getAddress().getPort() + "/?requestId=abcd-1234");
    return RestRequest.executeWithRetries(
        httpClient,
        request,
        0, // retry timeout
        0, // auth timeout
        1000, // socket timeout in ms
        maxRetries,
        0, // no socket timeout injection
        null, // no canceling
        false, // no cookie
        false, // no url retry query parameters
        false, // no request_guid
        false, // no retry on HTTP 403
        noRetry,
        false, // caller reads the response
        new ExecTimeTelemetryData());
  }
}

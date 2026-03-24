// Ported from snowflake-jdbc: net.snowflake.client.jdbc.telemetry.TelemetryTest
package net.snowflake.ingest.connection.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedList;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;

public class TelemetryClientTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testJsonConversion() {
    // Ported from TelemetryTest.testJsonConversion in JDBC
    LinkedList<TelemetryData> logs = new LinkedList<>();

    ObjectNode msg1 = mapper.createObjectNode();
    msg1.put(TelemetryUtil.TYPE, TelemetryField.TIME_CONSUME_FIRST_RESULT.toString());
    msg1.put(TelemetryUtil.QUERY_ID, "query1");
    logs.add(new TelemetryData(msg1, 100L));

    ObjectNode msg2 = mapper.createObjectNode();
    msg2.put(TelemetryUtil.TYPE, TelemetryField.TIME_CONSUME_LAST_RESULT.toString());
    msg2.put(TelemetryUtil.QUERY_ID, "query2");
    logs.add(new TelemetryData(msg2, 200L));

    String json = TelemetryClient.logsToString(logs);
    assertNotNull(json);
    assertTrue(json.contains("\"logs\""));
    assertTrue(json.contains("\"timestamp\""));
    assertTrue(json.contains("\"message\""));
    assertTrue(json.contains("client_time_consume_first_result"));
    assertTrue(json.contains("client_time_consume_last_result"));
  }

  @Test
  public void testLogsToJson() {
    LinkedList<TelemetryData> logs = new LinkedList<>();

    ObjectNode msg = mapper.createObjectNode();
    msg.put("key", "value");
    logs.add(new TelemetryData(msg, 50L));

    ObjectNode result = TelemetryClient.logsToJson(logs);
    assertNotNull(result.get("logs"));
    assertEquals(1, result.get("logs").size());
    assertEquals("50", result.get("logs").get(0).get("timestamp").asText());
  }

  @Test
  public void testLogsToJsonEmpty() {
    LinkedList<TelemetryData> logs = new LinkedList<>();
    ObjectNode result = TelemetryClient.logsToJson(logs);
    assertNotNull(result.get("logs"));
    assertEquals(0, result.get("logs").size());
  }

  @Test
  public void testCreateSessionlessTelemetryKeypairJwt() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    Telemetry client =
        TelemetryClient.createSessionlessTelemetry(
            httpClient, "https://account.snowflakecomputing.com", "KEYPAIR_JWT");
    assertNotNull(client);
  }

  @Test
  public void testCreateSessionlessTelemetryOAuth() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    Telemetry client =
        TelemetryClient.createSessionlessTelemetry(
            httpClient, "https://account.snowflakecomputing.com", "OAUTH");
    assertNotNull(client);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSessionlessTelemetryInvalidAuthType() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    TelemetryClient.createSessionlessTelemetry(
        httpClient, "https://account.snowflakecomputing.com", "INVALID");
  }

  @Test
  public void testBufferOperations() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    TelemetryClient client =
        (TelemetryClient)
            TelemetryClient.createSessionlessTelemetry(
                httpClient, "https://account.snowflakecomputing.com", "KEYPAIR_JWT", 100);

    assertEquals(0, client.bufferSize());

    ObjectNode msg = mapper.createObjectNode();
    msg.put("key", "value");
    client.refreshToken("test-token");
    client.addLogToBatch(new TelemetryData(msg, System.currentTimeMillis()));

    assertEquals(1, client.bufferSize());
    assertEquals(1, client.logBuffer().size());
  }

  @Test
  public void testAddLogAfterClose() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    TelemetryClient client =
        (TelemetryClient)
            TelemetryClient.createSessionlessTelemetry(
                httpClient, "https://account.snowflakecomputing.com", "KEYPAIR_JWT", 100);

    client.close();
    assertTrue(client.isClosed());

    // Adding log after close should not throw
    ObjectNode msg = mapper.createObjectNode();
    msg.put("key", "value");
    client.addLogToBatch(new TelemetryData(msg, System.currentTimeMillis()));

    // Buffer should remain empty since client is closed
    assertEquals(0, client.bufferSize());
  }

  @Test
  public void testDisableTelemetry() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    TelemetryClient client =
        (TelemetryClient)
            TelemetryClient.createSessionlessTelemetry(
                httpClient, "https://account.snowflakecomputing.com", "KEYPAIR_JWT", 100);

    assertTrue(client.isTelemetryEnabled());

    client.disableTelemetry();

    // After disabling, adding logs should be a no-op
    ObjectNode msg = mapper.createObjectNode();
    msg.put("key", "value");
    client.addLogToBatch(new TelemetryData(msg, System.currentTimeMillis()));
    assertEquals(0, client.bufferSize());
  }

  @Test
  public void testServerUrlTrailingSlash() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    // URL with trailing slash should still construct valid telemetry URL
    Telemetry client =
        TelemetryClient.createSessionlessTelemetry(
            httpClient, "https://account.snowflakecomputing.com/", "KEYPAIR_JWT");
    assertNotNull(client);
  }
}

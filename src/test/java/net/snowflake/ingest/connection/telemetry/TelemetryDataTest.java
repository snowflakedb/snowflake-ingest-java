// Ported from snowflake-jdbc: net.snowflake.client.jdbc.telemetry.TelemetryTest
package net.snowflake.ingest.connection.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

public class TelemetryDataTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testToJson() {
    ObjectNode msg = mapper.createObjectNode();
    msg.put("type", "test_metric");
    msg.put("query_id", "query123");

    TelemetryData data = new TelemetryData(msg, 1234567890L);

    ObjectNode json = data.toJson();
    assertEquals("1234567890", json.get("timestamp").asText());
    assertNotNull(json.get("message"));
    assertEquals("test_metric", json.get("message").get("type").asText());
  }

  @Test
  public void testGetters() {
    ObjectNode msg = mapper.createObjectNode();
    msg.put("key", "value");

    TelemetryData data = new TelemetryData(msg, 99L);

    assertEquals(99L, data.getTimeStamp());
    assertEquals("value", data.getMessage().get("key").asText());
  }

  @Test
  public void testToString() {
    ObjectNode msg = mapper.createObjectNode();
    msg.put("key", "value");

    TelemetryData data = new TelemetryData(msg, 100L);

    String str = data.toString();
    assertNotNull(str);
    // toString delegates to toJson().toString()
    assertEquals(data.toJson().toString(), str);
  }
}

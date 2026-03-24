// Ported from snowflake-jdbc: net.snowflake.client.jdbc.telemetry.TelemetryTest
package net.snowflake.ingest.connection.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

public class TelemetryUtilTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testBuildJobDataWithParams() {
    TelemetryData data =
        TelemetryUtil.buildJobData("query123", TelemetryField.TIME_CONSUME_FIRST_RESULT, 42L);

    ObjectNode msg = data.getMessage();
    assertEquals("client_time_consume_first_result", msg.get(TelemetryUtil.TYPE).asText());
    assertEquals("query123", msg.get(TelemetryUtil.QUERY_ID).asText());
    assertEquals(42L, msg.get(TelemetryUtil.VALUE).asLong());
    assertTrue(data.getTimeStamp() > 0);
  }

  @Test
  public void testBuildJobDataWithObjectNode() {
    ObjectNode obj = mapper.createObjectNode();
    obj.put("custom_field", "custom_value");

    TelemetryData data = TelemetryUtil.buildJobData(obj);

    assertEquals("custom_value", data.getMessage().get("custom_field").asText());
    assertTrue(data.getTimeStamp() > 0);
  }
}

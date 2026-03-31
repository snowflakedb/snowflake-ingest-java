// Ported from snowflake-jdbc: net.snowflake.client.jdbc.telemetry.TelemetryField
package net.snowflake.ingest.connection.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TelemetryFieldTest {

  @Test
  public void testToString() {
    assertEquals(
        "client_time_consume_first_result", TelemetryField.TIME_CONSUME_FIRST_RESULT.toString());
    assertEquals("client_sql_exception", TelemetryField.SQL_EXCEPTION.toString());
    assertEquals("client_metadata_api_metrics", TelemetryField.METADATA_METRICS.toString());
  }

  @Test
  public void testAllFieldsHaveClientPrefix() {
    for (TelemetryField field : TelemetryField.values()) {
      assertTrue(
          "Field " + field.name() + " should have client_ prefix",
          field.toString().startsWith("client_"));
    }
  }

  @Test
  public void testEnumCount() {
    assertEquals(10, TelemetryField.values().length);
  }
}

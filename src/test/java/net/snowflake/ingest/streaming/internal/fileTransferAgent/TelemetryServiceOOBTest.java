// Ported from snowflake-jdbc: net.snowflake.client.jdbc.telemetryOOB.TelemetryServiceTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TelemetryServiceOOBTest {
  private boolean defaultState;

  @Before
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    defaultState = service.isEnabled();
    service.enable();
  }

  @After
  public void tearDown() throws InterruptedException {
    TelemetryService service = TelemetryService.getInstance();
    if (defaultState) {
      service.enable();
    } else {
      service.disable();
    }
  }

  @Test
  public void testTelemetryInitErrors() {
    TelemetryService service = TelemetryService.getInstance();
    assertEquals(
        "Telemetry server failure count should be zero at first.",
        0,
        service.getServerFailureCount());
    assertEquals(
        "Telemetry client failure count should be zero at first.",
        0,
        service.getClientFailureCount());
  }

  @Test
  public void testTelemetryEnableDisable() {
    TelemetryService service = TelemetryService.getInstance();
    // We already enabled telemetry in setup phase.
    assertTrue("Telemetry should be already enabled here.", service.isEnabled());
    service.disable();
    assertFalse("Telemetry should be already disabled here.", service.isEnabled());
    service.enable();
    assertTrue("Telemetry should be enabled back", service.isEnabled());
  }

  @Test
  public void testTelemetryConnectionString() {
    String INVALID_CONNECTION_STRING =
        "://:-1"; // INVALID_CONNECT_STRING in SnowflakeConnectionString.java
    Map<String, String> param = new HashMap<>();
    param.put("uri", "jdbc:snowflake://snowflake.reg.local:8082");
    param.put("host", "snowflake.reg.local");
    param.put("port", "8082");
    param.put("account", "fakeaccount");
    param.put("user", "fakeuser");
    param.put("password", "fakepassword");
    param.put("database", "fakedatabase");
    param.put("schema", "fakeschema");
    param.put("role", "fakerole");
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(param);
    assertNotEquals(
        "Telemetry failed to generate sfConnectionStr.",
        INVALID_CONNECTION_STRING,
        service.getSnowflakeConnectionString().toString());
  }
}

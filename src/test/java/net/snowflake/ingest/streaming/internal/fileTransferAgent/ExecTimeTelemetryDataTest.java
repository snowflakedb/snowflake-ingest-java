// Ported from snowflake-jdbc: net.snowflake.client.core.ExecTimeTelemetryDataTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.junit.Test;

public class ExecTimeTelemetryDataTest {

  @Test
  public void testExecTimeTelemetryData() throws ParseException {
    ExecTimeTelemetryData execTimeTelemetryData = new ExecTimeTelemetryData();
    execTimeTelemetryData.sendData = true;
    execTimeTelemetryData.setBindStart();
    execTimeTelemetryData.setOCSPStatus(true);
    execTimeTelemetryData.setBindEnd();
    execTimeTelemetryData.setHttpClientStart();
    execTimeTelemetryData.setHttpClientEnd();
    execTimeTelemetryData.setGzipStart();
    execTimeTelemetryData.setGzipEnd();
    execTimeTelemetryData.setQueryEnd();
    execTimeTelemetryData.setQueryId("queryid");
    execTimeTelemetryData.setProcessResultChunkStart();
    execTimeTelemetryData.setProcessResultChunkEnd();
    execTimeTelemetryData.setResponseIOStreamStart();
    execTimeTelemetryData.setResponseIOStreamEnd();
    execTimeTelemetryData.setCreateResultSetStart();
    execTimeTelemetryData.setCreateResultSetEnd();
    execTimeTelemetryData.incrementRetryCount();
    execTimeTelemetryData.setRequestId("mockId");
    execTimeTelemetryData.addRetryLocation("retry");

    String telemetry = execTimeTelemetryData.generateTelemetry();
    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    JSONObject json = (JSONObject) parser.parse(telemetry);
    assertNotNull(json.get("BindStart"));
    assertNotNull(json.get("BindEnd"));
    assertEquals(json.get("ocspEnabled"), true);
    assertNotNull(json.get("HttpClientStart"));
    assertNotNull(json.get("HttpClientEnd"));
    assertNotNull(json.get("GzipStart"));
    assertNotNull(json.get("GzipEnd"));
    assertNotNull(json.get("QueryEnd"));
    assertEquals("queryid", json.get("QueryID"));
    assertNotNull(json.get("ProcessResultChunkStart"));
    assertNotNull(json.get("ProcessResultChunkEnd"));
    assertNotNull(json.get("ResponseIOStreamStart"));
    assertNotNull(json.get("CreateResultSetStart"));
    assertNotNull(json.get("CreateResultSetEnd"));
    assertNotNull(json.get("ElapsedQueryTime"));
    assertNotNull(json.get("ElapsedResultProcessTime"));
    assertNull(json.get("QueryFunction"));
    assertNull(json.get("BatchID"));
    assertEquals(1, ((Long) json.get("RetryCount")).intValue());
    assertEquals("mockId", json.get("RequestID"));
    assertEquals("retry", json.get("RetryLocations"));
    assertEquals(true, json.get("Urgent"));
    assertEquals("ExecutionTimeRecord", json.get("eventType"));
  }

  @Test
  public void testRetryLocation() throws ParseException {
    TelemetryService.enableHTAP();
    ExecTimeTelemetryData execTimeTelemetryData =
        new ExecTimeTelemetryData("queryFunction", "batchId");
    execTimeTelemetryData.addRetryLocation("hello");
    execTimeTelemetryData.addRetryLocation("world");
    execTimeTelemetryData.sendData = true;
    String telemetry = execTimeTelemetryData.generateTelemetry();

    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    JSONObject json = (JSONObject) parser.parse(telemetry);
    assertEquals("queryFunction", json.get("QueryFunction"));
    assertEquals("batchId", json.get("BatchID"));
    assertNotNull(json.get("QueryStart"));
    assertEquals("hello, world", json.get("RetryLocations"));
    TelemetryService.disableHTAP();
  }
}

// Ported from snowflake-jdbc: net.snowflake.client.log.SFLoggerFactoryTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SFLoggerFactoryTest {

  @Test
  public void testGetLoggerByNameDefault() {
    SFLogger sflogger = SFLoggerFactory.getLogger("SnowflakeConnectionV1");
    assertTrue(sflogger instanceof JDK14Logger);
  }
}

package net.snowflake;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class StandardIngestE2ETest {

  private IngestTestUtils ingestTestUtils;

  @Before
  public void setUp() throws Exception {
    ingestTestUtils = new IngestTestUtils("standard_ingest");
  }

  @After
  public void tearDown() throws Exception {
    ingestTestUtils.close();
  }

  @Test
  public void basicTest() throws InterruptedException {
    ingestTestUtils.runBasicTest();
  }

  @Test
  public void longRunningTest() throws InterruptedException {
    ingestTestUtils.runLongRunningTest(Duration.of(80, ChronoUnit.MINUTES));
  }
}

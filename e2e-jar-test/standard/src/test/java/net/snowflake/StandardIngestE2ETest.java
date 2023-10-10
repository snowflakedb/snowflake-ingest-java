package net.snowflake;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
  public void name() throws InterruptedException {
    ingestTestUtils.test();
  }
}

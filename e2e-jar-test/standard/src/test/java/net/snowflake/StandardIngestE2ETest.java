package net.snowflake;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StandardIngestE2ETest {

  @Parameterized.Parameters(name = "enableParquetReadbackVerification={0}")
  public static Object[] enableParquetReadbackVerification() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean enableParquetReadbackVerification;

  private IngestTestUtils ingestTestUtils;

  @Before
  public void setUp() throws Exception {
    ingestTestUtils = new IngestTestUtils("standard_ingest", enableParquetReadbackVerification);
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
  @Ignore("Takes too long to run")
  public void longRunningTest() throws InterruptedException {
    ingestTestUtils.runLongRunningTest(Duration.of(80, ChronoUnit.MINUTES));
  }
}

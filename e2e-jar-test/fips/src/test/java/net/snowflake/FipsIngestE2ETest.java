package net.snowflake;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.security.Security;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class FipsIngestE2ETest {

  private IngestTestUtils ingestTestUtils;

  @Before
  public void setUp() throws Exception {
    // Add FIPS provider, the SDK does not do this by default
    Security.addProvider(new BouncyCastleFipsProvider("C:HYBRID;ENABLE{All};"));

    ingestTestUtils = new IngestTestUtils("fips_ingest");
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

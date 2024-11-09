package net.snowflake.ingest.streaming.internal.datatypes;

import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class NullIT extends AbstractDataTypeTest {
  @Parameters(name = "{index}: {0}")
  public static Object[] parameters() {
    return new Object[] {"GZIP", "ZSTD"};
  }

  @Parameter public String compressionAlgorithm;

  @Before
  public void before() throws Exception {
    super.setUp(false, compressionAlgorithm, null);
  }

  @Test
  public void testNullIngestion() throws Exception {
    for (String type :
        Arrays.asList(
            "INT",
            "NUMBER(1, 1)",
            "NUMBER(1, 0)",
            "BOOLEAN",
            "VARCHAR",
            "BINARY",
            "DATE",
            "TIME",
            "TIME(0)",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_NTZ(0)",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_LTZ(0)",
            "TIMESTAMP_TZ",
            "TIMESTAMP_TZ(0)",
            "VARIANT",
            "ARRAY",
            "OBJECT")) {
      // Provider type does not matter as we are expecting null anyway
      testIngestion(type, null, null, new StringProvider());
    }
  }
}

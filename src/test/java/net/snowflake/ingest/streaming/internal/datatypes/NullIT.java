package net.snowflake.ingest.streaming.internal.datatypes;

import java.util.Arrays;
import net.snowflake.ingest.utils.Constants;
import org.junit.Test;

public class NullIT extends AbstractDataTypeTest {

  public NullIT(String name, Constants.BdecVersion bdecVersion) {
    super(name, bdecVersion);
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

package net.snowflake.ingest.connection;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import net.snowflake.ingest.SimpleIngestManager;
import org.junit.Assert;
import org.junit.Test;

public class UserAgentTest {
  @Test
  public void testDefaultSdkVersionMatchesProjectVersion() throws IOException {
    Properties properties = new Properties();
    try (InputStream is =
        SimpleIngestManager.class.getClassLoader().getResourceAsStream("project.properties")) {
      properties.load(is);
      Assert.assertEquals(RequestBuilder.DEFAULT_VERSION, properties.getProperty("version"));
    }
  }
}

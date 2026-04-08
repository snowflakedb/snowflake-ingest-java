// Ported from snowflake-jdbc: net.snowflake.client.core.ObjectMapperTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectMapperFactoryTest {
  private static final int jacksonDefaultMaxStringLength = 20_000_000;
  static String originalLogger;

  @BeforeClass
  public static void setProperty() {
    originalLogger = System.getProperty("net.snowflake.jdbc.loggerImpl");
    System.setProperty("net.snowflake.jdbc.loggerImpl", "net.snowflake.client.log.JDK14Logger");
  }

  @AfterClass
  public static void clearProperty() {
    if (originalLogger != null) {
      System.setProperty("net.snowflake.jdbc.loggerImpl", originalLogger);
    } else {
      System.clearProperty("net.snowflake.jdbc.loggerImpl");
    }
    System.clearProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM);
  }

  private static void setJacksonDefaultMaxStringLength(int maxJsonStringLength) {
    System.setProperty(
        ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, Integer.toString(maxJsonStringLength));
  }

  @Test
  public void testInvalidMaxJsonStringLength() throws SQLException {
    System.setProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, "abc");
    // calling getObjectMapper() should log the exception but not throw
    // default maxJsonStringLength value will be used
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    int stringLengthInMapper = mapper.getFactory().streamReadConstraints().getMaxStringLength();
    assertEquals(ObjectMapperFactory.DEFAULT_MAX_JSON_STRING_LEN, stringLengthInMapper);
  }

  @Test
  public void testObjectMapperWithLargeJsonString_16M_default() {
    verifyObjectMapperWithLargeJsonString(16 * 1024 * 1024, jacksonDefaultMaxStringLength);
  }

  @Test
  public void testObjectMapperWithLargeJsonString_16M_23M() {
    verifyObjectMapperWithLargeJsonString(16 * 1024 * 1024, 23_000_000);
  }

  @Test
  public void testObjectMapperWithLargeJsonString_32M_45M() {
    verifyObjectMapperWithLargeJsonString(32 * 1024 * 1024, 45_000_000);
  }

  @Test
  public void testObjectMapperWithLargeJsonString_64M_90M() {
    verifyObjectMapperWithLargeJsonString(64 * 1024 * 1024, 90_000_000);
  }

  @Test
  public void testObjectMapperWithLargeJsonString_128M_180M() {
    verifyObjectMapperWithLargeJsonString(128 * 1024 * 1024, 180_000_000);
  }

  private void verifyObjectMapperWithLargeJsonString(int lobSizeInBytes, int maxJsonStringLength) {
    setJacksonDefaultMaxStringLength(maxJsonStringLength);
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    try {
      JsonNode jsonNode = mapper.readTree(generateBase64EncodedJsonString(lobSizeInBytes));
      assertNotNull(jsonNode);
    } catch (Exception e) {
      // exception is expected when jackson's default maxStringLength value is used while retrieving
      // 16M string data
      assertEquals(jacksonDefaultMaxStringLength, maxJsonStringLength);
    }
  }

  private String generateBase64EncodedJsonString(int numChar) {
    StringBuilder jsonStr = new StringBuilder();
    String largeStr = SnowflakeUtil.randomAlphaNumeric(numChar);

    // encode the string and put it into a JSON formatted string
    jsonStr.append("[\"").append(encodeStringToBase64(largeStr)).append("\"]");
    return jsonStr.toString();
  }

  private String encodeStringToBase64(String stringToBeEncoded) {
    return Base64.getEncoder().encodeToString(stringToBeEncoded.getBytes(StandardCharsets.UTF_8));
  }
}

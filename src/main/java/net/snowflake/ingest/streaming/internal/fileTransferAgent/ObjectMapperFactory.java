/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/ObjectMapperFactory.java
 *
 * Permitted differences: package, @SnowflakeJdbcInternalApi removed,
 * SystemUtil.convertSystemPropertyToIntValue inlined.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.systemGetProperty;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;

/**
 * Factor method used to create ObjectMapper instance. All object mapper in JDBC should be created
 * by this method.
 */
public class ObjectMapperFactory {
  private static final SFLogger logger = SFLoggerFactory.getLogger(ObjectMapperFactory.class);

  // Snowflake allows up to 128M (after updating Max LOB size) string size and returns base64
  // encoded value that makes it up to 180M
  public static final int DEFAULT_MAX_JSON_STRING_LEN = 180_000_000;

  public static final String MAX_JSON_STRING_LENGTH_JVM =
      "net.snowflake.jdbc.objectMapper.maxJsonStringLength";

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(MapperFeature.OVERRIDE_PUBLIC_ACCESS_MODIFIERS, false);
    mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    // override the maxStringLength value in ObjectMapper
    int maxJsonStringLength =
        convertSystemPropertyToIntValue(MAX_JSON_STRING_LENGTH_JVM, DEFAULT_MAX_JSON_STRING_LEN);
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(maxJsonStringLength).build());
    return mapper;
  }

  /**
   * Inlined from SystemUtil.convertSystemPropertyToIntValue (JDBC source:
   * net.snowflake.client.core.SystemUtil)
   */
  private static int convertSystemPropertyToIntValue(String systemProperty, int defaultValue) {
    String systemPropertyValue = systemGetProperty(systemProperty);
    int returnVal = defaultValue;
    if (systemPropertyValue != null) {
      try {
        returnVal = Integer.parseInt(systemPropertyValue);
      } catch (NumberFormatException ex) {
        logger.warn(
            "Failed to parse the system parameter {} with value {}",
            systemProperty,
            systemPropertyValue);
      }
    }
    return returnVal;
  }
}

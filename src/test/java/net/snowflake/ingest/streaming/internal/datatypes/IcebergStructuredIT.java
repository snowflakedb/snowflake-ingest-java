package net.snowflake.ingest.streaming.internal.datatypes;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

@Ignore("This test can be enabled after server side Iceberg EP support is released")
public class IcebergStructuredIT extends AbstractDataTypeTest {
  @Parameterized.Parameters(name = "compressionAlgorithm={0}, icebergSerializationPolicy={1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"GZIP", Constants.IcebergSerializationPolicy.COMPATIBLE},
      {"GZIP", Constants.IcebergSerializationPolicy.OPTIMIZED},
      {"ZSTD", Constants.IcebergSerializationPolicy.COMPATIBLE},
      {"ZSTD", Constants.IcebergSerializationPolicy.OPTIMIZED}
    };
  }

  @Parameterized.Parameter public static String compressionAlgorithm;

  @Parameterized.Parameter(1)
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  @Before
  public void before() throws Exception {
    super.beforeIceberg(compressionAlgorithm, icebergSerializationPolicy);
  }

  @Test
  public void testStructuredDataType() throws Exception {
    assertStructuredDataType(
        "object(a int, b string, c boolean)", "{\"a\": 1, \"b\": \"test\", \"c\": true}");
    assertStructuredDataType("map(string, int)", "{\"key1\": 1}");
    assertStructuredDataType("array(int)", "[1, 2, 3]");
    assertMap(
        "map(string, int)",
        new HashMap<String, Integer>() {
          {
            put("key", 1);
          }
        });
    assertStructuredDataType("array(string)", null);

    /* Map with null key */
    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                assertMap(
                    "map(string, int)",
                    new HashMap<String, Integer>() {
                      {
                        put(null, 1);
                      }
                    }));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    /* Unknown field */
    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                assertStructuredDataType("object(a int, b string)", "{\"a\": 1, \"c\": \"test\"}"));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    /* Null struct, map list. TODO: SNOW-1727532 Should be fixed with null values EP calculation. */
    Assertions.assertThatThrownBy(
            () -> assertStructuredDataType("object(a int, b string, c boolean) not null", null))
        .isInstanceOf(NullPointerException.class);
    Assertions.assertThatThrownBy(() -> assertStructuredDataType("map(string, int) not null", null))
        .isInstanceOf(NullPointerException.class);
    Assertions.assertThatThrownBy(() -> assertStructuredDataType("array(int) not null", null))
        .isInstanceOf(NullPointerException.class);

    /* Nested data types. Should be fixed. Fixed in server side. */
    Assertions.assertThatThrownBy(
            () -> assertStructuredDataType("array(array(int))", "[[1, 2], [3, 4]]"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "array(map(string, int))", "[{\"key1\": 1}, {\"key2\": 2}]"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "array(object(a int, b string, c boolean))",
                    "[{\"a\": 1, \"b\": \"test\", \"c\": true}]"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "map(string, object(a int, b string, c boolean))",
                    "{\"key1\": {\"a\": 1, \"b\": \"test\", \"c\": true}}"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () -> assertStructuredDataType("map(string, array(int))", "{\"key1\": [1, 2, 3]}"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "map(string, map(string, int))", "{\"key1\": {\"key2\": 2}}"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "map(string, array(array(int)))", "{\"key1\": [[1, 2], [3, 4]]}"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "map(string, array(map(string, int)))",
                    "{\"key1\": [{\"key2\": 2}, {\"key3\": 3}]}"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "map(string, array(object(a int, b string, c boolean)))",
                    "{\"key1\": [{\"a\": 1, \"b\": \"test\", \"c\": true}]}"))
        .isInstanceOf(SFException.class);
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType(
                    "object(a int, b array(int), c map(string, int))",
                    "{\"a\": 1, \"b\": [1, 2, 3], \"c\": {\"key1\": 1}}"))
        .isInstanceOf(SFException.class);
  }

  private void assertStructuredDataType(String dataType, String value) throws Exception {
    String tableName = createIcebergTable(dataType);
    String offsetToken = UUID.randomUUID().toString();

    /* Ingest using streaming ingest */
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    channel.insertRow(
        createStreamingIngestRow(
            value == null ? null : objectMapper.readValue(value, Object.class)),
        offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    /* Verify the data */
    ResultSet res =
        conn.createStatement().executeQuery(String.format("select * from %s", tableName));
    res.next();
    String tmp = res.getString(2);
    JsonNode actualNode = tmp == null ? null : objectMapper.readTree(tmp);
    JsonNode expectedNode = value == null ? null : objectMapper.readTree(value);
    Assertions.assertThat(actualNode).isEqualTo(expectedNode);
  }

  private void assertMap(String dataType, Map<?, ?> value) throws Exception {
    String tableName = createIcebergTable(dataType);
    String offsetToken = UUID.randomUUID().toString();

    /* Ingest using streaming ingest */
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    channel.insertRow(createStreamingIngestRow(value), offsetToken);
    TestUtils.waitForOffset(channel, offsetToken);

    /* Verify the data */
    ResultSet res =
        conn.createStatement().executeQuery(String.format("select * from %s", tableName));
    res.next();
    String tmp = res.getString(2);
    JsonNode actualNode = tmp == null ? null : objectMapper.readTree(tmp);
    JsonNode expectedNode = value == null ? null : objectMapper.valueToTree(value);
    Assertions.assertThat(actualNode).isEqualTo(expectedNode);
  }
}

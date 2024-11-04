/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

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
    assertStructuredDataType("array(string) not null", "[]");
    assertStructuredDataType("map(string, int) not null", "{}");
    assertMap(
        "map(string, int)",
        new HashMap<String, Integer>() {
          {
            put("key", 1);
          }
        });
    assertStructuredDataType("array(string)", null);

    /* Map with null key */
    Assertions.assertThatThrownBy(
            () ->
                assertMap(
                    "map(string, int)",
                    new HashMap<String, Integer>() {
                      {
                        put(null, 1);
                      }
                    }))
        .isInstanceOf(SFException.class)
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    /* Unknown field */
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType("object(a int, b string)", "{\"a\": 1, \"c\": \"test\"}"))
        .isInstanceOf(SFException.class)
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    /* Null struct, map list. */
    Assertions.assertThatThrownBy(
            () -> assertStructuredDataType("object(a int, b string, c boolean) not null", null))
        .isInstanceOf(SFException.class)
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    Assertions.assertThatThrownBy(() -> assertStructuredDataType("map(string, int) not null", null))
        .isInstanceOf(SFException.class)
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    Assertions.assertThatThrownBy(() -> assertStructuredDataType("array(int) not null", null))
        .isInstanceOf(SFException.class)
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testNestedDataType() throws Exception {
    assertStructuredDataType("map(string, map(string, object(a int, b string)))", "{}");
    assertStructuredDataType("array(map(string, int))", "[{}]");
    assertStructuredDataType("array(array(int))", "[[1, 2], [3, 4]]");
    assertStructuredDataType("array(map(string, int))", "[{\"key1\": 1}, {\"key2\": 2}]");
    assertStructuredDataType(
        "array(object(a int, b string, c boolean))", "[{\"a\": 1, \"b\": \"test\", \"c\": true}]");
    assertStructuredDataType(
        "map(string, object(a int, b string, c boolean))",
        "{\"key1\": {\"a\": 1, \"b\": \"test\", \"c\": true}}");
    assertStructuredDataType("map(string, array(int))", "{\"key1\": [1, 2, 3]}");
    assertStructuredDataType("map(string, map(string, int))", "{\"key1\": {\"key2\": 2}}");
    assertStructuredDataType("map(string, array(array(int)))", "{\"key1\": [[1, 2], [3, 4]]}");
    assertStructuredDataType(
        "map(string, array(map(string, int)))", "{\"key1\": [{\"key2\": 2}, {\"key3\": 3}]}");
    assertStructuredDataType(
        "map(string, array(object(a int, b string, c boolean)))",
        "{\"key1\": [{\"a\": 1, \"b\": \"test\", \"c\": true}]}");
    assertStructuredDataType(
        "object(a int, b array(int), c map(string, int))",
        "{\"a\": 1, \"b\": [1, 2, 3], \"c\": {\"key1\": 1}}");
  }

  @Test
  public void testFieldName() throws Exception {
    Iterable<Object> val =
        (Iterable<Object>)
            objectMapper.readValue(
                "["
                    + "{\"test\": 1, \"TEST\": 2, \"TeSt\": 3},"
                    + "{\"test\": 4, \"TEST\": 5, \"TeSt\": 6},"
                    + "{\"test\": 7, \"TEST\": 8, \"TeSt\": 9}"
                    + "]",
                Object.class);
    testIcebergIngestAndQuery(
        "object(test int, TEST int, TeSt int)", val, "select {columnName} from {tableName}", val);

    /* Single row test, check EP info */
    objectMapper.readValue("[{\"test\\.test\": 1, \"TEST\": 2, \"TeSt\": 3}]", Object.class);
    val =
        (Iterable<Object>)
            objectMapper.readValue(
                "[{\"obj\\.obj\": false, \"test_test\": 1, \"test_x5Ftest\": 2, \"obj\\\\.obj\":"
                    + " 3.0, \"❄️\": 4.0, \"5566\": \"5.0\", \"_5566\": \"6.0\", \"_\":"
                    + " \"41424344454647484950\", \"_x27_x44_xFE_x0F\": \"41424344\", \"\\\"\":"
                    + " \"2024-01-01\", \"\\\\\": \"12:00:00\", \"\":"
                    + " \"2024-01-01T12:00:00.000000\", \"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프"
                    + " タイムスタンプ tidsstämpel\": \"2024-01-01T12:00:00.000000+08:00\", \"\\.\":"
                    + " {\"key1\": 1}, \"\\\\.\": [1, 2, 3], \"obj\": {\"obj\": true}}]",
                Object.class);
    testIcebergIngestAndQuery(
        "object("
            + "\"obj.obj\" boolean, "
            + "test_test int, "
            + "test_x5Ftest long, "
            + "\"obj\\.obj\" float, "
            + "\"❄️\" double, "
            + "\"5566\" string, "
            + "_5566 string, "
            + "\"_\" fixed(10), "
            + "_x27_x44_xFE_x0F binary, "
            + "\"\"\"\" date, "
            + "\"\\\" string, "
            + "\"\" string, "
            + "\"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프 タイムスタンプ tidsstämpel\" string, "
            + "\".\" map(string, int),"
            + "\"\\.\" array(int),"
            + "obj object(obj boolean))",
        val,
        "select {columnName} from {tableName}",
        val);

    /* Multiple rows test, check parquet file */
    val =
        (Iterable<Object>)
            objectMapper.readValue(
                "[{\"obj\\.obj\": false, \"test_test\": 1, \"test_x5Ftest\": 2, \"obj\\\\.obj\":"
                    + " 3.0, \"❄️\": 4.0, \"5566\": \"5.0\", \"_5566\": \"6.0\", \"_\":"
                    + " \"41424344454647484950\", \"_x27_x44_xFE_x0F\": \"41424344\", \"\\\"\":"
                    + " \"2024-01-01\", \"\\\\\": \"12:00:00\", \"\":"
                    + " \"2024-01-01T12:00:00.000000\", \"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프"
                    + " タイムスタンプ tidsstämpel\": \"2024-01-01T12:00:00.000000+08:00\", \"\\.\":"
                    + " {\"key1\": 1}, \"\\\\.\": [1, 2, 3], \"obj\": {\"obj\":"
                    + " true}},{\"obj\\.obj\": true, \"test_test\": 2, \"test_x5Ftest\": 3,"
                    + " \"obj\\\\.obj\": 4.0, \"❄️\": 5.0, \"5566\": \"6.0\", \"_5566\": \"7.0\","
                    + " \"_\": \"51525354555657585960\", \"_x27_x44_xFE_x0F\": \"51525354\","
                    + " \"\\\"\": \"2024-01-02\", \"\\\\\": \"13:00:00\", \"\":"
                    + " \"2024-01-02T13:00:00.000000\", \"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프"
                    + " タイムスタンプ tidsstämpel\": \"2024-01-02T13:00:00.000000+08:00\", \"\\.\":"
                    + " {\"key2\": 2}, \"\\\\.\": [4, 5, 6], \"obj\": {\"obj\":"
                    + " false}},{\"obj\\.obj\": false, \"test_test\": 3, \"test_x5Ftest\": 4,"
                    + " \"obj\\\\.obj\": 5.0, \"❄️\": 6.0, \"5566\": \"7.0\", \"_5566\": \"8.0\","
                    + " \"_\": \"61626364656667686970\", \"_x27_x44_xFE_x0F\": \"61626364\","
                    + " \"\\\"\": \"2024-01-03\", \"\\\\\": \"14:00:00\", \"\":"
                    + " \"2024-01-03T14:00:00.000000\", \"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프"
                    + " タイムスタンプ tidsstämpel\": \"2024-01-03T14:00:00.000000+08:00\", \"\\.\":"
                    + " {\"key3\": 3}, \"\\\\.\": [7, 8, 9], \"obj\": {\"obj\": true}}]",
                Object.class);

    testIcebergIngestAndQuery(
        "object("
            + "\"obj.obj\" boolean, "
            + "test_test int, "
            + "test_x5Ftest long, "
            + "\"obj\\.obj\" float, "
            + "\"❄️\" double, "
            + "\"5566\" string, "
            + "_5566 string, "
            + "\"_\" fixed(10), "
            + "_x27_x44_xFE_x0F binary, "
            + "\"\"\"\" date, "
            + "\"\\\" string, "
            + "\"\" string, "
            + "\"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프 タイムスタンプ tidsstämpel\" string, "
            + "\".\" map(string, int),"
            + "\"\\.\" array(int),"
            + "obj object(obj boolean))",
        val,
        "select {columnName} from {tableName}",
        val);
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

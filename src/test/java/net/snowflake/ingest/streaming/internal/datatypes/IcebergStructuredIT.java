/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.datatypes;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(IcebergIT.class)
@RunWith(Parameterized.class)
public class IcebergStructuredIT extends AbstractDataTypeTest {
  @Parameterized.Parameters(name = "compressionAlgorithm={0}, icebergSerializationPolicy={1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"ZSTD", Constants.IcebergSerializationPolicy.COMPATIBLE},
      {"ZSTD", Constants.IcebergSerializationPolicy.OPTIMIZED}
    };
  }

  @Parameterized.Parameter(0)
  public static String compressionAlgorithm;

  @Parameterized.Parameter(1)
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  @Before
  public void before() throws Exception {
    super.setUp(true, compressionAlgorithm, icebergSerializationPolicy);
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
        .hasMessage(
            "The given row cannot be converted to the internal format: Invalid row 0."
                + " missingNotNullColNames=null, extraColNames=null,"
                + " nullValueForNotNullColNames=[VALUE.key_value.key]")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    /* Unknown field */
    Assertions.assertThatThrownBy(
            () ->
                assertStructuredDataType("object(a int, b string)", "{\"a\": 1, \"c\": \"test\"}"))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Invalid row 0."
                + " missingNotNullColNames=null, extraColNames=[VALUE.c],"
                + " nullValueForNotNullColNames=null")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    /* Null struct, map list. */
    Assertions.assertThatThrownBy(
            () -> assertStructuredDataType("object(a int, b string, c boolean) not null", null))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Not-nullable columns with"
                + " null values: [VALUE]. Values for all non-nullable columns must not be null,"
                + " rowIndex:0")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    Assertions.assertThatThrownBy(() -> assertStructuredDataType("map(string, int) not null", null))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Not-nullable columns with"
                + " null values: [VALUE]. Values for all non-nullable columns must not be null,"
                + " rowIndex:0")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    Assertions.assertThatThrownBy(() -> assertStructuredDataType("array(int) not null", null))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Not-nullable columns with"
                + " null values: [VALUE]. Values for all non-nullable columns must not be null,"
                + " rowIndex:0")
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

  @Test
  public void testExtraFields() throws SQLException {
    String tableName = createIcebergTable("object(k1 int)");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);
    Map<String, Object> row = new HashMap<>();
    row.put("k2", 1);
    row.put("k.3", 1);
    row.put("k\\4", 1);
    InsertValidationResponse insertValidationResponse =
        channel.insertRow(createStreamingIngestRow(row), UUID.randomUUID().toString());
    assertThat(insertValidationResponse.getInsertErrors().size()).isEqualTo(1);
    assertThat(insertValidationResponse.getInsertErrors().get(0).getExtraColNames())
        .containsOnly("VALUE.k2", "VALUE.k\\.3", "VALUE.k\\\\4");

    tableName = createIcebergTable("map(string, array(object(k1 int)))");
    channel = openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);
    row.put(
        "key1",
        new ArrayList<Object>() {
          {
            add(
                new HashMap<String, Object>() {
                  {
                    put("k2", 1);
                  }
                });
          }
        });
    insertValidationResponse =
        channel.insertRow(createStreamingIngestRow(row), UUID.randomUUID().toString());
    assertThat(insertValidationResponse.getInsertErrors().size()).isEqualTo(1);
    assertThat(insertValidationResponse.getInsertErrors().get(0).getExtraColNames())
        .containsOnly("VALUE.key_value.value.list.element.k2");
  }

  @Test
  public void testNestedExtraFields() throws SQLException {
    String tableName =
        createIcebergTable(
            "array(map(string, object(k1 int, k2 long, k3 string, k4 boolean, k5 double, k6"
                + " fixed(10), k7 binary, k8 decimal(38, 10), k9 date, k10 time, k11 timestamp, k12"
                + " timestamp_ltz)))");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);
    List<Object> row =
        Collections.singletonList(
            Collections.singletonMap(
                "key",
                new HashMap<String, Object>() {
                  {
                    put("_k1", 1);
                    put("k1", 1);
                    put("_k2", 2);
                    put("k2", 2);
                    put("_k3", "3");
                    put("k3", "3");
                    put("_k4", true);
                    put("k4", true);
                    put("_k5", 5.0);
                    put("k5", 5.0);
                    put("_k6", "41424344454647484950");
                    put("k6", "41424344454647484950");
                    put("_k7", "41424344");
                    put("k7", "41424344");
                    put("_k8", "1234567890.1234567890");
                    put("k8", "1234567890.1234567890");
                    put("_k9", "2024-01-01");
                    put("k9", "2024-01-01");
                    put("_k10", "12:00:00");
                    put("k10", "12:00:00");
                    put("_k11", "2024-01-01T12:00:00.000000");
                    put("k11", "2024-01-01T12:00:00.000000");
                    put("_k12", "2024-01-01T12:00:00.000000+08:00");
                    put("k12", "2024-01-01T12:00:00.000000+08:00");
                  }
                }));
    InsertValidationResponse insertValidationResponse =
        channel.insertRow(createStreamingIngestRow(row), UUID.randomUUID().toString());
    assertThat(insertValidationResponse.getInsertErrors().size()).isEqualTo(1);
    assertThat(insertValidationResponse.getInsertErrors().get(0).getExtraColNames())
        .containsOnly(
            "VALUE.list.element.key_value.value._k1",
            "VALUE.list.element.key_value.value._k2",
            "VALUE.list.element.key_value.value._k3",
            "VALUE.list.element.key_value.value._k4",
            "VALUE.list.element.key_value.value._k5",
            "VALUE.list.element.key_value.value._k6",
            "VALUE.list.element.key_value.value._k7",
            "VALUE.list.element.key_value.value._k8",
            "VALUE.list.element.key_value.value._k9",
            "VALUE.list.element.key_value.value._k10",
            "VALUE.list.element.key_value.value._k11",
            "VALUE.list.element.key_value.value._k12");
  }

  @Test
  public void testMissingFields() throws SQLException {
    String tableName = createIcebergTable("object(k1 int not null, k2 int not null) not null");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.CONTINUE);

    InsertValidationResponse insertValidationResponse =
        channel.insertRow(createStreamingIngestRow(null), UUID.randomUUID().toString());
    assertThat(insertValidationResponse.getInsertErrors().size()).isEqualTo(1);
    assertThat(insertValidationResponse.getInsertErrors().get(0).getNullValueForNotNullColNames())
        .containsOnly("VALUE");

    insertValidationResponse =
        channel.insertRow(
            createStreamingIngestRow(new HashMap<String, Object>()), UUID.randomUUID().toString());
    assertThat(insertValidationResponse.getInsertErrors().size()).isEqualTo(1);
    assertThat(insertValidationResponse.getInsertErrors().get(0).getMissingNotNullColNames())
        .containsOnly("VALUE.k1", "VALUE.k2");

    insertValidationResponse =
        channel.insertRow(
            createStreamingIngestRow(
                new HashMap<String, Object>() {
                  {
                    put("k1", null);
                    put("k2", null);
                  }
                }),
            UUID.randomUUID().toString());
    assertThat(insertValidationResponse.getInsertErrors().size()).isEqualTo(1);
    assertThat(insertValidationResponse.getInsertErrors().get(0).getNullValueForNotNullColNames())
        .containsOnly("VALUE.k1", "VALUE.k2");
  }

  @Test
  public void testMultipleErrors() throws Exception {
    String tableName =
        createIcebergTable(
            "object(k1 int not null, k2 object(k3 int not null, k4 object(k5 int not null) not"
                + " null) not null) not null");
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, OpenChannelRequest.OnErrorOption.ABORT);

    Assertions.assertThatThrownBy(
            () ->
                channel.insertRow(
                    createStreamingIngestRow(
                        new HashMap<String, Object>() {
                          {
                            put("k1", null);
                            put(
                                "k2",
                                new HashMap<String, Object>() {
                                  {
                                    put(
                                        "k4",
                                        new HashMap<String, Object>() {
                                          {
                                            put("k5", null);
                                            put("k7", 1);
                                          }
                                        });
                                    put("k6", null);
                                  }
                                });
                          }
                        }),
                    UUID.randomUUID().toString()))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Invalid row 0. "
                + "missingNotNullColNames=[VALUE.k2.k3], "
                + "extraColNames=[VALUE.k2.k4.k7, VALUE.k2.k6], "
                + "nullValueForNotNullColNames=[VALUE.k1, VALUE.k2.k4.k5]")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
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
    assertThat(actualNode).isEqualTo(expectedNode);
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
    assertThat(actualNode).isEqualTo(expectedNode);
  }
}

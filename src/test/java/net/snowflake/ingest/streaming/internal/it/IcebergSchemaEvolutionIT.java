/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.datatypes.AbstractDataTypeTest;
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
public class IcebergSchemaEvolutionIT extends AbstractDataTypeTest {
  @Parameterized.Parameters(name = "icebergSerializationPolicy={0}")
  public static Object[] parameters() {
    return new Object[] {
      Constants.IcebergSerializationPolicy.COMPATIBLE,
      Constants.IcebergSerializationPolicy.OPTIMIZED
    };
  }

  @Parameterized.Parameter
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  @Before
  public void before() throws Exception {
    super.setUp(true, "ZSTD", icebergSerializationPolicy);
  }

  @Test
  public void testPrimitiveColumns() throws Exception {
    String tableName =
        createIcebergTableWithColumns(
            "id int, int_col int, string_col string, double_col double, boolean_col boolean, "
                + " binary_col binary");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    Map<String, Object> value = new HashMap<>();
    value.put("id", 0L);
    value.put("int_col", 1L);
    value.put("string_col", "2");
    value.put("double_col", 3.0);
    value.put("boolean_col", true);
    value.put("binary_col", "4".getBytes());
    verifyMultipleColumns(
        tableName,
        channel,
        Collections.singletonList(value),
        Collections.singletonList(value),
        "id");

    conn.createStatement()
        .execute(
            String.format(
                "ALTER ICEBERG TABLE %s ADD COLUMN new_int_col int, new_string_col string,"
                    + " new_boolean_col boolean, new_binary_col binary",
                tableName));
    Map<String, Object> newValue = new HashMap<>();
    newValue.put("id", 1L);
    newValue.put("int_col", 2L);
    newValue.put("string_col", "3");
    newValue.put("double_col", 4.0);
    newValue.put("boolean_col", false);
    newValue.put("binary_col", "5".getBytes());
    newValue.put("new_int_col", 6L);
    newValue.put("new_string_col", "7");
    newValue.put("new_boolean_col", true);
    newValue.put("new_binary_col", "8".getBytes());
    Assertions.assertThatThrownBy(
            () ->
                verifyMultipleColumns(
                    tableName,
                    channel,
                    Collections.singletonList(newValue),
                    Arrays.asList(value, newValue),
                    "id"))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: "
                + "Extra columns: [new_binary_col, new_boolean_col, new_int_col, new_string_col]. "
                + "Columns not present in the table shouldn't be specified, rowIndex:0")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    channel.close();

    SnowflakeStreamingIngestChannel newChannel = openChannel(tableName);
    verifyMultipleColumns(
        tableName,
        newChannel,
        Collections.singletonList(newValue),
        Arrays.asList(value, newValue),
        "id");
  }

  @Test
  public void testStructType() throws Exception {
    String tableName = createIcebergTableWithColumns("id int, object_col object(a int)");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    Map<String, Object> value = new HashMap<>();
    value.put("id", 0L);
    value.put("object_col", ImmutableMap.of("a", 1));
    verifyMultipleColumns(
        tableName,
        channel,
        Collections.singletonList(value),
        Collections.singletonList(value),
        "id");

    conn.createStatement()
        .execute(
            String.format(
                "ALTER ICEBERG TABLE %s ALTER COLUMN object_col SET DATA TYPE object(a int, b int)",
                tableName));
    value.put(
        "object_col",
        Collections.unmodifiableMap(
            new HashMap<String, Object>() {
              {
                put("a", 1);
                put("b", null);
              }
            }));
    Map<String, Object> newValue = new HashMap<>();
    newValue.put("id", 1L);
    newValue.put("object_col", ImmutableMap.of("a", 2, "b", 3));
    Assertions.assertThatThrownBy(
            () ->
                verifyMultipleColumns(
                    tableName,
                    channel,
                    Collections.singletonList(newValue),
                    Arrays.asList(value, newValue),
                    "id"))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Invalid row 0."
                + " missingNotNullColNames=null, extraColNames=[OBJECT_COL.b],"
                + " nullValueForNotNullColNames=null")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    channel.close();

    SnowflakeStreamingIngestChannel newChannel = openChannel(tableName);
    verifyMultipleColumns(
        tableName,
        newChannel,
        Collections.singletonList(newValue),
        Arrays.asList(value, newValue),
        "id");
  }

  @Test
  public void testNestedDataType() throws Exception {
    String tableName =
        createIcebergTableWithColumns(
            "id int, object_col object(map_col map(string, array(object(a int))))");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    Map<String, Object> value = new HashMap<>();
    value.put("id", 0L);
    value.put(
        "object_col",
        ImmutableMap.of(
            "map_col", ImmutableMap.of("key", ImmutableList.of((ImmutableMap.of("a", 1))))));
    verifyMultipleColumns(
        tableName,
        channel,
        Collections.singletonList(value),
        Collections.singletonList(value),
        "id");

    conn.createStatement()
        .execute(
            String.format(
                "ALTER ICEBERG TABLE %s ALTER COLUMN object_col SET DATA TYPE object(map_col"
                    + " map(string, array(object(a int, b int))), map_col_2 map(string, int))",
                tableName));
    value.put(
        "object_col",
        Collections.unmodifiableMap(
            new HashMap<String, Object>() {
              {
                put(
                    "map_col",
                    ImmutableMap.of(
                        "key",
                        ImmutableList.of(
                            Collections.unmodifiableMap(
                                new HashMap<String, Object>() {
                                  {
                                    put("a", 1);
                                    put("b", null);
                                  }
                                }))));
                put("map_col_2", null);
              }
            }));

    Map<String, Object> newValue = new HashMap<>();
    newValue.put("id", 1L);
    newValue.put(
        "object_col",
        ImmutableMap.of(
            "map_col",
            ImmutableMap.of("key", ImmutableList.of(ImmutableMap.of("a", 2, "b", 3))),
            "map_col_2",
            ImmutableMap.of("key", 4)));
    Assertions.assertThatThrownBy(
            () ->
                verifyMultipleColumns(
                    tableName,
                    channel,
                    Collections.singletonList(newValue),
                    Arrays.asList(value, newValue),
                    "id"))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Invalid row 0."
                + " missingNotNullColNames=null,"
                + " extraColNames=[OBJECT_COL.map_col.key_value.value.list.element.b,"
                + " OBJECT_COL.map_col_2], nullValueForNotNullColNames=null")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
    channel.close();

    SnowflakeStreamingIngestChannel newChannel = openChannel(tableName);
    verifyMultipleColumns(
        tableName,
        newChannel,
        Collections.singletonList(newValue),
        Arrays.asList(value, newValue),
        "id");
  }

  /** KC Testing pattern */
  @Test
  public void testReopen() throws Exception {
    String tableName = createIcebergTableWithColumns("id int, int_col int");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String channelName = channel.getName();

    Map<String, Object> value = new HashMap<>();
    value.put("id", 0L);
    value.put("int_col", 1L);

    verifyMultipleColumns(
        tableName,
        channel,
        Collections.singletonList(value),
        Collections.singletonList(value),
        "id");

    Map<String, Object> newValue = new HashMap<>();
    newValue.put("id", 1L);
    newValue.put("new_int_col", 2L);
    Assertions.assertThatThrownBy(
            () ->
                verifyMultipleColumns(
                    tableName,
                    channel,
                    Collections.singletonList(newValue),
                    Arrays.asList(value, newValue),
                    "id"))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Extra columns:"
                + " [new_int_col]. Columns not present in the table shouldn't be specified,"
                + " rowIndex:0")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    conn.createStatement()
        .execute(String.format("ALTER ICEBERG TABLE %s ADD COLUMN new_int_col int", tableName));

    Assertions.assertThat(channel.isValid()).isTrue();
    SnowflakeStreamingIngestChannel newChannel = openChannel(tableName);
    Assertions.assertThat(newChannel.getName()).isEqualTo(channelName);
    Assertions.assertThat(channel.isValid()).isFalse();

    verifyMultipleColumns(
        tableName,
        newChannel,
        Collections.singletonList(newValue),
        Arrays.asList(value, newValue),
        "id");
  }

  @Test
  public void testNewFieldId() throws Exception {
    String tableName = createIcebergTableWithColumns("id int, obj_col object(a int, b int)");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);

    Map<String, Object> value = new HashMap<>();
    value.put("id", 0L);
    value.put("obj_col", ImmutableMap.of("a", 1, "b", 2));

    verifyMultipleColumns(
        tableName,
        channel,
        Collections.singletonList(value),
        Collections.singletonList(value),
        "id");

    conn.createStatement()
        .execute(String.format("ALTER ICEBERG TABLE %s ADD COLUMN new_col int", tableName));

    Map<String, Object> newValue = new HashMap<>();
    newValue.put("id", 1L);
    newValue.put("obj_col", ImmutableMap.of("a", 3, "b", 4));
    newValue.put("new_col", 5L);

    Assertions.assertThatThrownBy(
            () ->
                verifyMultipleColumns(
                    tableName,
                    channel,
                    Collections.singletonList(newValue),
                    Arrays.asList(value, newValue),
                    "id"))
        .isInstanceOf(SFException.class)
        .hasMessage(
            "The given row cannot be converted to the internal format: Extra columns: [new_col]."
                + " Columns not present in the table shouldn't be specified, rowIndex:0")
        .extracting("vendorCode")
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    channel.close();
    SnowflakeStreamingIngestChannel newChannel = openChannel(tableName);
    verifyMultipleColumns(
        tableName,
        newChannel,
        Collections.singletonList(newValue),
        Arrays.asList(value, newValue),
        "id");
  }
}

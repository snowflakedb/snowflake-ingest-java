/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.ingest.utils.IcebergDataTypeParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test for Iceberg data type serialization and deserialization. */
public class IcebergDataTypeParserTest {
  private class DataTypeInfo {

    // Json representation of Iceberg data type
    String jsonStr;
    Type icebergType;

    DataTypeInfo(String jsonStr, Type icebergType) {
      this.jsonStr = jsonStr;
      this.icebergType = icebergType;
    }
  }

  private int fieldId = 0;

  List<DataTypeInfo> dataTypesToTest;

  @Before
  public void setup() {
    fieldId = 0;

    // Create a Iceberg data type information list with primitive types and nested types.
    dataTypesToTest = new ArrayList<>();
    dataTypesToTest.add(new DataTypeInfo("\"boolean\"", Types.BooleanType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"int\"", Types.IntegerType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"long\"", Types.LongType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"float\"", Types.FloatType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"double\"", Types.DoubleType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"string\"", Types.StringType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"date\"", Types.DateType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"time\"", Types.TimeType.get()));
    dataTypesToTest.add(new DataTypeInfo("\"timestamptz\"", Types.TimestampType.withZone()));
    dataTypesToTest.add(
        new DataTypeInfo(
            "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"first\",\"required\":false,\"type\":\"int\"},{\"id\":2,\"name\":\"second\",\"required\":false,\"type\":\"int\"}]}",
            Types.StructType.of(
                Types.NestedField.optional(generateFieldId(), "first", Types.IntegerType.get()),
                Types.NestedField.optional(generateFieldId(), "second", Types.IntegerType.get()))));
    dataTypesToTest.add(
        new DataTypeInfo(
            "{\"type\":\"list\",\"element-id\":3,\"element\":\"int\",\"element-required\":false}",
            Types.ListType.ofOptional(generateFieldId(), Types.IntegerType.get())));
    dataTypesToTest.add(
        new DataTypeInfo(
            "{\"type\":\"map\",\"key-id\":4,\"key\":\"int\",\"value-id\":5,\"value\":\"string\",\"value-required\":false}",
            Types.MapType.ofOptional(
                generateFieldId(),
                generateFieldId(),
                Types.IntegerType.get(),
                Types.StringType.get())));
  }

  /** Helper function to generate a unique fieldId for nested types */
  private int generateFieldId() {
    fieldId++;
    return fieldId;
  }

  /** Test for Iceberg data type deserialization. */
  @Test
  public void testDeserializeIcebergType() {
    for (int i = 0; i < dataTypesToTest.size(); i++) {
      DataTypeInfo typeInfo = dataTypesToTest.get(i);
      Type dataType = IcebergDataTypeParser.deserializeIcebergType(typeInfo.jsonStr);
      Assert.assertEquals(typeInfo.icebergType, dataType);
    }
  }

  @Test
  public void testDeserializeIcebergTypeFailed() {
    String json = "bad json";
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergDataTypeParser.deserializeIcebergType(json));
    Assert.assertEquals(
        "Failed to deserialize Iceberg data type: bad json", exception.getMessage());
  }

  @Test
  public void testUnsupportedIcebergType() {
    String json = "{\"type\":\"unsupported\"}";
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergDataTypeParser.deserializeIcebergType(json));
    Assert.assertEquals(
        "Cannot parse Iceberg type: unsupported, schema: {\"type\":\"unsupported\"}",
        exception.getMessage());
  }

  @Test
  public void testOriginalFieldName() {
    String objectJson =
        "{\n"
            + "  \"type\": \"struct\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"id\": 2,\n"
            + "      \"name\": \"obj.obj\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"boolean\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 3,\n"
            + "      \"name\": \"test_test\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"int\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 4,\n"
            + "      \"name\": \"test_x5Ftest\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 5,\n"
            + "      \"name\": \"obj\\\\.obj\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"float\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 6,\n"
            + "      \"name\": \"❄️\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 7,\n"
            + "      \"name\": \"5566\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"decimal(26, 10)\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 8,\n"
            + "      \"name\": \"_5566\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 9,\n"
            + "      \"name\": \"_\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"fixed[10]\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 10,\n"
            + "      \"name\": \"_x27_x44_xFE_x0F\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"binary\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 11,\n"
            + "      \"name\": \"\\\"\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"date\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 12,\n"
            + "      \"name\": \"\\\\\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"time\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 13,\n"
            + "      \"name\": \"\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"timestamp\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 14,\n"
            + "      \"name\": \"временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프 タイムスタンプ tidsstämpel\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": \"timestamptz\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 15,\n"
            + "      \"name\": \".\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": {\n"
            + "        \"type\": \"map\",\n"
            + "        \"key-id\": 16,\n"
            + "        \"key\": \"string\",\n"
            + "        \"value-id\": 17,\n"
            + "        \"value\": \"int\",\n"
            + "        \"value-required\": false\n"
            + "      }\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 18,\n"
            + "      \"name\": \"\\\\.\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": {\n"
            + "        \"type\": \"list\",\n"
            + "        \"element-id\": 19,\n"
            + "        \"element\": \"int\",\n"
            + "        \"element-required\": false\n"
            + "      }\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": 20,\n"
            + "      \"name\": \"obj\",\n"
            + "      \"required\": false,\n"
            + "      \"type\": {\n"
            + "        \"type\": \"struct\",\n"
            + "        \"fields\": [\n"
            + "          {\n"
            + "            \"id\": 21,\n"
            + "            \"name\": \"obj\",\n"
            + "            \"required\": false,\n"
            + "            \"type\": \"boolean\"\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    org.apache.parquet.schema.Type type =
        IcebergDataTypeParser.parseIcebergDataTypeStringToParquetType(
            objectJson, org.apache.parquet.schema.Type.Repetition.OPTIONAL, 1, "OBJ_COL");

    Assertions.assertThat(type.toString())
        .isEqualTo(
            "optional group OBJ_COL = 1 {\n"
                + "  optional boolean obj.obj = 2;\n"
                + "  optional int32 test_test = 3;\n"
                + "  optional int64 test_x5Ftest = 4;\n"
                + "  optional float obj\\.obj = 5;\n"
                + "  optional double ❄️ = 6;\n"
                + "  optional fixed_len_byte_array(11) 5566 (DECIMAL(26,10)) = 7;\n"
                + "  optional binary _5566 (STRING) = 8;\n"
                + "  optional fixed_len_byte_array(10) _ = 9;\n"
                + "  optional binary _x27_x44_xFE_x0F = 10;\n"
                + "  optional int32 \" (DATE) = 11;\n"
                + "  optional int64 \\ (TIME(MICROS,false)) = 12;\n"
                + "  optional int64  (TIMESTAMP(MICROS,false)) = 13;\n"
                + "  optional int64 временнаяметка समयमोहर 時間戳記 ㄕˊㄔㄨㄛ 타임스탬프 タイムスタンプ tidsstämpel"
                + " (TIMESTAMP(MICROS,true)) = 14;\n"
                + "  optional group . (MAP) = 15 {\n"
                + "    repeated group key_value {\n"
                + "      required binary key (STRING) = 16;\n"
                + "      optional int32 value = 17;\n"
                + "    }\n"
                + "  }\n"
                + "  optional group \\. (LIST) = 18 {\n"
                + "    repeated group list {\n"
                + "      optional int32 element = 19;\n"
                + "    }\n"
                + "  }\n"
                + "  optional group obj = 20 {\n"
                + "    optional boolean obj = 21;\n"
                + "  }\n"
                + "}");
  }
}

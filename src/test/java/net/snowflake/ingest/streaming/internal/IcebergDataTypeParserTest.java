/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.ingest.utils.IcebergDataTypeParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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
}

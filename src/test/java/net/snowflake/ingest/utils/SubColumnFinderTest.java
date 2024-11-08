/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class SubColumnFinderTest {

  @Test
  public void testFlatSchema() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message schema {\n"
                + "  optional boolean BOOLEAN_COL = 1;\n"
                + "  optional int32 INT_COL = 2;\n"
                + "  optional int64 LONG_COL = 3;\n"
                + "  optional float FLOAT_COL = 4;\n"
                + "  optional double DOUBLE_COL = 5;\n"
                + "  optional int64 DECIMAL_COL (DECIMAL(10,5)) = 6;\n"
                + "  optional binary STRING_COL (STRING) = 7;\n"
                + "  optional fixed_len_byte_array(10) FIXED_COL = 8;\n"
                + "  optional binary BINARY_COL = 9;\n"
                + "  optional int32 DATE_COL (DATE) = 10;\n"
                + "  optional int64 TIME_COL (TIME(MICROS,false)) = 11;\n"
                + "  optional int64 TIMESTAMP_NTZ_COL (TIMESTAMP(MICROS,false)) = 12;\n"
                + "  optional int64 TIMESTAMP_LTZ_COL (TIMESTAMP(MICROS,true)) = 13;\n"
                + "}\n");
    assertFindSubColumns(schema);
  }

  @Test
  public void testNestedSchema() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message schema {\n"
                + "  optional group LIST_COL (LIST) = 1 {\n"
                + "    repeated group list {\n"
                + "      optional group element = 4 {\n"
                + "        optional group map_col (MAP) = 5 {\n"
                + "          repeated group key_value {\n"
                + "            required binary key (STRING) = 6;\n"
                + "            optional group value (LIST) = 7 {\n"
                + "              repeated group list {\n"
                + "                optional group element = 8 {\n"
                + "                  optional int32 int_col = 9;\n"
                + "                  optional boolean boolean_col = 10;\n"
                + "                  optional group map_col (MAP) = 11 {\n"
                + "                    repeated group key_value {\n"
                + "                      required int32 key = 12;\n"
                + "                      optional int32 value = 13;\n"
                + "                    }\n"
                + "                  }\n"
                + "                }\n"
                + "              }\n"
                + "            }\n"
                + "          }\n"
                + "        }\n"
                + "        optional group obj_col = 14 {\n"
                + "          optional group list_col (LIST) = 15 {\n"
                + "            repeated group list {\n"
                + "              optional int32 element = 16;\n"
                + "            }\n"
                + "          }\n"
                + "          optional group map_col (MAP) = 17 {\n"
                + "            repeated group key_value {\n"
                + "              required binary key (STRING) = 18;\n"
                + "              optional binary value (STRING) = 19;\n"
                + "            }\n"
                + "          }\n"
                + "        }\n"
                + "        optional int32 int_col = 20;\n"
                + "        optional float float_col = 21;\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "  optional group OBJ_COL = 2 {\n"
                + "    optional group obj_col = 22 {\n"
                + "      optional group map_col (MAP) = 23 {\n"
                + "        repeated group key_value {\n"
                + "          required int32 key = 24;\n"
                + "          optional int32 value = 25;\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "  optional double DOUBLE_COL = 3;\n"
                + "}");
    assertFindSubColumns(schema);
  }

  private void assertFindSubColumns(MessageType schema) {
    SubColumnFinder subColumnFinder = new SubColumnFinder(schema);
    for (Type.ID id : getAllPossibleFieldId(schema)) {
      assertThat(subColumnFinder.getSubColumns(id))
          .usingRecursiveComparison()
          .ignoringCollectionOrder()
          .isEqualTo(findSubColumn(schema, id, false));
    }
  }

  private Iterable<Type.ID> getAllPossibleFieldId(MessageType schema) {
    Set<Type.ID> ids = new HashSet<>();
    for (ColumnDescriptor column : schema.getColumns()) {
      String[] path = column.getPath();
      if (path.length == 0) {
        continue;
      }
      for (int i = 1; i < path.length; i++) {
        Type type = schema.getType(Arrays.copyOfRange(path, 0, i));
        if (type.getId() != null) {
          ids.add(type.getId());
        }
      }
    }
    return ids;
  }

  private List<String> findSubColumn(Type node, Type.ID id, boolean isDescendant) {
    if (node.getId() != null && node.getId().equals(id)) {
      isDescendant = true;
    }
    if (node.isPrimitive()) {
      if (isDescendant) {
        return Arrays.asList(node.getId().toString());
      }
      return new ArrayList<>();
    }

    List<String> subColumn = new ArrayList<>();
    for (Type child : node.asGroupType().getFields()) {
      subColumn.addAll(findSubColumn(child, id, isDescendant));
    }
    return subColumn;
  }
}

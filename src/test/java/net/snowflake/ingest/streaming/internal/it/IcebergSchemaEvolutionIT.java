/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.streaming.internal.datatypes.AbstractDataTypeTest;
import net.snowflake.ingest.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class IcebergSchemaEvolutionIT extends AbstractDataTypeTest {
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
  public void testPrimitiveColumns() throws Exception {
    String tableName =
        createIcebergTableWithColumns(
            "id int, int_col int, string_col string, double_col double, boolean_col boolean, "
                + " binary_col binary");
    Map<String, Object> value = new HashMap<>();
    value.put("id", 0L);
    value.put("int_col", 1L);
    value.put("string_col", "2");
    value.put("double_col", 3.0);
    value.put("boolean_col", true);
    value.put("binary_col", "4".getBytes());
    verifyMultipleColumns(
        tableName, Collections.singletonList(value), Collections.singletonList(value), "id");

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
    verifyMultipleColumns(
        tableName, Collections.singletonList(newValue), Arrays.asList(value, newValue), "id");
  }
}

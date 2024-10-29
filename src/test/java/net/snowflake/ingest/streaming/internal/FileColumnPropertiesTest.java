/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class FileColumnPropertiesTest {
  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean enableIcebergStreaming;

  @Test
  public void testFileColumnPropertiesConstructor() {
    // Test simple construction
    RowBufferStats stats =
        enableIcebergStreaming
            ? new RowBufferStats(
                "COL",
                null,
                1,
                1,
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .id(1)
                    .named("test"),
                enableIcebergStreaming,
                enableIcebergStreaming)
            : new RowBufferStats("COL", null, 1, null, null, false, false);
    stats.addStrValue("bcd");
    stats.addStrValue("abcde");
    FileColumnProperties props = new FileColumnProperties(stats, !enableIcebergStreaming);
    Assert.assertEquals(1, props.getColumnOrdinal());
    Assert.assertEquals(enableIcebergStreaming ? 1 : null, props.getFieldId());
    Assert.assertEquals("6162636465", props.getMinStrValue());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertEquals("626364", props.getMaxStrValue());
    Assert.assertNull(props.getMaxStrNonCollated());

    // Test that truncation is performed
    stats =
        enableIcebergStreaming
            ? new RowBufferStats(
                "COL",
                null,
                1,
                1,
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .id(1)
                    .named("test"),
                enableIcebergStreaming,
                enableIcebergStreaming)
            : new RowBufferStats("COL", null, 1, null, null, false, false);
    stats.addStrValue("aßßßßßßßßßßßßßßßß");
    Assert.assertEquals(33, stats.getCurrentMinStrValue().length);
    props = new FileColumnProperties(stats, !enableIcebergStreaming);
    Assert.assertEquals(1, props.getColumnOrdinal());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertNull(props.getMaxStrNonCollated());
    Assert.assertEquals(32 * 2, props.getMinStrValue().length());
    Assert.assertEquals(32 * 2, props.getMaxStrValue().length());
  }
}

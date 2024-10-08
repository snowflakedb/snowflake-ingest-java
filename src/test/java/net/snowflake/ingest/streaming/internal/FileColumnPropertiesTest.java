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
  @Parameterized.Parameters(name = "isIceberg: {0}")
  public static Object[] isIceberg() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean isIceberg;

  @Test
  public void testFileColumnPropertiesConstructor() {
    // Test simple construction
    RowBufferStats stats =
        isIceberg
            ? new RowBufferStats(
                "COL",
                1,
                1,
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .id(1)
                    .named("test"))
            : new RowBufferStats("COL", null, 1);
    stats.addStrValue("bcd");
    stats.addStrValue("abcde");
    FileColumnProperties props = new FileColumnProperties(stats);
    Assert.assertEquals(1, props.getColumnOrdinal());
    Assert.assertEquals(isIceberg ? 1 : null, props.getFieldId());
    Assert.assertEquals("6162636465", props.getMinStrValue());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertEquals("626364", props.getMaxStrValue());
    Assert.assertNull(props.getMaxStrNonCollated());

    // Test that truncation is performed
    stats =
        isIceberg
            ? new RowBufferStats(
                "COL",
                1,
                1,
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .id(1)
                    .named("test"))
            : new RowBufferStats("COL", null, 1);
    stats.addStrValue("aßßßßßßßßßßßßßßßß");
    Assert.assertEquals(33, stats.getCurrentMinStrValue().length);
    props = new FileColumnProperties(stats);
    Assert.assertEquals(1, props.getColumnOrdinal());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertNull(props.getMaxStrNonCollated());
    Assert.assertEquals(32 * 2, props.getMinStrValue().length());
    Assert.assertEquals(32 * 2, props.getMaxStrValue().length());
  }
}

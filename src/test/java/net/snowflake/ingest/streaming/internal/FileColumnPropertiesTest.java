/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

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
    RowBufferStats stats = new RowBufferStats("COL", null, 1, 0);
    stats.addStrValue("bcd");
    stats.addStrValue("abcde");
    FileColumnProperties props = new FileColumnProperties(stats, isIceberg);
    Assert.assertEquals(1, props.getColumnOrdinal());
    Assert.assertEquals("6162636465", props.getMinStrValue());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertEquals("626364", props.getMaxStrValue());
    Assert.assertNull(props.getMaxStrNonCollated());

    // Test that truncation is performed
    stats = new RowBufferStats("COL", null, 1, 0);
    stats.addStrValue("aßßßßßßßßßßßßßßßß");
    Assert.assertEquals(33, stats.getCurrentMinStrValue().length);
    props = new FileColumnProperties(stats, isIceberg);
    Assert.assertEquals(1, props.getColumnOrdinal());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertNull(props.getMaxStrNonCollated());
    Assert.assertEquals(32 * 2, props.getMinStrValue().length());
    Assert.assertEquals(32 * 2, props.getMaxStrValue().length());
  }
}

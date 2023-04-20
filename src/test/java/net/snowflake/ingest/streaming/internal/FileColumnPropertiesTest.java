package net.snowflake.ingest.streaming.internal;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class FileColumnPropertiesTest {

  @Test
  public void testFileColumnPropertiesConstructor() {
    // Test simple construction
    RowBufferStats stats = new RowBufferStats("COL");
    stats.addBinaryValue("bcd".getBytes(StandardCharsets.UTF_8));
    stats.addBinaryValue("abcde".getBytes(StandardCharsets.UTF_8));
    FileColumnProperties props = new FileColumnProperties(stats);
    Assert.assertEquals("6162636465", props.getMinStrValue());
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertEquals("626364", props.getMaxStrValue());
    Assert.assertNull(props.getMaxStrNonCollated());

    // Test that truncation is performed
    stats = new RowBufferStats("COL");
    stats.addBinaryValue("aßßßßßßßßßßßßßßßß".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(33, stats.getCurrentMinStrValue().length);
    props = new FileColumnProperties(stats);
    Assert.assertNull(props.getMinStrNonCollated());
    Assert.assertNull(props.getMaxStrNonCollated());
    Assert.assertEquals(32 * 2, props.getMinStrValue().length());
    Assert.assertEquals(32 * 2, props.getMaxStrValue().length());
  }
}

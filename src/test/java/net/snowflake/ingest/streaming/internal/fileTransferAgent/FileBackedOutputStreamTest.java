package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import org.junit.Test;

public class FileBackedOutputStreamTest {

  @Test
  public void testSmallDataStaysInMemory() throws Exception {
    FileBackedOutputStream stream = new FileBackedOutputStream(1024);
    byte[] data = "hello world".getBytes();
    stream.write(data);
    stream.flush();

    assertNull("Small data should stay in memory", stream.getFile());
    InputStream in = stream.asByteSource().openStream();
    byte[] result = new byte[data.length];
    assertEquals(data.length, in.read(result));
    assertEquals("hello world", new String(result));
    in.close();
    stream.close();
  }

  @Test
  public void testLargeDataSpillsToFile() throws Exception {
    FileBackedOutputStream stream = new FileBackedOutputStream(10, true);
    byte[] data = "this is longer than the threshold of 10 bytes".getBytes();
    stream.write(data);
    stream.flush();

    assertNotNull("Large data should spill to file", stream.getFile());
    assertTrue(stream.getFile().exists());

    InputStream in = stream.asByteSource().openStream();
    byte[] result = new byte[data.length];
    assertEquals(data.length, in.read(result));
    assertEquals(new String(data), new String(result));
    in.close();
    stream.reset();
  }

  @Test
  public void testReset() throws Exception {
    FileBackedOutputStream stream = new FileBackedOutputStream(10, true);
    stream.write("longer than threshold data".getBytes());
    stream.flush();
    assertNotNull(stream.getFile());

    stream.reset();
    assertNull(stream.getFile());
    stream.close();
  }
}

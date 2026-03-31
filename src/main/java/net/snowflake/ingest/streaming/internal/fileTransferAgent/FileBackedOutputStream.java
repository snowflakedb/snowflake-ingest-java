/*
 * Replicated from snowflake-jdbc: net.snowflake.client.jdbc.FileBackedOutputStream
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/FileBackedOutputStream.java
 *
 * Originally from Guava (Copyright 2008 The Guava Authors, Apache 2.0).
 * Adapted by Snowflake to expose getFile().
 *
 * Permitted differences from JDBC source:
 * - Package line
 * - @Beta annotation removed (Guava annotation)
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import com.google.common.io.ByteSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;

public final class FileBackedOutputStream extends OutputStream {

  private static final SFLogger logger = SFLoggerFactory.getLogger(FileBackedOutputStream.class);

  private final int fileThreshold;
  private final boolean resetOnFinalize;
  private final ByteSource source;

  private OutputStream out;
  private MemoryOutput memory;
  private File file;

  /** ByteArrayOutputStream that exposes its internals. */
  private static class MemoryOutput extends ByteArrayOutputStream {
    byte[] getBuffer() {
      return buf;
    }

    int getCount() {
      return count;
    }
  }

  /**
   * @return the file holding the data (possibly null).
   */
  public synchronized File getFile() {
    return file;
  }

  /**
   * Creates a new instance that uses the given file threshold, and does not reset the data when the
   * {@link ByteSource} returned by {@link #asByteSource} is finalized.
   *
   * @param fileThreshold the number of bytes before the stream should switch to buffering to a file
   */
  public FileBackedOutputStream(int fileThreshold) {
    this(fileThreshold, false);
  }

  /**
   * Creates a new instance that uses the given file threshold, and optionally resets the data when
   * the {@link ByteSource} returned by {@link #asByteSource} is finalized.
   *
   * @param fileThreshold the number of bytes before the stream should switch to buffering to a file
   * @param resetOnFinalize if true, the {@link #reset} method will be called when the {@link
   *     ByteSource} returned by {@link #asByteSource} is finalized
   */
  public FileBackedOutputStream(int fileThreshold, boolean resetOnFinalize) {
    this.fileThreshold = fileThreshold;
    this.resetOnFinalize = resetOnFinalize;
    memory = new MemoryOutput();
    out = memory;

    if (resetOnFinalize) {
      source =
          new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
              return openInputStream();
            }

            @Override
            protected void finalize() {
              try {
                reset();
              } catch (Throwable t) {
                logger.error("Exception occurred on finalize", t);
              }
            }
          };
    } else {
      source =
          new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
              return openInputStream();
            }
          };
    }
  }

  /**
   * @return a readable {@link ByteSource} view of the data that has been written to this stream.
   * @since 15.0
   */
  public ByteSource asByteSource() {
    return source;
  }

  private synchronized InputStream openInputStream() throws IOException {
    if (file != null) {
      FileUtil.logFileUsage(file, "Data buffering stream", false);
      return new FileInputStream(file);
    } else {
      return new ByteArrayInputStream(memory.getBuffer(), 0, memory.getCount());
    }
  }

  /**
   * Calls {@link #close} if not already closed, and then resets this object back to its initial
   * state, for reuse. If data was buffered to a file, it will be deleted.
   *
   * @throws IOException if an I/O error occurred while deleting the file buffer
   */
  public synchronized void reset() throws IOException {
    try {
      close();
    } finally {
      if (memory == null) {
        memory = new MemoryOutput();
      } else {
        memory.reset();
      }
      out = memory;
      if (file != null) {
        File deleteMe = file;
        file = null;
        if (!deleteMe.delete()) {
          throw new IOException("Could not delete: " + deleteMe);
        }
      }
    }
  }

  @Override
  public synchronized void write(int b) throws IOException {
    update(1);
    out.write(b);
  }

  @Override
  public synchronized void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    update(len);
    out.write(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    out.close();
  }

  @Override
  public synchronized void flush() throws IOException {
    out.flush();
  }

  /**
   * Checks if writing {@code len} bytes would go over threshold, and switches to file buffering if
   * so.
   */
  private void update(int len) throws IOException {
    if (file == null && (memory.getCount() + len > fileThreshold)) {
      File temp = File.createTempFile("FileBackedOutputStream", null);
      if (resetOnFinalize) {
        // Finalizers are not guaranteed to be called on system shutdown;
        // this is insurance.
        temp.deleteOnExit();
      }
      FileOutputStream transfer = new FileOutputStream(temp);
      transfer.write(memory.getBuffer(), 0, memory.getCount());
      transfer.flush();

      // We've successfully transferred the data; switch to writing to file
      out = transfer;
      file = temp;
      memory = null;
    }
  }
}

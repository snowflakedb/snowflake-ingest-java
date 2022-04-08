package net.snowflake.ingest.streaming.internal;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/** {@link ArrowFileWriter} with configurable {@link CompressionCodec}. */
class ArrowFileWriterWithCompression extends ArrowFileWriter {
  private final VectorUnloader unloader;
  private final boolean compressed;

  ArrowFileWriterWithCompression(
      VectorSchemaRoot root, WritableByteChannel out, CompressionCodec codec) {
    // Note: the dictionary provider is null, this allows to simplify writeBatch() in this class
    // and exclude ensureDictionariesWritten() call from the original ArrowWriter#writeBatch()
    // implementation
    super(root, null, out);

    // we have to create an unloader, different to ArrowWriter#unloader
    // because it is private and we need to configure it with CompressionCodec
    // that is not available in ArrowWriter
    this.unloader = new VectorUnloader(root, true, codec, true);
    this.compressed = codec != NoCompressionCodec.INSTANCE;
  }

  // same as ArrowWriter#writeBatch but does not write dictionaries
  // and uses an unloader configured with a custom CompressionCodec
  @Override
  public void writeBatch() throws IOException {
    start(); // same as ensureStarted() in ArrowWriter#writeBatch()

    // not called as in ArrowWriter#writeBatch
    // because the dictionary provider is always null in ArrowFileWriterWithCompression()
    // constructor
    // ensureDictionariesWritten();

    // ArrowWriter#unloader is private but we need to configure it with CompressionCodec
    // hence we use here a different unloader
    try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
      writeRecordBatch(batch);
      if (compressed) {
        // compression creates new compressed buffers added to the ArrowRecordBatch
        // their reference counter is incremented twice:
        // once in CompressionCodec on create and once in ArrowRecordBatch to retain
        // first the buffers are released here, second in the ArrowRecordBatch#close on try exit
        for (ArrowBuf arrowBuf : batch.getBuffers()) {
          arrowBuf.getReferenceManager().release();
        }
      }
    }
  }
}

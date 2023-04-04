package org.apache.parquet.hadoop;

import net.snowflake.ingest.utils.Constants;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ParquetBdecColumnWriteStore;
import org.apache.parquet.column.impl.ParquetBdecColumnWriter;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.parquet.hadoop.BdecParquetWriter.createParquetProperties;

public class BdecParquetBufferWriter {
    private static final Logger LOG = LoggerFactory.getLogger(BdecParquetBufferWriter.class);

    private final CodecFactory codecFactory;
    private final ParquetFileWriter parquetFileWriter;
    private final Map<String, String> bdecMetadata;

    private final ParquetBdecColumnWriteStore columnStore;

    private final ColumnChunkPageWriteStore pageStore;

    private boolean closed;

    /**
     * Creates a BDEC specific parquet writer.
     *
     * @param stream        output
     * @param schema        row schema
     * @param bdecMetadata
     * @throws IOException
     */
    public BdecParquetBufferWriter(
            ByteArrayOutputStream stream,
            MessageType schema,
            Map<String, String> bdecMetadata,
            BufferAllocator allocator,
            Map<String, Integer> columnWriterNameIndex)
            throws IOException {
        this.bdecMetadata = bdecMetadata;
        ParquetProperties props = createParquetProperties();
        OutputFile file = new ByteArrayOutputFile(stream);
        Configuration conf = new Configuration();
        parquetFileWriter =
                new ParquetFileWriter(
                        file,
                        schema,
                        ParquetFileWriter.Mode.CREATE,
                        Constants.MAX_BLOB_SIZE_IN_BYTES * 2,
                        ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
                        props.getColumnIndexTruncateLength(),
                        props.getStatisticsTruncateLength(),
                        props.getPageWriteChecksumEnabled(),
                        (FileEncryptionProperties) null);
        parquetFileWriter.start();

        /*
        Internally parquet writer initialises CodecFactory with the configured page size.
        We set the page size to the max chunk size that is quite big in general.
        CodecFactory allocates a byte buffer of that size on heap during initialisation.

        If we use Parquet writer for buffering, there will be one writer per channel on each flush.
        The memory will be allocated for each writer at the beginning even if we don't write anything with each writer,
        which is the case when we enable parquet writer buffering.
        Hence, to avoid huge memory allocations, we have to internally initialise CodecFactory with `ParquetWriter.DEFAULT_PAGE_SIZE` as it usually happens.
        To get code access to this internal initialisation, we have to move the BdecParquetWriter class in the parquet.hadoop package.
        */
        codecFactory = new CodecFactory(conf, ParquetWriter.DEFAULT_PAGE_SIZE);
        @SuppressWarnings("deprecation") // Parquet does not support the new one now
        CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(CompressionCodecName.GZIP);
        pageStore = new ColumnChunkPageWriteStore(compressor,
                schema, props.getAllocator(), props.getColumnIndexTruncateLength(), props.getPageWriteChecksumEnabled(),
                parquetFileWriter.getEncryptor(), 0);
        columnStore = new ParquetBdecColumnWriteStore(schema, pageStore, allocator, columnWriterNameIndex);
    }

    public ParquetBdecColumnWriter getColumnWriter(int index) {
        return columnStore.getColumnWriter(index);
    }

    public void rowAdded() {
        columnStore.endRecord();
    }

    public void close() throws IOException {
        if (!closed) {
            flushRowGroupToStore();
            parquetFileWriter.end(bdecMetadata);
            closed = true;
        }
        codecFactory.release();
    }

    /**
     * @return the total size of data written to the file and buffered in memory
     */
    public long getDataSize() {
        return columnStore.getBufferedSize();
    }

    private void flushRowGroupToStore()
            throws IOException {
        LOG.debug("Flushing mem columnStore to file. allocated memory: {}", columnStore.getAllocatedSize());
        if (columnStore.getAllocatedSize() > (3 * Constants.MAX_BLOB_SIZE_IN_BYTES * 2)) {
            LOG.warn("Too much memory used: {}", columnStore.memUsageString());
        }
        parquetFileWriter.startBlock(columnStore.getRowCount());
        columnStore.flush();
        pageStore.flushToFileWriter(parquetFileWriter);
        parquetFileWriter.endBlock();
    }

    public void appendWriter(BdecParquetBufferWriter other) {
        columnStore.appendStore(other.columnStore);
    }
}

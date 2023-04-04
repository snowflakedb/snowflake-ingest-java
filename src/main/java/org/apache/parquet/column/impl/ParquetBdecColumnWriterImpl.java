package org.apache.parquet.column.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter;
import org.apache.parquet.hadoop.ParquetBdecBitLengthWriter;
import org.apache.parquet.hadoop.ParquetBdecPlainValueWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.parquet.bytes.BytesInput.concat;

public class ParquetBdecColumnWriterImpl implements ParquetBdecColumnWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetBdecColumnWriterImpl.class);

    // By default, Debugging disabled this way (using the "//if (DEBUG)" IN the methods) to allow
    // the java compiler (not the JIT) to remove the unused statements during build time.
    private static final boolean DEBUG = false;

    private Statistics<?> statistics;
    final ColumnDescriptor path;
    final PageWriter pageWriter;
    private final ValuesWriter repetitionLevelColumn;
    private final ValuesWriter definitionLevelColumn;
    private final ParquetBdecPlainValueWriter dataColumn;
    private int valueCount;

    private int pageRowCount;

    public ParquetBdecColumnWriterImpl(ColumnDescriptor path,
                                       PageWriter pageWriter,
                                       BufferAllocator allocator) {
        this.path = path;
        this.pageWriter = pageWriter;
        resetStatistics();

        this.repetitionLevelColumn = path.getMaxRepetitionLevel() == 0 ? new DevNullValuesWriter() : new ParquetBdecBitLengthWriter(allocator);
        this.definitionLevelColumn = path.getMaxDefinitionLevel() == 0 ? new DevNullValuesWriter() : new ParquetBdecBitLengthWriter(allocator);
        this.dataColumn = new ParquetBdecPlainValueWriter(allocator);
    }

    private void log(Object value) {
        LOG.debug("{} {}", path, value);
    }

    private void resetStatistics() {
        this.statistics = Statistics.createStats(path.getPrimitiveType());
    }

    /**
     * Writes the current null value
     */
    @Override
    public void writeNull() {
        //repetitionLevelColumn.writeBoolean(false);
        definitionLevelColumn.writeBoolean(false);
        ++pageRowCount;
        //statistics.incrementNumNulls();
        ++valueCount;
    }

    @Override
    public void close() {
        // Close the Values writers.
        repetitionLevelColumn.close();
        definitionLevelColumn.close();
        dataColumn.close();
    }

    @Override
    public long getBufferedSizeInMemory() {
        return repetitionLevelColumn.getBufferedSize()
                + definitionLevelColumn.getBufferedSize()
                + dataColumn.getBufferedSize()
                + pageWriter.getMemSize();
    }

    /**
     * Writes the current value
     */
    @Override
    public void write(double value) {
        //if (DEBUG)
            //log(value);
        //repetitionLevelColumn.writeBoolean(false);
        //definitionLevelColumn.writeBoolean(true);
        ++pageRowCount;
        dataColumn.writeDouble(value);
        //statistics.updateStats(value);
        ++valueCount;
    }

    /**
     * Writes the current value
     */
    @Override
    public void write(float value) {
        //if (DEBUG)
            //log(value);
        //repetitionLevelColumn.writeBoolean(false);
        //definitionLevelColumn.writeBoolean(true);
        ++pageRowCount;
        dataColumn.writeFloat(value);
        //statistics.updateStats(value);
        ++valueCount;
    }

    @Override
    public void write(byte[] value) {
        //if (DEBUG)
            //log(value);
        //repetitionLevelColumn.writeBoolean(false);
        //definitionLevelColumn.writeBoolean(true);
        ++pageRowCount;
        dataColumn.writeBytes(value);
        //statistics.updateStats(Binary.fromConstantByteArray(value));
        ++valueCount;
    }

    @Override
    public void write(String value) {
        write(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Writes the current value
     */
    @Override
    public void write(boolean value) {
        //if (DEBUG)
            //log(value);
        //repetitionLevelColumn.writeBoolean(false);
        //definitionLevelColumn.writeBoolean(true);
        ++pageRowCount;
        dataColumn.writeBoolean(value);
        //statistics.updateStats(value);
        ++valueCount;
    }

    /**
     * Writes the current value
     */
    @Override
    public void write(int value) {
        //if (DEBUG)
            //log(value);
        //repetitionLevelColumn.writeBoolean(false);
        //definitionLevelColumn.writeBoolean(true);
        ++pageRowCount;
        dataColumn.writeInteger(value);
        //statistics.updateStats(value);
        ++valueCount;
    }

    /**
     * Writes the current value
     */
    @Override
    public void write(long value) {
        //if (DEBUG)
            //log(value);
        //repetitionLevelColumn.writeBoolean(false);
        //definitionLevelColumn.writeBoolean(true);
        ++pageRowCount;
        dataColumn.writeLong(value);
        //statistics.updateStats(value);
        ++valueCount;
    }

    /**
     * Used to decide when to write a page or row group
     *
     * @return the number of bytes of memory used to buffer the current data and the previously written pages
     */
    long getTotalBufferedSize() {
        return repetitionLevelColumn.getBufferedSize()
                + definitionLevelColumn.getBufferedSize()
                + dataColumn.getBufferedSize()
                + pageWriter.getMemSize();
    }

    /**
     * @return actual memory used
     */
    long allocatedSize() {
        return repetitionLevelColumn.getAllocatedSize()
                + definitionLevelColumn.getAllocatedSize()
                + dataColumn.getAllocatedSize()
                + pageWriter.allocatedSize();
    }

    /**
     * @param indent a prefix to format lines
     * @return a formatted string showing how memory is used
     */
    String memUsageString(String indent) {
        StringBuilder b = new StringBuilder(indent).append(path).append(" {\n");
        b.append(indent).append(" r:").append(repetitionLevelColumn.getAllocatedSize()).append(" bytes\n");
        b.append(indent).append(" d:").append(definitionLevelColumn.getAllocatedSize()).append(" bytes\n");
        b.append(dataColumn.memUsageString(indent + "  data:")).append("\n");
        b.append(pageWriter.memUsageString(indent + "  pages:")).append("\n");
        b.append(indent).append(String.format("  total: %,d/%,d", getTotalBufferedSize(), allocatedSize())).append("\n");
        b.append(indent).append("}\n");
        return b.toString();
    }

    void writePage() {
        writePage(pageWriter);
    }

    /**
     * Writes the current data to a new page in the page store
     */
    void writePage(PageWriter pageWriter) {
        if (pageRowCount == 0) {
            return;
        }
        LOG.debug("write page");
        try {
            writePage(pageWriter, pageRowCount, valueCount, statistics, repetitionLevelColumn, definitionLevelColumn, dataColumn);
        } catch (IOException e) {
            throw new ParquetEncodingException("could not write page for " + path, e);
        }
        repetitionLevelColumn.reset();
        definitionLevelColumn.reset();
        dataColumn.reset();
        valueCount = 0;
        resetStatistics();
        pageRowCount = 0;
    }

    void writePage(PageWriter pageWriter, int rowCount, int valueCount, Statistics<?> statistics, ValuesWriter repetitionLevels,
                   ValuesWriter definitionLevels, ValuesWriter values) throws IOException {
        pageWriter.writePage(
                concat(repetitionLevels.getBytes(), definitionLevels.getBytes(), values.getBytes()),
                valueCount,
                rowCount,
                statistics,
                repetitionLevels.getEncoding(),
                definitionLevels.getEncoding(),
                values.getEncoding());
    }
}

package org.apache.parquet.column.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class ParquetBdecColumnWriteStore {
    private final ParquetBdecColumnWriterImpl[] columnWriters;
    private long rowCount;

    public ParquetBdecColumnWriteStore(MessageType schema, PageWriteStore pageWriteStore, BufferAllocator allocator, Map<String, Integer> columnWriterNameIndex) {
        this.columnWriters = new ParquetBdecColumnWriterImpl[columnWriterNameIndex.size()];
        for (ColumnDescriptor path : schema.getColumns()) {
            PageWriter pageWriter = pageWriteStore.getPageWriter(path);
            int index = columnWriterNameIndex.get(path.getPath()[0]);
            columnWriters[index] = new ParquetBdecColumnWriterImpl(path, pageWriter, allocator);
        }
    }

    public ParquetBdecColumnWriter getColumnWriter(int index) {
        return columnWriters[index];
    }

    public long getBufferedSize() {
        long total = 0;
        for (ParquetBdecColumnWriterImpl memColumn : columnWriters) {
            total += memColumn.getTotalBufferedSize();
        }
        return total;
    }

    public void endRecord() {
        ++rowCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getAllocatedSize() {
        long total = 0;
        for (ParquetBdecColumnWriterImpl memColumn : columnWriters) {
            total += memColumn.allocatedSize();
        }
        return total;
    }

    public String memUsageString() {
        StringBuilder b = new StringBuilder("Store {\n");
        for (ParquetBdecColumnWriterImpl memColumn : columnWriters) {
            b.append(memColumn.memUsageString(" "));
        }
        b.append("}\n");
        return b.toString();
    }

    public void flush() {
        for (ParquetBdecColumnWriterImpl memColumn : columnWriters) {
            memColumn.writePage();
            memColumn.close();
        }
    }

    public void appendStore(ParquetBdecColumnWriteStore other) {
        assert other.columnWriters.length == columnWriters.length;
        for (int i = 0; i < columnWriters.length; i++) {
            other.columnWriters[i].writePage(columnWriters[i].pageWriter);
            other.columnWriters[i].close();
        }
        rowCount += other.rowCount;
    }
}

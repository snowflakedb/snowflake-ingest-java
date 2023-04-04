package org.apache.parquet.hadoop;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;

public class ParquetBdecValueWriter extends ValuesWriter {
    public static final String MAX_ALLOCATION_SIZE_PROPERTY = "arrow.vector.max_allocation_bytes";
    public static final long MAX_ALLOCATION_SIZE = Long.getLong(MAX_ALLOCATION_SIZE_PROPERTY, Long.MAX_VALUE);
    private static final int MAX_BUFFER_SIZE = (int) Math.min(MAX_ALLOCATION_SIZE, Integer.MAX_VALUE);

    private final BufferAllocator allocator;

    public static final byte LEN_BYTE_WIDTH = 4;

    ArrowBuf valueBuffer;

    public ParquetBdecValueWriter(BufferAllocator allocator) {
        this.allocator = allocator;
        valueBuffer = allocator.getEmpty();
    }

    @Override
    public long getBufferedSize() {
        return valueBuffer.writerIndex();
    }

    @Override
    public BytesInput getBytes() {
        return new BdecArrowBytesInput(valueBuffer);
    }

    @Override
    public Encoding getEncoding() {
        return Encoding.PLAIN;
    }

    @Override
    public void reset() {
        valueBuffer.clear();
    }

    @Override
    public long getAllocatedSize() {
        return valueBuffer.capacity();
    }

    @Override
    public String memUsageString(String prefix) {
        return "Used " + valueBuffer.getActualMemoryConsumed() + " bytes";
    }

    void handleSafe(long startOffset, int dataLength) {
        long desiredAllocSize = startOffset + dataLength;
        if (valueBuffer.capacity() < desiredAllocSize) {
            if (desiredAllocSize == 0) {
                return;
            }

            long newAllocationSize = CommonUtil.nextPowerOfTwo(desiredAllocSize);
            if (newAllocationSize > MAX_BUFFER_SIZE || newAllocationSize <= 0) {
                throw new OversizedAllocationException("Memory required for vector " +
                        "is (" + newAllocationSize + "), which is overflow or more than max allowed (" + MAX_BUFFER_SIZE + "). " +
                        "You could consider using LargeVarCharVector/LargeVarBinaryVector for large strings/large bytes types");
            }

            final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
            newBuf.setBytes(0, valueBuffer, 0, valueBuffer.capacity());
            newBuf.writerIndex(valueBuffer.writerIndex());
            valueBuffer.getReferenceManager().release();
            valueBuffer = newBuf;
        }
    }

    @Override
    public void close() {
        super.close();
        valueBuffer.getReferenceManager().release();
    }
}

package org.apache.parquet.hadoop;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;

public class ParquetBdecPlainValueWriter extends ParquetBdecValueWriter {
    private int index;

    public ParquetBdecPlainValueWriter(BufferAllocator allocator) {
        super(allocator);
    }

    public int getIndex() {
        return index;
    }

    @Override
    public void reset() {
        super.reset();
        index = 0;
    }

    @Override
    public void writeBoolean(boolean v) {
        int dataLength = LEN_BYTE_WIDTH + 1;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        BitVectorHelper.setBit(valueBuffer, index);
        index++;
    }

    @Override
    public void writeInteger(int v) {
        int dataLength = LEN_BYTE_WIDTH + Integer.BYTES;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeInt(v);
        index++;
    }

    @Override
    public void writeLong(long v) {
        int dataLength = LEN_BYTE_WIDTH + Long.BYTES;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeLong(v);
        index++;
    }

    @Override
    public void writeDouble(double v) {
        int dataLength = LEN_BYTE_WIDTH + Double.BYTES;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeDouble(v);
        index++;
    }

    @Override
    public void writeFloat(float v) {
        int dataLength = LEN_BYTE_WIDTH + Float.BYTES;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeFloat(v);
        index++;
    }

    public void writeBytes(byte[] v) {
        int dataLength = LEN_BYTE_WIDTH + v.length;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeInt(v.length);
        valueBuffer.writeBytes(v, 0, v.length);
        index++;
    }
}

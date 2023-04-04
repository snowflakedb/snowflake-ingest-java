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
        handleSafe(valueBuffer.writerIndex(), 1);
        if (v) {
            BitVectorHelper.setBit(valueBuffer, index);
        } else {
            BitVectorHelper.unsetBit(valueBuffer, index);
        }
        valueBuffer.writerIndex((index / 8) + 1);
        index++;
    }

    @Override
    public void writeInteger(int v) {
        handleSafe(valueBuffer.writerIndex(), Integer.BYTES);
        valueBuffer.writeInt(v);
        index++;
    }

    @Override
    public void writeLong(long v) {
        handleSafe(valueBuffer.writerIndex(), Long.BYTES);
        valueBuffer.writeLong(v);
        index++;
    }

    @Override
    public void writeDouble(double v) {
        handleSafe(valueBuffer.writerIndex(), Double.BYTES);
        valueBuffer.writeDouble(v);
        index++;
    }

    @Override
    public void writeFloat(float v) {
        handleSafe(valueBuffer.writerIndex(), Float.BYTES);
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

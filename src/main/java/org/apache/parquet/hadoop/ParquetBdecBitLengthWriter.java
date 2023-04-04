package org.apache.parquet.hadoop;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;

public class ParquetBdecBitLengthWriter extends ParquetBdecValueWriter {
    private boolean previousValue;

    private byte bufferedValues;
    private int numBufferedValues;

    private int repeatCount;

    private int bitPackedGroupCount;

    private long bitPackedRunHeaderPointer;

    private boolean toBytesCalled;

    public ParquetBdecBitLengthWriter(BufferAllocator allocator) {
        super(allocator);
        this.bufferedValues = 0;
        reset();
    }

    @Override
    public void reset() {
        super.reset();
        this.previousValue = false;
        this.numBufferedValues = 0;
        this.repeatCount = 0;
        this.bitPackedGroupCount = 0;
        this.bitPackedRunHeaderPointer = -1;
        this.toBytesCalled = false;
        bufferedValues = 0;
    }

    @Override
    public void writeInteger(int v) {
        writeBoolean(v > 0);
    }

    @Override
    public void writeBoolean(boolean value) {
        if (value == previousValue) {
            // keep track of how many times we've seen this value
            // consecutively
            ++repeatCount;

            if (repeatCount >= 8) {
                // we've seen this at least 8 times, we're
                // certainly going to write an rle-run,
                // so just keep on counting repeats for now
                return;
            }
        } else {
            // This is a new value, check if it signals the end of
            // an rle-run
            if (repeatCount >= 8) {
                // it does! write an rle-run
                writeRleRun();
            }

            // this is a new value so we've only seen it once
            repeatCount = 1;
            // start tracking this value for repeats
            previousValue = value;
        }

        // We have not seen enough repeats to justify an rle-run yet,
        // so buffer this value in case we decide to write a bit-packed-run
        bufferedValues = (byte)(value ? bufferedValues & (1 << numBufferedValues) : bufferedValues & ~(1 << numBufferedValues));
        ++numBufferedValues;

        if (numBufferedValues == 8) {
            // we've encountered less than 8 repeated values, so
            // either start a new bit-packed-run or append to the
            // current bit-packed-run
            writeOrAppendBitPackedRun();
        }
    }

    private void writeOrAppendBitPackedRun() {
        if (bitPackedGroupCount >= 63) {
            // we've packed as many values as we can for this run,
            // end it and start a new one
            endPreviousBitPackedRun();
        }

        if (bitPackedRunHeaderPointer == -1) {
            // this is a new bit-packed-run, allocate a byte for the header
            // and keep a "pointer" to it so that it can be mutated later
            writeByteInternal((byte)0); // write a sentinel value
            bitPackedRunHeaderPointer = valueBuffer.writerIndex();
        }

        writeByteInternal(bufferedValues);

        // empty the buffer, they've all been written
        bufferedValues = 0;
        numBufferedValues = 0;

        // clear the repeat count, as some repeated values
        // may have just been bit packed into this run
        repeatCount = 0;

        ++bitPackedGroupCount;
    }

    private void endPreviousBitPackedRun() {
        if (bitPackedRunHeaderPointer == -1) {
            // we're not currently in a bit-packed-run
            return;
        }

        // create bit-packed-header, which needs to fit in 1 byte
        byte bitPackHeader = (byte) ((bitPackedGroupCount << 1) | 1);

        // update this byte
        valueBuffer.setByte(bitPackedRunHeaderPointer, bitPackHeader);

        // mark that this run is over
        bitPackedRunHeaderPointer = -1;

        // reset the number of groups
        bitPackedGroupCount = 0;
    }

    private void writeRleRun() {
        // we may have been working on a bit-packed-run
        // so close that run if it exists before writing this
        // rle-run
        endPreviousBitPackedRun();

        // write the rle-header (lsb of 0 signifies a rle run)
        writeUnsignedVarInt(repeatCount << 1);
        // write the repeated-value
        writeIntInternal((previousValue ? 1 : 0) & 0xFF);

        // reset the repeat count
        repeatCount = 0;

        // throw away all the buffered values, they were just repeats and they've been written
        numBufferedValues = 0;
    }

    private void writeUnsignedVarInt(int value) {
        while ((value & 0xFFFFFF80) != 0L) {
            writeIntInternal((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        writeIntInternal(value & 0x7F);
    }

    private void writeByteInternal(byte b) {
        int dataLength = LEN_BYTE_WIDTH + Integer.BYTES;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeByte(b);
    }

    private void writeIntInternal(int i) {
        int dataLength = LEN_BYTE_WIDTH + Integer.BYTES;
        handleSafe(valueBuffer.writerIndex(), dataLength);
        valueBuffer.writeInt(i);
    }

    @Override
    public BytesInput getBytes() {
        Preconditions.checkArgument(!toBytesCalled,
                "You cannot call toBytes() more than once without calling reset()");

        // write anything that is buffered / queued up for an rle-run
        if (repeatCount >= 8) {
            writeRleRun();
        } else if(numBufferedValues > 0) {
            bufferedValues = 0;
            writeOrAppendBitPackedRun();
            endPreviousBitPackedRun();
        } else {
            endPreviousBitPackedRun();
        }

        toBytesCalled = true;
        return super.getBytes();
    }

    @Override
    public Encoding getEncoding() {
        return Encoding.RLE;
    }
}

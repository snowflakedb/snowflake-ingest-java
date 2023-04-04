package org.apache.parquet.hadoop;

import org.apache.arrow.memory.ArrowBuf;

public class BdecBytesUtils {
    public static void writeUnsignedVarInt(int value, ArrowBuf buffer) {
        while ((value & 0xFFFFFF80) != 0L) {
            buffer.writeInt((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer.writeInt(value & 0x7F);
    }
}

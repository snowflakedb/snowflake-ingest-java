package net.snowflake.ingest.utils;

import org.apache.arrow.memory.ArrowBuf;

import java.io.ByteArrayOutputStream;

public class EnhancedByteArrayOutputStream extends ByteArrayOutputStream {
    public void write(ArrowBuf arrowBuf, long index, int len) {
        arrowBuf.getBytes(index, buf, count, len);
        count += len;
    }
}

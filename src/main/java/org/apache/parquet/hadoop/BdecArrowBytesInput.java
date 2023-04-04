package org.apache.parquet.hadoop;

import net.snowflake.ingest.utils.EnhancedByteArrayOutputStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.parquet.bytes.BytesInput;

import java.io.IOException;
import java.io.OutputStream;

public class BdecArrowBytesInput extends BytesInput {
    private final ArrowBuf buffer;

    public BdecArrowBytesInput(ArrowBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
        if (out instanceof EnhancedByteArrayOutputStream) {
            ((EnhancedByteArrayOutputStream)out).write(buffer, 0, (int) buffer.writerIndex());
        } else {
            buffer.getBytes(0, out, (int) buffer.writerIndex());
        }
    }

    @Override
    public long size() {
        return buffer.writerIndex();
    }
}

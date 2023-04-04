package org.apache.parquet.hadoop;

import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static net.snowflake.ingest.utils.Constants.MAX_CHUNK_SIZE_IN_BYTES;

/**
 * A parquet specific file output implementation.
 *
 * <p>This class is implemented as parquet library API requires, mostly to create our {@link
 * ByteArrayDelegatingPositionOutputStream} implementation.
 */
class ByteArrayOutputFile implements OutputFile {
    private final ByteArrayOutputStream stream;

    ByteArrayOutputFile(ByteArrayOutputStream stream) {
        this.stream = stream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        stream.reset();
        return new ByteArrayDelegatingPositionOutputStream(stream);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return (int) MAX_CHUNK_SIZE_IN_BYTES;
    }

    /**
     * A parquet specific output stream implementation.
     *
     * <p>This class is implemented as parquet library API requires, mostly to wrap our BDEC output
     * {@link ByteArrayOutputStream}.
     */
    private static class ByteArrayDelegatingPositionOutputStream
            extends DelegatingPositionOutputStream {
        private final ByteArrayOutputStream stream;

        public ByteArrayDelegatingPositionOutputStream(ByteArrayOutputStream stream) {
            super(stream);
            this.stream = stream;
        }

        @Override
        public long getPos() {
            return stream.size();
        }
    }
}

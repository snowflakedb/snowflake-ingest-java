package org.apache.parquet.hadoop;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.bytes.ByteBufferAllocator;

class ArrowByteBufferAllocator implements ByteBufferAllocator, AutoCloseable {
  private final BufferAllocator arrowAllocator;
  // we use IdentityHashMap that indexes by object mem reference
  // because ByteBuffer#hashCode() depends on its state, hence it is unstable to use it with HashMap
  private final Map<ByteBuffer, ArrowBuf> nioBufferRefToArrowBufferMap;
  private final boolean verifyBeforeClose;
  private boolean isClosed;

  ArrowByteBufferAllocator(BufferAllocator arrowAllocator, boolean verifyBeforeClose) {
    this.arrowAllocator = arrowAllocator;
    this.nioBufferRefToArrowBufferMap = new IdentityHashMap<>();
    this.verifyBeforeClose = verifyBeforeClose;
  }

  @Override
  public synchronized ByteBuffer allocate(int size) {
    if (isClosed) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Allocation on closed Parquet ByteBufferAllocator");
    }
    ArrowBuf arrowBuffer = arrowAllocator.buffer(size);
    ByteBuffer nioBuffer = arrowBuffer.nioBuffer(0, size);
    nioBufferRefToArrowBufferMap.put(nioBuffer, arrowBuffer);
    return nioBuffer;
  }

  @Override
  public synchronized void release(ByteBuffer buffer) {
    if (isClosed) {
      throw new SFException(
              ErrorCode.INTERNAL_ERROR, "Release on closed Parquet ByteBufferAllocator");
    }
    ArrowBuf buf = nioBufferRefToArrowBufferMap.remove(buffer);
    if (buf != null) {
      buf.getReferenceManager().release();
    }
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public synchronized void close() {
    if (verifyBeforeClose && !nioBufferRefToArrowBufferMap.isEmpty()) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Non-empty Parquet ByteBufferAllocator on close");
    }
    isClosed = true;
    nioBufferRefToArrowBufferMap.values().forEach(b -> b.getReferenceManager().release());
    nioBufferRefToArrowBufferMap.clear();
  }
}

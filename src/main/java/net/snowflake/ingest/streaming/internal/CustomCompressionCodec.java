package net.snowflake.ingest.streaming.internal;

import org.apache.arrow.compression.Lz4CompressionCodec;
import org.apache.arrow.compression.ZstdCompressionCodec;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * This is custom wrapper on top of {@link CompressionCodec} implementations.
 *
 * <p>The main stream {@link CompressionCodec} implementations are based on {@link
 * org.apache.arrow.vector.compression.AbstractCompressionCodec}. This implementation is similar to
 * it with some tweaks.
 *
 * <p>The reason to have this custom implementation is to fix certain issues with mainstream codecs
 * that lead to buffer reference counting problems due to creation of new compressed buffers and
 * releasing uncompressed. The release of uncompressed buffer from the original vectors to write
 * into file results in double release when the original vector gets released as usual.
 *
 * <p>The custom code extraction is not optimal in a long term and there is a TODO: to follow up on
 * this.
 */
public class CustomCompressionCodec implements CompressionCodec {
  private final CompressionCodec actualCodec;

  public CustomCompressionCodec(CompressionUtil.CodecType codecType) {
    switch (codecType) {
      case LZ4_FRAME:
        // TODO: LZ4 compression currently breaks server side decompression
        actualCodec =
            new Lz4CompressionCodec() {
              @Override
              public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
                return doCompress(allocator, uncompressedBuffer);
              }
            };
        break;
      case ZSTD:
        actualCodec =
            new ZstdCompressionCodec() {
              @Override
              public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
                return doCompress(allocator, uncompressedBuffer);
              }
            };
        break;
      case NO_COMPRESSION:
        actualCodec = new CustomNoCompressionCodec();
        break;
      default:
        throw new IllegalArgumentException("unsupported compression type: " + codecType);
    }
  }

  @Override
  public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    long startTime = System.nanoTime();
    if (uncompressedBuffer.writerIndex() == 0L) {
      // shortcut for empty buffer
      ArrowBuf compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      compressedBuffer.setLong(0, 0);
      compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      uncompressedBuffer.close();
      return compressedBuffer;
    }

    ArrowBuf compressedBuffer = actualCodec.compress(allocator, uncompressedBuffer);
    // Here we simplify the original AbstractCompressionCodec#compress
    // and always write the compressed data even if the compression does not give anything
    // in case of e.g. random integers.
    // The reason is that mixing compressed and uncompressed buffers breaks the server side
    // decompression that happens to expect only compressed buffers for version 8.0.0.
    long uncompressedLength = uncompressedBuffer.writerIndex();
    writeUncompressedLength(compressedBuffer, uncompressedLength);

    CompressionTime.compressionTimeNano += (System.nanoTime() - startTime);

    // Here we again simplify the original AbstractCompressionCodec#compress
    // and do not release the uncompressedBuffer because it is part of the root vector to write.
    // This root vector will be released as usual by client.
    return compressedBuffer;
  }

  @Override
  public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    throw new UnsupportedOperationException("decompress is not supported");
  }

  protected void writeUncompressedLength(ArrowBuf compressedBuffer, long uncompressedLength) {
    if (!MemoryUtil.LITTLE_ENDIAN) {
      uncompressedLength = Long.reverseBytes(uncompressedLength);
    }
    // first 8 bytes reserved for uncompressed length, according to the specification
    compressedBuffer.setLong(0, uncompressedLength);
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return actualCodec.getCodecType();
  }

  private static class CustomNoCompressionCodec implements CompressionCodec {
    @Override
    public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
      return CompressionUtil.packageRawBuffer(allocator, uncompressedBuffer);
    }

    @Override
    public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
      return compressedBuffer;
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return CompressionUtil.CodecType.NO_COMPRESSION;
    }
  }
}

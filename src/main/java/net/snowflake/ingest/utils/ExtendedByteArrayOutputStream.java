package net.snowflake.ingest.utils;

import java.io.ByteArrayOutputStream;

/* An extension of ByteArrayOutputStream to access buffer without using toByteArray() to avoid copy */
public class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {
  public ExtendedByteArrayOutputStream() {
    super();
  }

  public ExtendedByteArrayOutputStream(int size) {
    super(size);
  }

  /* The returned byte array does not match the length of OutputStream, size() should be called to get the length */
  public byte[] getBytes() {
    return buf;
  }
}

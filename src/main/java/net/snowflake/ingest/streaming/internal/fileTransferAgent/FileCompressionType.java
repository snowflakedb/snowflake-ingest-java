/*
 * Replicated from snowflake-jdbc-thin (v3.25.1):
 *   net.snowflake.client.jdbc.internal.snowflake.common.core.FileCompressionType
 * Originally from net.snowflake:snowflake-common (decompiled).
 *
 * Only the fields/methods used by the replicated uploadWithoutConnection are included.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/** File compression type enum. */
public enum FileCompressionType {
  GZIP(".gz", "application/x-gzip", true),
  DEFLATE(".deflate", "application/zlib", true),
  RAW_DEFLATE(".raw_deflate", "application/x-deflate", true),
  BZIP2(".bz2", "application/x-bzip2", true),
  ZSTD(".zst", "application/zstd", true),
  BROTLI(".br", "application/x-brotli", true),
  LZIP(".lz", "application/x-lzip", false),
  LZMA(".lzma", "application/x-lzma", false),
  LZO(".lzo", "application/x-lzop", false),
  XZ(".xz", "application/x-xz", false),
  COMPRESS(".Z", "application/x-compress", false),
  PARQUET(".parquet", "application/x-parquet", true),
  ORC(".orc", "application/x-orc", true);

  private final String fileExtension;
  private final String mimeType;
  private final boolean supported;

  FileCompressionType(String fileExtension, String mimeType, boolean supported) {
    this.fileExtension = fileExtension;
    this.mimeType = mimeType;
    this.supported = supported;
  }

  public boolean isSupported() {
    return supported;
  }

  public String getFileExtension() {
    return fileExtension;
  }

  public String getMimeType() {
    return mimeType;
  }
}

/*
 * Replicated from snowflake-jdbc-thin (v3.25.1):
 *   net.snowflake.client.jdbc.internal.snowflake.common.core.FileCompressionType
 * Originally from net.snowflake:snowflake-common (decompiled from JDBC thin jar).
 *
 * Permitted differences: package.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** File compression type enum. */
public enum FileCompressionType {
  GZIP(".gz", "application", Arrays.asList("gzip", "x-gzip"), true),
  DEFLATE(".deflate", "application", Arrays.asList("zlib", "deflate"), true),
  RAW_DEFLATE(".raw_deflate", "application", Arrays.asList("raw_deflate"), true),
  BZIP2(".bz2", "application", Arrays.asList("bzip2", "x-bzip2", "x-bz2", "x-bzip", "bz2"), true),
  ZSTD(".zst", "application", Arrays.asList("zstd"), true),
  BROTLI(".br", "application", Arrays.asList("brotli", "x-brotli"), true),
  LZIP(".lz", "application", Arrays.asList("lzip", "x-lzip"), false),
  LZMA(".lzma", "application", Arrays.asList("lzma", "x-lzma"), false),
  LZO(".lzo", "application", Arrays.asList("lzo", "x-lzop"), false),
  XZ(".xz", "application", Arrays.asList("xz", "x-xz"), false),
  COMPRESS(".Z", "application", Arrays.asList("compress", "x-compress"), false),
  PARQUET(".parquet", "application", Arrays.asList("parquet"), true),
  ORC(".orc", "application", Arrays.asList("orc"), true);

  private final String fileExtension;
  private final String mimeType;
  private final List<String> mimeSubTypes;
  private final boolean supported;

  static final Map<String, FileCompressionType> mimeSubTypeToCompressionMap;

  static {
    mimeSubTypeToCompressionMap = new HashMap<>();
    for (FileCompressionType type : values()) {
      for (String mimeSubType : type.mimeSubTypes) {
        mimeSubTypeToCompressionMap.put(mimeSubType, type);
      }
    }
  }

  FileCompressionType(
      String fileExtension, String mimeType, List<String> mimeSubTypes, boolean supported) {
    this.fileExtension = fileExtension;
    this.mimeType = mimeType;
    this.mimeSubTypes = mimeSubTypes;
    this.supported = supported;
  }

  public static Optional<FileCompressionType> lookupByMimeSubType(String mimeSubType) {
    return Optional.ofNullable(mimeSubTypeToCompressionMap.get(mimeSubType));
  }

  public static Optional<FileCompressionType> lookupByFileExtension(String fileExtension) {
    for (FileCompressionType type : values()) {
      if (type.fileExtension.equalsIgnoreCase(fileExtension)) {
        return Optional.of(type);
      }
    }
    return Optional.empty();
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

  public List<String> getMimeSubTypes() {
    return mimeSubTypes;
  }
}

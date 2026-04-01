/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/CommonObjectMetadata.java
 *
 * Permitted differences: package, SnowflakeUtil.createCaseInsensitiveMap replaced
 * with StorageClientUtil.createCaseInsensitiveMap.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import java.util.TreeMap;

/**
 * Implements platform-independent interface Azure BLOB and GCS object metadata
 *
 * <p>Only the metadata accessors and mutators used by the JDBC client currently are supported,
 * additional methods should be added as needed
 */
public class CommonObjectMetadata implements StorageObjectMetadata {
  private long contentLength;
  private final Map<String, String> userDefinedMetadata;
  private String contentEncoding;

  CommonObjectMetadata() {
    userDefinedMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  }

  /*
   * Constructs a common metadata object
   * from the set of parameters that the JDBC client is using
   */
  CommonObjectMetadata(
      long contentLength, String contentEncoding, Map<String, String> userDefinedMetadata) {
    this.contentEncoding = contentEncoding;
    this.contentLength = contentLength;
    this.userDefinedMetadata = StorageClientUtil.createCaseInsensitiveMap(userDefinedMetadata);
  }

  /**
   * @return returns a Map/key-value pairs of metadata properties
   */
  @Override
  public Map<String, String> getUserMetadata() {
    return userDefinedMetadata;
  }

  /**
   * @return returns the size of object in bytes
   */
  @Override
  public long getContentLength() {
    return contentLength;
  }

  /** Sets size of the associated object in bytes */
  @Override
  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  /** Adds the key value pair of custom user-metadata for the associated object. */
  @Override
  public void addUserMetadata(String key, String value) {
    userDefinedMetadata.put(key, value);
  }

  /**
   * Sets the optional Content-Encoding HTTP header specifying what content encodings, have been
   * applied to the object and what decoding mechanisms must be applied, in order to obtain the
   * media-type referenced by the Content-Type field.
   */
  @Override
  public void setContentEncoding(String encoding) {
    contentEncoding = encoding;
  }

  /*
   * @return returns the content encoding type
   */
  @Override
  public String getContentEncoding() {
    return contentEncoding;
  }
}

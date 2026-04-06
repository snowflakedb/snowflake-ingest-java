/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/HttpResponseContextDto.java
 *
 * Permitted differences: package. @SnowflakeJdbcInternalApi removed.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import org.apache.http.client.methods.CloseableHttpResponse;

public class HttpResponseContextDto {

  private CloseableHttpResponse httpResponse;
  private String unpackedCloseableHttpResponse;
  private Exception savedEx;

  // Constructors
  public HttpResponseContextDto() {}

  public HttpResponseContextDto(
      CloseableHttpResponse httpResponse, String unpackedCloseableHttpResponse) {
    this.httpResponse = httpResponse;
    this.unpackedCloseableHttpResponse = unpackedCloseableHttpResponse;
  }

  public CloseableHttpResponse getHttpResponse() {
    return httpResponse;
  }

  public void setHttpResponse(CloseableHttpResponse httpResponse) {
    this.httpResponse = httpResponse;
  }

  public String getUnpackedCloseableHttpResponse() {
    return unpackedCloseableHttpResponse;
  }

  public void setUnpackedCloseableHttpResponse(String unpackedCloseableHttpResponse) {
    this.unpackedCloseableHttpResponse = unpackedCloseableHttpResponse;
  }

  public Exception getSavedEx() {
    return savedEx;
  }

  public void setSavedEx(Exception savedEx) {
    this.savedEx = savedEx;
  }

  @Override
  public String toString() {
    return "CloseableHttpResponseContextDto{"
        + "httpResponse="
        + httpResponse
        + ", unpackedCloseableHttpResponse="
        + unpackedCloseableHttpResponse
        + '}';
  }
}

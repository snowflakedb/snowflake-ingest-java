package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class GeneratePresignedUrlsResponse extends StreamingIngestResponse {
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class PresignedUrlInfo {
    @JsonProperty("file_name")
    public String fileName;

    @JsonProperty("url")
    public String url;

    // default constructor for jackson deserialization
    public PresignedUrlInfo() {}

    public PresignedUrlInfo(String fileName, String url) {
      this.fileName = fileName;
      this.url = url;
    }
  }

  @JsonProperty("status_code")
  private Long statusCode;

  @JsonProperty("message")
  private String message;

  @JsonProperty("presigned_url_infos")
  private List<PresignedUrlInfo> presignedUrlInfos;

  @Override
  Long getStatusCode() {
    return this.statusCode;
  }

  String getMessage() {
    return this.message;
  }

  List<PresignedUrlInfo> getPresignedUrlInfos() {
    return this.presignedUrlInfos;
  }
}

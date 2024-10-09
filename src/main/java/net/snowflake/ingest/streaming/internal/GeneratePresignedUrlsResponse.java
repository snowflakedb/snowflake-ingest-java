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

    /*
    Locally-managed expiry timestamp for this url info. We need this since everytime a new URL is
    used for the same chunk, it requires re-serializing the chunk's metadata as the file name is
    embedded in there (search for PRIMARY_FILE_ID_KEY for context). By tracking per-URL expiry
    (with some buffers to account for delays) we can minimize the chances of using a URL that has
    an expired token.
    */
    public long validUntilTimestamp;

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

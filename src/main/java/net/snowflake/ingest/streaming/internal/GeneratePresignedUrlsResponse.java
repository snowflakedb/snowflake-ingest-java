package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

class GeneratePresignedUrlsResponse extends StreamingIngestResponse {
    private Long statusCode;
    private String message;
    private String icebergSerializationPolicy;
    private FileLocationInfo icebergLocationInfo;
    private String encodedFigsId;
    private List<String> presignedUrls;

    @JsonProperty("status_code")
    void setStatusCode(Long statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    Long getStatusCode() {
        return this.statusCode;
    }

    @JsonProperty("message")
    void setMessage(String message) {
        this.message = message;
    }

    String getMessage() {
        return this.message;
    }

    @JsonProperty("iceberg_serialization_policy")
    void setIcebergSerializationPolicy(String icebergSerializationPolicy) {
        this.icebergSerializationPolicy = icebergSerializationPolicy;
    }

    String getIcebergSerializationPolicy() {
        return this.icebergSerializationPolicy;
    }

    @JsonProperty("iceberg_location")
    void setIcebergLocationInfo(FileLocationInfo icebergLocationInfo) {
        this.icebergLocationInfo = icebergLocationInfo;
    }

    FileLocationInfo getIcebergLocationInfo() {
        return this.icebergLocationInfo;
    }

    @JsonProperty("presigned_urls")
    void setPresignedUrls(List<String> presignedUrls) {
        this.presignedUrls = presignedUrls;
    }

    List<String> getPresignedUrls() {
        return this.presignedUrls;
    }

    @JsonProperty("encoded_figs_id")
    void setEncodedFigsId(String encodedFigsId) {
        this.encodedFigsId = encodedFigsId;
    }

    String getEncodedFigsId() {
        return this.encodedFigsId;
    }
}

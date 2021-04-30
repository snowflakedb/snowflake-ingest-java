/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;

public class SnowflakeInternalStage {
    private static final ObjectMapper mapper = new ObjectMapper();

    private SnowflakeFileTransferMetadata fileTransferMetadata;
    private SnowflakeConnection connection;
    public SnowflakeInternalStage(SnowflakeConnection connection, SnowflakeFileTransferMetadataV1 fileTransferMetadata) {
        this.connection = connection;
        this.fileTransferMetadata = fileTransferMetadata;
    }

    public SnowflakeInternalStage(SnowflakeConnection connection) throws SnowflakeSQLException, IOException {
        this.connection = connection;
        this.fileTransferMetadata = this.refreshSnowflakeMetadata();
    }

    /**
     * Upload file to internal stage with previously cached credentials. Refresh credential every 30
     * minutes
     *
     * @param fullFilePath Full file name to be uploaded
     * @param data Data string to be uploaded
     */
    public void putRemote(String fullFilePath, byte[] data) throws SnowflakeSQLException, IOException {
        // Set filename to be uploaded
        ((SnowflakeFileTransferMetadataV1) fileTransferMetadata).setPresignedUrlFileName(fullFilePath);

        InputStream inStream = new ByteArrayInputStream(data);

        try {
            SnowflakeFileTransferAgent.uploadWithoutConnection(
                    SnowflakeFileTransferConfig.Builder.newInstance()
                            .setSnowflakeFileTransferMetadata(fileTransferMetadata)
                            .setUploadStream(inStream)
                            .setRequireCompress(false) // TODO confirm no compression
                            .setOcspMode(OCSPMode.FAIL_OPEN)
                            .build());
        } catch (NullPointerException npe) {
            // TODO Update JDBC driver to throw a reliable token expired error
            if (npe.getStackTrace()[0].getClassName() == "net.snowflake.client.core.SFStatement" &&
                    npe.getStackTrace()[0].getLineNumber() == 332
            ) {
                this.fileTransferMetadata = this.refreshSnowflakeMetadata();
                this.putRemote(fullFilePath, data);
                return;
            }
            throw npe;
        } catch (Exception e) {
            throw new SnowflakeSQLException(e, ErrorCode.IO_ERROR);
        }
    }

    SnowflakeFileTransferMetadata refreshSnowflakeMetadata() throws SnowflakeSQLException, IOException {
        String sessionToken =  ((SnowflakeConnectionV1) connection).getSfSession().getSessionToken();
        SnowflakeConnectString connectString = ((SnowflakeConnectionV1) connection).getSfSession().getSnowflakeConnectionString();
        String configureUrl = String.format("%s://%s:%s/v1/streaming/client/configure",
                connectString.getScheme(),
                connectString.getHost(),
                connectString.getPort());

        HttpPost postRequest = new HttpPost(configureUrl);
        postRequest.addHeader("accept", "application/json");
        postRequest.setHeader(HttpHeaders.AUTHORIZATION, "Basic");

        postRequest.setHeader(
                "Authorization",
                "Snowflake"
                        + " "
                        + "token"
                        + "=\""
                        + sessionToken
                        + "\"");

        StringEntity input = new StringEntity("{}", StandardCharsets.UTF_8);
        input.setContentType("application/json");
        postRequest.setEntity(input);

        JsonNode responseNode;
        String response = HttpUtil.executeGeneralRequest(postRequest, 100, null);
        responseNode = mapper.readTree(response);

        // Currently have a few mismatches between the client/configure response and what SnowflakeFileTransferAgent expects
        ObjectNode mutable = (ObjectNode) responseNode;
        mutable.putObject("data");
        ObjectNode dataNode = (ObjectNode) mutable.get("data");
        dataNode.set("stageInfo", responseNode.get("stage_location"));
        dataNode.putArray("src_locations").add("foo/");

        return SnowflakeFileTransferAgent.getFileTransferMetadatas(responseNode).get(0);
    }
}

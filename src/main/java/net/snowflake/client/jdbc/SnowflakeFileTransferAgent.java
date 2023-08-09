/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.jdbc.cloud.storage.StorageClientFactory;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.cloud.storage.*;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.FileCompressionType;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.*;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import static net.snowflake.client.core.Constants.NO_SPACE_LEFT_ON_DEVICE_ERR;

/**
 * Class for uploading/downloading files
 *
 * @author jhuang
 */
public class SnowflakeFileTransferAgent  {
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  static final StorageClientFactory storageFactory = StorageClientFactory.getFactory();

  private static final String FILE_PROTOCOL = "file://";
  private static final String localFSFileSep = SnowflakeUtil.systemGetProperty("file.separator");


  // We will allow buffering of upto 128M data before spilling to disk during
  // compression and digest computation
  static final int MAX_BUFFER_SIZE = 1 << 27;
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeFileTransferAgent.class);

  private static Throwable injectedFileTransferException = null; // for testing purpose
  // This function should only be used for testing purpose
  static void setInjectedFileTransferException(Throwable th) {
    injectedFileTransferException = th;
  }

  static boolean isInjectedFileTransferExceptionEnabled() {
    return injectedFileTransferException != null;
  }
  static class InputStreamWithMetadata {
    long size;
    String digest;

    // FileBackedOutputStream that should be destroyed when
    // the input stream has been consumed entirely
    FileBackedOutputStream fileBackedOutputStream;

    InputStreamWithMetadata(
            long size, String digest, FileBackedOutputStream fileBackedOutputStream) {
      this.size = size;
      this.digest = digest;
      this.fileBackedOutputStream = fileBackedOutputStream;
    }
  }

  /**
   * Static API function to upload data without JDBC session.
   *
   * <p>NOTE: This function is developed based on getUploadFileCallable().
   *
   * @param config Configuration to upload a file to cloud storage
   * @throws Exception if error occurs while data upload.
   */
  public static void uploadWithoutConnection(SnowflakeFileTransferConfig config) throws Exception {
    logger.debug("Entering uploadWithoutConnection...");

    SnowflakeFileTransferMetadataV1 metadata =
            (SnowflakeFileTransferMetadataV1) config.getSnowflakeFileTransferMetadata();
    InputStream uploadStream = config.getUploadStream();
    boolean requireCompress = config.getRequireCompress();
    int networkTimeoutInMilli = config.getNetworkTimeoutInMilli();
    OCSPMode ocspMode = config.getOcspMode();
    Properties proxyProperties = config.getProxyProperties();
    String streamingIngestClientName = config.getStreamingIngestClientName();
    String streamingIngestClientKey = config.getStreamingIngestClientKey();

    // Create HttpClient key
    HttpClientSettingsKey key =
            SnowflakeUtil.convertProxyPropertiesToHttpClientKey(ocspMode, proxyProperties);

    StageInfo stageInfo = metadata.getStageInfo();
    stageInfo.setProxyProperties(proxyProperties);
    String destFileName = metadata.getPresignedUrlFileName();

    logger.debug("Begin upload data for " + destFileName);

    long uploadSize;
    File fileToUpload = null;
    String digest = null;

    // Temp file that needs to be cleaned up when upload was successful
    FileBackedOutputStream fileBackedOutputStream = null;

    RemoteStoreFileEncryptionMaterial encMat = metadata.getEncryptionMaterial();
    if (encMat.getQueryId() == null
            && encMat.getQueryStageMasterKey() == null
            && encMat.getSmkId() == null) {
      encMat = null;
    }
    // SNOW-16082: we should capture exception if we fail to compress or
    // calculate digest.
    try {
      if (requireCompress) {
        InputStreamWithMetadata compressedSizeAndStream =
                (encMat == null
                        ? compressStreamWithGZIPNoDigest(uploadStream)
                        : compressStreamWithGZIP(uploadStream));

        fileBackedOutputStream = compressedSizeAndStream.fileBackedOutputStream;

        // update the size
        uploadSize = compressedSizeAndStream.size;
        digest = compressedSizeAndStream.digest;

        if (compressedSizeAndStream.fileBackedOutputStream.getFile() != null) {
          fileToUpload = compressedSizeAndStream.fileBackedOutputStream.getFile();
        }

        logger.debug("New size after compression: {}", uploadSize);
      } else {
        // If it's not local_fs, we store our digest in the metadata
        // In local_fs, we don't need digest, and if we turn it on,
        // we will consume whole uploadStream, which local_fs uses.
        InputStreamWithMetadata result = computeDigest(uploadStream, true);
        digest = result.digest;
        fileBackedOutputStream = result.fileBackedOutputStream;
        uploadSize = result.size;

        if (result.fileBackedOutputStream.getFile() != null) {
          fileToUpload = result.fileBackedOutputStream.getFile();
        }
      }

      logger.debug(
              "Started copying file to {}:{} destName: {} compressed ? {} size={}",
              stageInfo.getStageType().name(),
              stageInfo.getLocation(),
              destFileName,
              (requireCompress ? "yes" : "no"),
              uploadSize);

      SnowflakeStorageClient initialClient =
              storageFactory.createClient(stageInfo, 1, encMat);

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()) {
        throw (Exception) SnowflakeFileTransferAgent.injectedFileTransferException;
      }

      switch (stageInfo.getStageType()) {
        case S3:
        case AZURE:
          pushFileToRemoteStore(
                  metadata.getStageInfo(),
                  destFileName,
                  uploadStream,
                  fileBackedOutputStream,
                  uploadSize,
                  digest,
                  (requireCompress ? FileCompressionType.GZIP : null),
                  initialClient,
                  null,
                  1,
                  fileToUpload,
                  (fileToUpload == null),
                  encMat,
                  streamingIngestClientName,
                  streamingIngestClientKey);
          break;
        case GCS:
          // If the down-scoped token is used to upload file, one metadata may be used to upload
          // multiple files, so use the dest file name in config.
          destFileName =
                  metadata.isForOneFile()
                          ? metadata.getPresignedUrlFileName()
                          : config.getDestFileName();

          pushFileToRemoteStoreWithPresignedUrl(
                  metadata.getStageInfo(),
                  destFileName,
                  uploadStream,
                  fileBackedOutputStream,
                  uploadSize,
                  digest,
                  (requireCompress ? FileCompressionType.GZIP : null),
                  initialClient,
                  networkTimeoutInMilli,
                  key,
                  1,
                  null,
                  true,
                  encMat,
                  metadata.getPresignedUrl(),
                  streamingIngestClientName,
                  streamingIngestClientKey);
          break;
      }
    } catch (Exception ex) {
      logger.error("Exception encountered during file upload: ", ex.getMessage());
      throw ex;
    } finally {
      if (fileBackedOutputStream != null) {
        try {
          fileBackedOutputStream.reset();
        } catch (IOException ex) {
          logger.debug("failed to clean up temp file: {}", ex);
        }
      }
    }
  }

  private static void pushFileToRemoteStore(
          StageInfo stage,
          String destFileName,
          InputStream inputStream,
          FileBackedOutputStream fileBackedOutStr,
          long uploadSize,
          String digest,
          FileCompressionType compressionType,
          SnowflakeStorageClient initialClient,
          String command,
          int parallel,
          File srcFile,
          boolean uploadFromStream,
          RemoteStoreFileEncryptionMaterial encMat,
          String streamingIngestClientName,
          String streamingIngestClientKey)
          throws SQLException, IOException {
    RemoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    String origDestFileName = destFileName;
    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
              remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.debug(
            "upload object. location={}, key={}, srcFile={}, encryption={}",
            remoteLocation.location,
            destFileName,
            srcFile,
            (ArgSupplier)
                    () -> (encMat == null ? "NULL" : encMat.getSmkId() + "|" + encMat.getQueryId()));

    StorageObjectMetadata meta = storageFactory.createStorageMetadataObj(stage.getStageType());
    meta.setContentLength(uploadSize);
    if (digest != null) {
      initialClient.addDigestMetadata(meta, digest);
    }

    if (compressionType != null && compressionType.isSupported()) {
      meta.setContentEncoding(compressionType.name().toLowerCase());
    }

    if (streamingIngestClientName != null && streamingIngestClientKey != null) {
      initialClient.addStreamingIngestMetadata(
              meta, streamingIngestClientName, streamingIngestClientKey);
    }

    try {
      String presignedUrl = "";
//      if (initialClient.requirePresignedUrl()) {
//        // need to replace file://mypath/myfile?.csv with file://mypath/myfile1.csv.gz
//        String localFilePath = getLocalFilePathFromCommand(command, false);
//        String commandWithExactPath = command.replace(localFilePath, origDestFileName);
//        // then hand that to GS to get the actual presigned URL we'll use
//        SFStatement statement = new SFStatement(null);
//        JsonNode jsonNode = parseCommandInGS(statement, commandWithExactPath);
//
//        if (!jsonNode.path("data").path("stageInfo").path("presignedUrl").isMissingNode()) {
//          presignedUrl = jsonNode.path("data").path("stageInfo").path("presignedUrl").asText();
//        }
//      }
      initialClient.upload(
              null,
              command,
              parallel,
              uploadFromStream,
              remoteLocation.location,
              srcFile,
              destFileName,
              inputStream,
              fileBackedOutStr,
              meta,
              stage.getRegion(),
              presignedUrl);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }

  /**
   * Push a file (or stream) to remote store with pre-signed URL without JDBC session.
   *
   * <p>NOTE: This function is developed based on pushFileToRemoteStore(). The main difference is
   * that the caller needs to provide pre-signed URL and the upload doesn't need JDBC session.
   */
  private static void pushFileToRemoteStoreWithPresignedUrl(
          StageInfo stage,
          String destFileName,
          InputStream inputStream,
          FileBackedOutputStream fileBackedOutStr,
          long uploadSize,
          String digest,
          FileCompressionType compressionType,
          SnowflakeStorageClient initialClient,
          int networkTimeoutInMilli,
          HttpClientSettingsKey ocspModeAndProxyKey,
          int parallel,
          File srcFile,
          boolean uploadFromStream,
          RemoteStoreFileEncryptionMaterial encMat,
          String presignedUrl,
          String streamingIngestClientName,
          String streamingIngestClientKey)
          throws SQLException, IOException {
    RemoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
              remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.debug(
            "upload object. location={}, key={}, srcFile={}, encryption={}",
            remoteLocation.location,
            destFileName,
            srcFile,
            (ArgSupplier)
                    () -> (encMat == null ? "NULL" : encMat.getSmkId() + "|" + encMat.getQueryId()));

    StorageObjectMetadata meta = storageFactory.createStorageMetadataObj(stage.getStageType());
    meta.setContentLength(uploadSize);
    if (digest != null) {
      initialClient.addDigestMetadata(meta, digest);
    }

    if (compressionType != null && compressionType.isSupported()) {
      meta.setContentEncoding(compressionType.name().toLowerCase());
    }

    if (streamingIngestClientName != null && streamingIngestClientKey != null) {
      initialClient.addStreamingIngestMetadata(
              meta, streamingIngestClientName, streamingIngestClientKey);
    }

    try {
      initialClient.uploadWithPresignedUrlWithoutConnection(
              networkTimeoutInMilli,
              ocspModeAndProxyKey,
              parallel,
              uploadFromStream,
              remoteLocation.location,
              srcFile,
              destFileName,
              inputStream,
              fileBackedOutStr,
              meta,
              stage.getRegion(),
              presignedUrl);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }



  /**
   * Compress an input stream with GZIP and return the result size, digest and compressed stream.
   *
   * @param inputStream The input stream to compress
   * @return the compressed stream
   * @throws SnowflakeSQLException Will be thrown if there is a problem with compression
   * @deprecated Can be removed when all accounts are encrypted
   */
  @Deprecated
  private static InputStreamWithMetadata compressStreamWithGZIPNoDigest(
          InputStream inputStream) throws SnowflakeSQLException {
    try {
      FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

      CountingOutputStream countingStream = new CountingOutputStream(tempStream);

      // construct a gzip stream with sync_flush mode
      GZIPOutputStream gzipStream;

      gzipStream = new GZIPOutputStream(countingStream, true);

      IOUtils.copy(inputStream, gzipStream);

      inputStream.close();

      gzipStream.finish();
      gzipStream.flush();

      countingStream.flush();

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()) {
        throw (IOException) SnowflakeFileTransferAgent.injectedFileTransferException;
      }
      return new SnowflakeFileTransferAgent.InputStreamWithMetadata(countingStream.getCount(), null, tempStream);

    } catch (IOException ex) {
      logger.error("Exception compressing input stream", ex);

      throw new SnowflakeSQLLoggedException(
              null,
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              ex,
              "error encountered for compression");
    }
  }

  /**
   * Compress an input stream with GZIP and return the result size, digest and compressed stream.
   *
   * @param inputStream data input
   * @return result size, digest and compressed stream
   * @throws SnowflakeSQLException if encountered exception when compressing
   */
  private static InputStreamWithMetadata compressStreamWithGZIP(
          InputStream inputStream) throws SnowflakeSQLException {
    FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

    try {

      DigestOutputStream digestStream =
              new DigestOutputStream(tempStream, MessageDigest.getInstance("SHA-256"));

      CountingOutputStream countingStream = new CountingOutputStream(digestStream);

      // construct a gzip stream with sync_flush mode
      GZIPOutputStream gzipStream;

      gzipStream = new GZIPOutputStream(countingStream, true);

      IOUtils.copy(inputStream, gzipStream);

      inputStream.close();

      gzipStream.finish();
      gzipStream.flush();

      countingStream.flush();

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()
              && SnowflakeFileTransferAgent.injectedFileTransferException
              instanceof NoSuchAlgorithmException) {
        throw (NoSuchAlgorithmException) SnowflakeFileTransferAgent.injectedFileTransferException;
      }

      return new SnowflakeFileTransferAgent.InputStreamWithMetadata(
              countingStream.getCount(),
              Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
              tempStream);

    } catch (IOException | NoSuchAlgorithmException ex) {
      logger.error("Exception compressing input stream", ex);

      throw new SnowflakeSQLLoggedException(
              null,
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              ex,
              "error encountered for compression");
    }
  }

  /**
   * This is API function to parse the File Transfer Metadatas from a supplied PUT call response.
   *
   * <p>NOTE: It only supports PUT on S3/AZURE/GCS (i.e. NOT LOCAL_FS)
   *
   * @param jsonNode JSON doc returned by GS from PUT call
   * @return The file transfer metadatas for to-be-transferred files.
   * @throws SnowflakeSQLException if any error occurs
   */
  public static List<SnowflakeFileTransferMetadata> getFileTransferMetadatas(JsonNode jsonNode)
          throws SnowflakeSQLException {
    CommandType commandType =
            !jsonNode.path("data").path("command").isMissingNode()
                    ? CommandType.valueOf(jsonNode.path("data").path("command").asText())
                    : CommandType.UPLOAD;
    if (commandType != CommandType.UPLOAD) {
      throw new SnowflakeSQLException(
              ErrorCode.INTERNAL_ERROR, "This API only supports PUT commands");
    }

    JsonNode locationsNode = jsonNode.path("data").path("src_locations");

    if (!locationsNode.isArray()) {
      throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "src_locations must be an array");
    }

    final String[] srcLocations;
    final List<RemoteStoreFileEncryptionMaterial> encryptionMaterial;
    try {
      srcLocations = mapper.readValue(locationsNode.toString(), String[].class);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
              ErrorCode.INTERNAL_ERROR, "Failed to parse the locations due to: " + ex.getMessage());
    }

    try {
      encryptionMaterial = getEncryptionMaterial(commandType, jsonNode);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
              ErrorCode.INTERNAL_ERROR,
              "Failed to parse encryptionMaterial due to: " + ex.getMessage());
    }

    // For UPLOAD we expect encryptionMaterial to have length 1
    assert encryptionMaterial.size() == 1;

    final Set<String> sourceFiles = expandFileNames(srcLocations);

    StageInfo stageInfo = getStageInfo(jsonNode);

    List<SnowflakeFileTransferMetadata> result = new ArrayList<>();
    if (stageInfo.getStageType() != StageInfo.StageType.GCS
            && stageInfo.getStageType() != StageInfo.StageType.AZURE
            && stageInfo.getStageType() != StageInfo.StageType.S3) {
      throw new SnowflakeSQLException(
              ErrorCode.INTERNAL_ERROR,
              "This API only supports S3/AZURE/GCS, received=" + stageInfo.getStageType());
    }

    for (String sourceFilePath : sourceFiles) {
      String sourceFileName = sourceFilePath.substring(sourceFilePath.lastIndexOf("/") + 1);
      result.add(
              new SnowflakeFileTransferMetadataV1(
                      stageInfo.getPresignedUrl(),
                      sourceFileName,
                      encryptionMaterial.get(0) != null
                              ? encryptionMaterial.get(0).getQueryStageMasterKey()
                              : null,
                      encryptionMaterial.get(0) != null ? encryptionMaterial.get(0).getQueryId() : null,
                      encryptionMaterial.get(0) != null ? encryptionMaterial.get(0).getSmkId() : null,
                      commandType,
                      stageInfo));
    }

    return result;
  }

  static  StageInfo getStageInfo(JsonNode jsonNode) throws  SnowflakeSQLException {

    // more parameters common to upload/download
    String stageLocation = jsonNode.path("data").path("stageInfo").path("location").asText();

    String stageLocationType =
            jsonNode.path("data").path("stageInfo").path("locationType").asText();

    String stageRegion = null;
    if (!jsonNode.path("data").path("stageInfo").path("region").isMissingNode()) {
      stageRegion = jsonNode.path("data").path("stageInfo").path("region").asText();
    }

    boolean isClientSideEncrypted = true;
    if (!jsonNode.path("data").path("stageInfo").path("isClientSideEncrypted").isMissingNode()) {
      isClientSideEncrypted =
              jsonNode.path("data").path("stageInfo").path("isClientSideEncrypted").asBoolean(true);
    }

    // endPoint is currently known to be set for Azure stages or S3. For S3 it will be set
    // specifically
    // for FIPS or VPCE S3 endpoint. SNOW-652696
    String endPoint = null;
    if ("AZURE".equalsIgnoreCase(stageLocationType) || "S3".equalsIgnoreCase(stageLocationType)) {
      endPoint = jsonNode.path("data").path("stageInfo").findValue("endPoint").asText();
    }

    String stgAcct = null;
    // storageAccount are only available in Azure stages. Value
    // will be present but null in other platforms.
    if ("AZURE".equalsIgnoreCase(stageLocationType)) {
      // Jackson is doing some very strange things trying to pull the value of
      // the storageAccount node after adding the GCP library dependencies.
      // If we try to pull the value by name, we get back null, but clearly the
      // node is there. This code works around the issue by enumerating through
      // all the nodes and getting the one that starts with "sto". The value
      // then comes back with double quotes around it, so we're stripping them
      // off. As long as our JSON doc doesn't add another node that starts with
      // "sto", this should work fine.
      Iterator<Entry<String, JsonNode>> fields = jsonNode.path("data").path("stageInfo").fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> jsonField = fields.next();
        if (jsonField.getKey().startsWith("sto")) {
          stgAcct =
                  jsonField
                          .getValue()
                          .toString()
                          .trim()
                          .substring(1, jsonField.getValue().toString().trim().lastIndexOf("\""));
        }
      }
    }

    if ("LOCAL_FS".equalsIgnoreCase(stageLocationType)) {
      if (stageLocation.startsWith("~")) {
        // replace ~ with user home
        stageLocation =  SnowflakeUtil.systemGetProperty("user.home") + stageLocation.substring(1);
      }

      if (!(new File(stageLocation)).isAbsolute()) {
        String cwd =  SnowflakeUtil.systemGetProperty("user.dir");

        logger.debug("Adding current working dir to stage file path.");

        stageLocation = cwd + localFSFileSep + stageLocation;
      }
    }

    Map<?, ?> stageCredentials = extractStageCreds(jsonNode);

     StageInfo stageInfo =
             StageInfo.createStageInfo(
                    stageLocationType,
                    stageLocation,
                    stageCredentials,
                    stageRegion,
                    endPoint,
                    stgAcct,
                    isClientSideEncrypted);

    // Setup pre-signed URL into stage info if pre-signed URL is returned.
    if (stageInfo.getStageType() ==  StageInfo.StageType.GCS) {
      JsonNode presignedUrlNode = jsonNode.path("data").path("stageInfo").path("presignedUrl");
      if (!presignedUrlNode.isMissingNode()) {
        String presignedUrl = presignedUrlNode.asText();
        if (!Strings.isNullOrEmpty(presignedUrl)) {
          stageInfo.setPresignedUrl(presignedUrl);
        }
      }
    }

    return stageInfo;
  }

  /**
   * @param rootNode JSON doc returned by GS
   * @throws SnowflakeSQLException Will be thrown if we fail to parse the stage credentials
   */
  private static Map<?, ?> extractStageCreds(JsonNode rootNode) throws SnowflakeSQLException {
    JsonNode credsNode = rootNode.path("data").path("stageInfo").path("creds");
    Map<?, ?> stageCredentials = null;

    try {
      TypeReference<HashMap<String, String>> typeRef =
              new TypeReference<HashMap<String, String>>() {};
      stageCredentials = mapper.readValue(credsNode.toString(), typeRef);

    } catch (Exception ex) {
      throw new SnowflakeSQLException(
              ex,
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "Failed to parse the credentials ("
                      + (credsNode != null ? credsNode.toString() : "null")
                      + ") due to exception: "
                      + ex.getMessage());
    }

    return stageCredentials;
  }
  private static InputStreamWithMetadata computeDigest(InputStream is, boolean resetStream)
          throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    if (resetStream) {
      FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

      CountingOutputStream countingOutputStream = new CountingOutputStream(tempStream);

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);

      IOUtils.copy(is, digestStream);

      return new SnowflakeFileTransferAgent.InputStreamWithMetadata(
              countingOutputStream.getCount(),
              Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
              tempStream);
    } else {
      CountingOutputStream countingOutputStream =
              new CountingOutputStream(ByteStreams.nullOutputStream());

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);
      IOUtils.copy(is, digestStream);
      return new SnowflakeFileTransferAgent.InputStreamWithMetadata(
              countingOutputStream.getCount(),
              Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
              null);
    }
  }

  /**
   * A small helper for extracting location name and path from full location path
   *
   * @param stageLocationPath stage location
   * @return remoteLocation object
   */
  public static RemoteLocation extractLocationAndPath(String stageLocationPath) {
    String location = stageLocationPath;
    String path = "";

    // split stage location as location name and path
    if (stageLocationPath.contains("/")) {
      location = stageLocationPath.substring(0, stageLocationPath.indexOf("/"));
      path = stageLocationPath.substring(stageLocationPath.indexOf("/") + 1);
    }

    return new RemoteLocation(location, path);
  }

  /**
   * Get the encryption information for an UPLOAD or DOWNLOAD given a PUT command response JsonNode
   *
   * @param commandType CommandType of action (e.g UPLOAD or DOWNLOAD)
   * @param jsonNode JsonNod of PUT call response
   * @return List of RemoteStoreFileEncryptionMaterial objects
   */
  static List<RemoteStoreFileEncryptionMaterial> getEncryptionMaterial(
          CommandType commandType, JsonNode jsonNode)
          throws SnowflakeSQLException, JsonProcessingException {
    List<RemoteStoreFileEncryptionMaterial> encryptionMaterial = new ArrayList<>();
    JsonNode rootNode = jsonNode.path("data").path("encryptionMaterial");
    if (commandType == CommandType.UPLOAD) {
      logger.debug("initEncryptionMaterial: UPLOAD", false);

      RemoteStoreFileEncryptionMaterial encMat = null;
      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        encMat = mapper.treeToValue(rootNode, RemoteStoreFileEncryptionMaterial.class);
      }
      encryptionMaterial.add(encMat);

    } else {
      logger.debug("initEncryptionMaterial: DOWNLOAD", false);

      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        encryptionMaterial =
                Arrays.asList(mapper.treeToValue(rootNode, RemoteStoreFileEncryptionMaterial[].class));
      }
    }
    return encryptionMaterial;
  }

  /**
   * process a list of file paths separated by "," and expand the wildcards if any to generate the
   * list of paths for all files matched by the wildcards
   *
   * @param filePathList file path list
   * @return a set of file names that is matched
   * @throws SnowflakeSQLException if cannot find the file
   */
  static Set<String> expandFileNames(String[] filePathList) throws SnowflakeSQLException {
    Set<String> result = new HashSet<String>();

    // a location to file pattern map so that we only need to list the
    // same directory once when they appear in multiple times.
    Map<String, List<String>> locationToFilePatterns;

    locationToFilePatterns = new HashMap<String, List<String>>();

    String cwd = SnowflakeUtil.systemGetProperty("user.dir");

    for (String path : filePathList) {
      // replace ~ with user home
      if (path.startsWith("~")) {
        path = SnowflakeUtil.systemGetProperty("user.home") + path.substring(1);
      }

      // user may also specify files relative to current directory
      // add the current path if that is the case
      if (!(new File(path)).isAbsolute()) {
        logger.debug("Adding current working dir to relative file path.");

        path = cwd + localFSFileSep + path;
      }

      // check if the path contains any wildcards
      if (!path.contains("*")
              && !path.contains("?")
              && !(path.contains("[") && path.contains("]"))) {
        /* this file path doesn't have any wildcard, so we don't need to
         * expand it
         */
        result.add(path);
      } else {
        // get the directory path
        int lastFileSepIndex = path.lastIndexOf(localFSFileSep);

        // SNOW-15203: if we don't find a default file sep, try "/" if it is not
        // the default file sep.
        if (lastFileSepIndex < 0 && !"/".equals(localFSFileSep)) {
          lastFileSepIndex = path.lastIndexOf("/");
        }

        String loc = path.substring(0, lastFileSepIndex + 1);

        String filePattern = path.substring(lastFileSepIndex + 1);

        List<String> filePatterns = locationToFilePatterns.get(loc);

        if (filePatterns == null) {
          filePatterns = new ArrayList<String>();
          locationToFilePatterns.put(loc, filePatterns);
        }

        filePatterns.add(filePattern);
      }
    }

    // For each location, list files and match against the patterns
    for (Entry<String, List<String>> entry : locationToFilePatterns.entrySet()) {
      try {
        File dir = new File(entry.getKey());

        logger.debug(
                "Listing files under: {} with patterns: {}",
                entry.getKey(),
                entry.getValue().toString());

        // Normal flow will never hit here. This is only for testing purposes
        if (isInjectedFileTransferExceptionEnabled()
                && injectedFileTransferException instanceof Exception) {
          throw (Exception) injectedFileTransferException;
        }

        // The following currently ignore sub directories
        for (Object file :
                FileUtils.listFiles(dir, new WildcardFileFilter(entry.getValue()), null)) {
          result.add(((File) file).getCanonicalPath());
        }
      } catch (Exception ex) {
        throw new SnowflakeSQLException(
                ex,
                SqlState.DATA_EXCEPTION,
                ErrorCode.FAIL_LIST_FILES.getMessageCode(),
                "Exception: "
                        + ex.getMessage()
                        + ", Dir="
                        + entry.getKey()
                        + ", Patterns="
                        + entry.getValue().toString());
      }
    }

    logger.debug("Expanded file paths: ");

    for (String filePath : result) {
      logger.debug("file: {}", filePath);
    }

    return result;
  }

  /** The types of file transfer: upload and download. */
  public enum CommandType {
    UPLOAD,
    DOWNLOAD
  }

  /** Remote object location: "bucket" for S3, "container" for Azure BLOB */
  private static class RemoteLocation {
    String location;
    String path;

    public RemoteLocation(String remoteStorageLocation, String remotePath) {
      location = remoteStorageLocation;
      path = remotePath;
    }
  }

  /*
   * Handles an InvalidKeyException which indicates that the JCE component
   * is not installed properly
   * @param operation a string indicating the operation type, e.g. upload/download
   * @param ex The exception to be handled
   * @throws throws the error as a SnowflakeSQLException
   */
  public static void throwJCEMissingError(String operation, Exception ex)
          throws SnowflakeSQLException {
    // Most likely cause: Unlimited strength policy files not installed
    String msg =
            "Strong encryption with Java JRE requires JCE "
                    + "Unlimited Strength Jurisdiction Policy files. "
                    + "Follow JDBC client installation instructions "
                    + "provided by Snowflake or contact Snowflake Support.";

    logger.error(
            "JCE Unlimited Strength policy files missing: {}. {}.",
            ex.getMessage(),
            ex.getCause().getMessage());

    String bootLib =  SnowflakeUtil.systemGetProperty("sun.boot.library.path");
    if (bootLib != null) {
      msg +=
              " The target directory on your system is: " + Paths.get(bootLib, "security").toString();
      logger.error(msg);
    }
    throw new SnowflakeSQLException(
            ex, SqlState.SYSTEM_ERROR, ErrorCode.AWS_CLIENT_ERROR.getMessageCode(), operation, msg);
  }
  
   /**
   * For handling IOException: No space left on device when attempting to download a file to a
   * location where there is not enough space. We don't want to retry on this exception.
   *
   * @param session the current session
   * @param operation the operation i.e. GET
   * @param ex the exception caught
   * @throws SnowflakeSQLLoggedException
   */
  public static void throwNoSpaceLeftError(SFSession session, String operation, Exception ex)
      throws SnowflakeSQLLoggedException {
    String exMessage = SnowflakeUtil.getRootCause(ex).getMessage();
    if (exMessage != null && exMessage.equals(NO_SPACE_LEFT_ON_DEVICE_ERR)) {
      throw new SnowflakeSQLLoggedException(
          session,
          SqlState.SYSTEM_ERROR,
          ErrorCode.IO_ERROR.getMessageCode(),
          ex,
          "Encountered exception during " + operation + ": " + ex.getMessage());
    }
  }
}

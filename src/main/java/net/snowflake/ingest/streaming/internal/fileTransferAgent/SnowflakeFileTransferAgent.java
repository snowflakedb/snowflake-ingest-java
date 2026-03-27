/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
 *
 * Contains only the static parsing methods used by InternalStage:
 *   getFileTransferMetadatas, getStageInfo, getEncryptionMaterial,
 *   extractStageCreds, expandFileNames, setupUseRegionalUrl, setupUseVirtualUrl.
 *
 * Permitted differences: package, SFLogger uses ingest's replicated version,
 * SnowflakeUtil.isNullOrEmpty/systemGetProperty replaced with StorageClientUtil versions,
 * ErrorCode/SqlState/SnowflakeSQLException use ingest versions.
 * CommandType inherited from SFBaseFileTransferAgent parent class.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.isNullOrEmpty;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.systemGetProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.core.SFSession;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/** Class for uploading/downloading files */
public class SnowflakeFileTransferAgent extends SFBaseFileTransferAgent {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeFileTransferAgent.class);

  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  // We will allow buffering of upto 128M data before spilling to disk during
  // compression and digest computation
  static final int MAX_BUFFER_SIZE = 1 << 27;

  private static final String localFSFileSep = systemGetProperty("file.separator");

  // For testing purpose
  private static Throwable injectedFileTransferException = null;

  public static void setInjectedFileTransferException(Throwable th) {
    injectedFileTransferException = th;
  }

  static boolean isInjectedFileTransferExceptionEnabled() {
    return injectedFileTransferException != null;
  }

  static List<RemoteStoreFileEncryptionMaterial> getEncryptionMaterial(
      CommandType commandType, JsonNode jsonNode)
      throws SnowflakeSQLException, JsonProcessingException {
    List<RemoteStoreFileEncryptionMaterial> encryptionMaterial = new ArrayList<>();
    JsonNode rootNode = jsonNode.path("data").path("encryptionMaterial");
    if (commandType == CommandType.UPLOAD) {
      logger.debug("InitEncryptionMaterial: UPLOAD", false);

      RemoteStoreFileEncryptionMaterial encMat = null;
      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        encMat = mapper.treeToValue(rootNode, RemoteStoreFileEncryptionMaterial.class);
      }
      encryptionMaterial.add(encMat);

    } else {
      logger.debug("InitEncryptionMaterial: DOWNLOAD", false);

      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        encryptionMaterial =
            Arrays.asList(mapper.treeToValue(rootNode, RemoteStoreFileEncryptionMaterial[].class));
      }
    }
    return encryptionMaterial;
  }

  static StageInfo getStageInfo(JsonNode jsonNode, SFSession session) throws SnowflakeSQLException {
    String queryId = jsonNode.path("data").path("queryId").asText();

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
    if ("AZURE".equalsIgnoreCase(stageLocationType)
        || "S3".equalsIgnoreCase(stageLocationType)
        || "GCS".equalsIgnoreCase(stageLocationType)) {
      endPoint = jsonNode.path("data").path("stageInfo").findValue("endPoint").asText();
      if ("GCS".equalsIgnoreCase(stageLocationType)
          && endPoint != null
          && (endPoint.trim().isEmpty() || "null".equals(endPoint))) {
        // setting to null to preserve previous behaviour for GCS
        endPoint = null;
      }
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
        stageLocation = systemGetProperty("user.home") + stageLocation.substring(1);
      }

      if (!(new File(stageLocation)).isAbsolute()) {
        String cwd = systemGetProperty("user.dir");

        logger.debug("Adding current working dir to stage file path.");

        stageLocation = cwd + localFSFileSep + stageLocation;
      }
    }

    Map<?, ?> stageCredentials = extractStageCreds(jsonNode, queryId);

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
    if (stageInfo.getStageType() == StageInfo.StageType.GCS) {
      JsonNode presignedUrlNode = jsonNode.path("data").path("stageInfo").path("presignedUrl");
      if (!presignedUrlNode.isMissingNode()) {
        String presignedUrl = presignedUrlNode.asText();
        if (!isNullOrEmpty(presignedUrl)) {
          stageInfo.setPresignedUrl(presignedUrl);
        }
      }
    }

    setupUseRegionalUrl(jsonNode, stageInfo);
    setupUseVirtualUrl(jsonNode, stageInfo);

    if (stageInfo.getStageType() == StageInfo.StageType.S3) {
      if (session == null) {
        // This node's value is set if PUT is used without Session. (For Snowpipe Streaming, we rely
        // on a response from a server to have this field set to use S3RegionalURL)
        JsonNode useS3RegionalURLNode =
            jsonNode.path("data").path("stageInfo").path("useS3RegionalUrl");
        if (!useS3RegionalURLNode.isMissingNode()) {
          boolean useS3RegionalUrl = useS3RegionalURLNode.asBoolean(false);
          stageInfo.setUseS3RegionalUrl(useS3RegionalUrl);
        }
      } else {
        // Update StageInfo to reflect use of S3 regional URL.
        // This is required for connecting to S3 over privatelink when the
        // target stage is in us-east-1
        stageInfo.setUseS3RegionalUrl(session.getUseRegionalS3EndpointsForPresignedURL());
      }
    }

    return stageInfo;
  }

  private static void setupUseRegionalUrl(JsonNode jsonNode, StageInfo stageInfo) {
    if (stageInfo.getStageType() != StageInfo.StageType.GCS
        && stageInfo.getStageType() != StageInfo.StageType.S3) {
      return;
    }
    JsonNode useRegionalURLNode = jsonNode.path("data").path("stageInfo").path("useRegionalUrl");
    if (!useRegionalURLNode.isMissingNode()) {
      boolean useRegionalURL = useRegionalURLNode.asBoolean(false);
      stageInfo.setUseRegionalUrl(useRegionalURL);
    }
  }

  private static void setupUseVirtualUrl(JsonNode jsonNode, StageInfo stageInfo) {
    if (stageInfo.getStageType() != StageInfo.StageType.GCS) {
      return;
    }
    JsonNode useVirtualURLNode = jsonNode.path("data").path("stageInfo").path("useVirtualUrl");
    if (!useVirtualURLNode.isMissingNode()) {
      boolean useVirtualURL = useVirtualURLNode.asBoolean(false);
      stageInfo.setUseVirtualUrl(useVirtualURL);
    } else {
      logger.debug("useVirtualUrl property missing from stage info");
    }
  }

  private static Map<?, ?> extractStageCreds(JsonNode rootNode, String queryId)
      throws SnowflakeSQLException {
    JsonNode credsNode = rootNode.path("data").path("stageInfo").path("creds");
    Map<?, ?> stageCredentials = null;

    try {
      TypeReference<HashMap<String, String>> typeRef =
          new TypeReference<HashMap<String, String>>() {};
      stageCredentials = mapper.readValue(credsNode.toString(), typeRef);

    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
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

  public static List<SnowflakeFileTransferMetadata> getFileTransferMetadatas(JsonNode jsonNode)
      throws SnowflakeSQLException {
    return getFileTransferMetadatas(jsonNode, null);
  }

  /**
   * This is API function to parse the File Transfer Metadatas from a supplied PUT call response.
   *
   * <p>NOTE: It only supports PUT on S3/AZURE/GCS (i.e. NOT LOCAL_FS)
   *
   * <p>It also assumes there is no active SFSession
   *
   * @param jsonNode JSON doc returned by GS from PUT call
   * @param queryId String last executed query id if available
   * @return The file transfer metadatas for to-be-transferred files.
   * @throws SnowflakeSQLException if any error occurs
   */
  public static List<SnowflakeFileTransferMetadata> getFileTransferMetadatas(
      JsonNode jsonNode, String queryId) throws SnowflakeSQLException {
    CommandType commandType =
        !jsonNode.path("data").path("command").isMissingNode()
            ? CommandType.valueOf(jsonNode.path("data").path("command").asText())
            : CommandType.UPLOAD;
    if (commandType != CommandType.UPLOAD) {
      throw new SnowflakeSQLException(
          queryId, ErrorCode.INTERNAL_ERROR, "This API only supports PUT commands");
    }

    JsonNode locationsNode = jsonNode.path("data").path("src_locations");

    if (!locationsNode.isArray()) {
      throw new SnowflakeSQLException(
          queryId, ErrorCode.INTERNAL_ERROR, "src_locations must be an array");
    }

    final String[] srcLocations;
    final List<RemoteStoreFileEncryptionMaterial> encryptionMaterial;
    try {
      srcLocations = mapper.readValue(locationsNode.toString(), String[].class);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Failed to parse the locations due to: " + ex.getMessage());
    }

    try {
      encryptionMaterial = getEncryptionMaterial(commandType, jsonNode);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Failed to parse encryptionMaterial due to: " + ex.getMessage());
    }

    // For UPLOAD we expect encryptionMaterial to have length 1
    if (encryptionMaterial.size() != 1) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Encryption material for UPLOAD should have size 1 but have "
              + encryptionMaterial.size());
    }

    final Set<String> sourceFiles = expandFileNames(srcLocations, queryId);

    StageInfo stageInfo = getStageInfo(jsonNode, null /*SFSession*/);

    List<SnowflakeFileTransferMetadata> result = new ArrayList<>();
    if (stageInfo.getStageType() != StageInfo.StageType.GCS
        && stageInfo.getStageType() != StageInfo.StageType.AZURE
        && stageInfo.getStageType() != StageInfo.StageType.S3) {
      throw new SnowflakeSQLException(
          queryId,
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

  static Set<String> expandFileNames(String[] filePathList, String queryId)
      throws SnowflakeSQLException {
    Set<String> result = new HashSet<String>();

    // a location to file pattern map so that we only need to list the
    // same directory once when they appear in multiple times.
    Map<String, List<String>> locationToFilePatterns;

    locationToFilePatterns = new HashMap<String, List<String>>();

    String cwd = systemGetProperty("user.dir");

    for (String path : filePathList) {
      // replace ~ with user home
      if (path.startsWith("~")) {
        path = systemGetProperty("user.home") + path.substring(1);
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
    for (Map.Entry<String, List<String>> entry : locationToFilePatterns.entrySet()) {
      try {
        File dir = new File(entry.getKey());

        logger.debug(
            "Listing files under: {} with patterns: {}",
            entry.getKey(),
            entry.getValue().toString());

        // Normal flow will never hit here. This is only for testing purposes
        if (isInjectedFileTransferExceptionEnabled()
            && injectedFileTransferException instanceof Exception) {
          throw (Exception) SnowflakeFileTransferAgent.injectedFileTransferException;
        }
        // The following currently ignore sub directories
        File[] filesMatchingPattern =
            dir.listFiles((FileFilter) new WildcardFileFilter(entry.getValue()));
        if (filesMatchingPattern != null) {
          for (File file : filesMatchingPattern) {
            result.add(file.getCanonicalPath());
          }
        } else {
          logger.debug("No files under {} matching pattern {}", entry.getKey(), entry.getValue());
        }
      } catch (Exception ex) {
        throw new SnowflakeSQLException(
            queryId,
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
      logger.debug("File: {}", filePath);
    }

    return result;
  }

  /**
   * Replicated from SnowflakeFileTransferAgent.parseCommandInGS. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   */
  private static com.fasterxml.jackson.databind.JsonNode parseCommandInGS(
      net.snowflake.client.core.SFStatement statement, String command)
      throws SnowflakeSQLException, net.snowflake.client.jdbc.SnowflakeSQLException {
    Object result = null;
    // send the command to GS
    try {
      result =
          statement.executeHelper(
              command,
              "application/json",
              null, // bindValues
              false, // describeOnly
              false, // internal
              false, // async
              new net.snowflake.client.core
                  .ExecTimeTelemetryData()); // OOB telemetry timing queries
    } catch (net.snowflake.client.core.SFException ex) {
      throw new SnowflakeSQLException(
          ex.getQueryId(), ex, ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    com.fasterxml.jackson.databind.JsonNode jsonNode =
        (com.fasterxml.jackson.databind.JsonNode) result;

    logger.debug(
        "Response: {}", net.snowflake.client.util.SecretDetector.maskSecrets(jsonNode.toString()));

    net.snowflake.client.jdbc.SnowflakeUtil.checkErrorAndThrowException(jsonNode);
    return jsonNode;
  }

  /**
   * Replicated from SnowflakeFileTransferAgent.getLocalFilePathFromCommand. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   */
  private static String getLocalFilePathFromCommand(String command, boolean unescape) {
    if (command == null) {
      logger.error("null command", false);
      return null;
    }

    if (command.indexOf(FILE_PROTOCOL) < 0) {
      logger.error("file:// prefix not found in command: {}", command);
      return null;
    }

    int localFilePathBeginIdx = command.indexOf(FILE_PROTOCOL) + FILE_PROTOCOL.length();
    boolean isLocalFilePathQuoted =
        (localFilePathBeginIdx > FILE_PROTOCOL.length())
            && (command.charAt(localFilePathBeginIdx - 1 - FILE_PROTOCOL.length()) == '\'');

    // the ending index is exclusive
    int localFilePathEndIdx = 0;
    String localFilePath = "";

    if (isLocalFilePathQuoted) {
      // look for the matching quote
      localFilePathEndIdx = command.indexOf("'", localFilePathBeginIdx);
      if (localFilePathEndIdx > localFilePathBeginIdx) {
        localFilePath = command.substring(localFilePathBeginIdx, localFilePathEndIdx);
      }
      // unescape backslashes to match the file name from GS
      if (unescape) {
        localFilePath = localFilePath.replaceAll("\\\\\\\\", "\\\\");
      }
    } else {
      // look for the first space or new line or semi colon
      java.util.List<Integer> indexList = new java.util.ArrayList<>();
      char[] delimiterChars = {' ', '\n', ';'};
      for (int i = 0; i < delimiterChars.length; i++) {
        int charIndex = command.indexOf(delimiterChars[i], localFilePathBeginIdx);
        if (charIndex != -1) {
          indexList.add(charIndex);
        }
      }

      localFilePathEndIdx = indexList.isEmpty() ? -1 : java.util.Collections.min(indexList);

      if (localFilePathEndIdx > localFilePathBeginIdx) {
        localFilePath = command.substring(localFilePathBeginIdx, localFilePathEndIdx);
      } else if (localFilePathEndIdx == -1) {
        localFilePath = command.substring(localFilePathBeginIdx);
      }
    }

    return localFilePath;
  }

  private static final String FILE_PROTOCOL = "file://";

  /**
   * Replicated from SnowflakeFileTransferAgent.renewExpiredToken. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   */
  public static void renewExpiredToken(
      net.snowflake.client.core.SFSession session, String command, SnowflakeStorageClient client)
      throws SnowflakeSQLException, net.snowflake.client.jdbc.SnowflakeSQLException {
    net.snowflake.client.core.SFStatement statement =
        new net.snowflake.client.core.SFStatement(session);
    com.fasterxml.jackson.databind.JsonNode jsonNode = parseCommandInGS(statement, command);
    String queryId = jsonNode.path("data").path("queryId").asText();
    java.util.Map<?, ?> stageCredentials = extractStageCreds(jsonNode, queryId);

    // renew client with the fresh token
    logger.debug("Renewing expired access token");
    client.renew(stageCredentials);
  }

  // ---- Inner classes and methods replicated from JDBC for uploadWithoutConnection ----

  static class InputStreamWithMetadata {
    long size;
    String digest;
    FileBackedOutputStream fileBackedOutputStream;

    InputStreamWithMetadata(
        long size, String digest, FileBackedOutputStream fileBackedOutputStream) {
      this.size = size;
      this.digest = digest;
      this.fileBackedOutputStream = fileBackedOutputStream;
    }
  }

  private static class remoteLocation {
    String location;
    String path;

    public remoteLocation(String remoteStorageLocation, String remotePath) {
      location = remoteStorageLocation;
      path = remotePath;
    }
  }

  public static remoteLocation extractLocationAndPath(String stageLocationPath) {
    String location = stageLocationPath;
    String path = "";

    // split stage location as location name and path
    if (stageLocationPath.contains("/")) {
      location = stageLocationPath.substring(0, stageLocationPath.indexOf("/"));
      path = stageLocationPath.substring(stageLocationPath.indexOf("/") + 1);
    }

    return new remoteLocation(location, path);
  }

  private static InputStreamWithMetadata compressStreamWithGZIP(
      InputStream inputStream, net.snowflake.client.core.SFBaseSession session, String queryId)
      throws SnowflakeSQLException {
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

      return new InputStreamWithMetadata(
          countingStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          tempStream);

    } catch (IOException | NoSuchAlgorithmException ex) {
      logger.error("Exception compressing input stream", ex);

      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "error encountered for compression");
    }
  }

  @Deprecated
  private static InputStreamWithMetadata compressStreamWithGZIPNoDigest(
      InputStream inputStream, net.snowflake.client.core.SFBaseSession session, String queryId)
      throws SnowflakeSQLException {
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
      return new InputStreamWithMetadata(countingStream.getCount(), null, tempStream);

    } catch (IOException ex) {
      logger.error("Exception compressing input stream", ex);

      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "error encountered for compression");
    }
  }

  private static InputStreamWithMetadata computeDigest(InputStream is, boolean resetStream)
      throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    if (resetStream) {
      FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

      CountingOutputStream countingOutputStream = new CountingOutputStream(tempStream);

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);

      IOUtils.copy(is, digestStream);

      return new InputStreamWithMetadata(
          countingOutputStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          tempStream);
    } else {
      CountingOutputStream countingOutputStream =
          new CountingOutputStream(ByteStreams.nullOutputStream());

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);
      IOUtils.copy(is, digestStream);
      return new InputStreamWithMetadata(
          countingOutputStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          null);
    }
  }

  public static void uploadWithoutConnection(SnowflakeFileTransferConfig config) throws Exception {
    logger.trace("Entering uploadWithoutConnection...");

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) config.getSnowflakeFileTransferMetadata();
    InputStream uploadStream = config.getUploadStream();
    boolean requireCompress = config.getRequireCompress();
    int networkTimeoutInMilli = config.getNetworkTimeoutInMilli();
    net.snowflake.ingest.utils.OCSPMode ocspMode = config.getOcspMode();
    Properties proxyProperties = config.getProxyProperties();
    String streamingIngestClientName = config.getStreamingIngestClientName();
    String streamingIngestClientKey = config.getStreamingIngestClientKey();

    // Create HttpClient key
    net.snowflake.client.core.HttpClientSettingsKey key =
        net.snowflake.client.jdbc.SnowflakeUtil.convertProxyPropertiesToHttpClientKey(
            net.snowflake.client.core.OCSPMode.FAIL_OPEN, proxyProperties);

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
                ? compressStreamWithGZIPNoDigest(uploadStream, /* session= */ null, null)
                : compressStreamWithGZIP(uploadStream, /* session= */ null, encMat.getQueryId()));

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
          StorageClientFactory.getFactory().createClient(stageInfo, 1, encMat, /* session= */ null);

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()) {
        throw (Exception) SnowflakeFileTransferAgent.injectedFileTransferException;
      }

      String queryId = encMat != null && encMat.getQueryId() != null ? encMat.getQueryId() : null;
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
              config.getSession(),
              config.getCommand(),
              1,
              fileToUpload,
              (fileToUpload == null),
              encMat,
              streamingIngestClientName,
              streamingIngestClientKey,
              queryId);
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
              streamingIngestClientKey,
              queryId);
          break;
      }
    } catch (Exception ex) {
      if (!config.isSilentException()) {
        logger.error("Exception encountered during file upload in uploadWithoutConnection", ex);
      }
      throw ex;
    } finally {
      if (fileBackedOutputStream != null) {
        try {
          fileBackedOutputStream.reset();
        } catch (IOException ex) {
          logger.debug("Failed to clean up temp file: {}", ex);
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
      net.snowflake.client.core.SFSession session,
      String command,
      int parallel,
      File srcFile,
      boolean uploadFromStream,
      RemoteStoreFileEncryptionMaterial encMat,
      String streamingIngestClientName,
      String streamingIngestClientKey,
      String queryId)
      throws SQLException, IOException {
    remoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    String origDestFileName = destFileName;
    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
          remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.debug(
        "Upload object. Location: {}, key: {}, srcFile: {}, encryption: {}",
        remoteLocation.location,
        destFileName,
        srcFile,
        (net.snowflake.ingest.streaming.internal.fileTransferAgent.log.ArgSupplier)
            () -> (encMat == null ? "NULL" : encMat.getSmkId() + "|" + encMat.getQueryId()));

    StorageObjectMetadata meta =
        StorageClientFactory.getFactory().createStorageMetadataObj(stage.getStageType());
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
      if (initialClient.requirePresignedUrl()) {
        // need to replace file://mypath/myfile?.csv with file://mypath/myfile1.csv.gz
        String localFilePath = getLocalFilePathFromCommand(command, false);
        String commandWithExactPath = command.replace(localFilePath, origDestFileName);
        // then hand that to GS to get the actual presigned URL we'll use
        net.snowflake.client.core.SFStatement statement =
            new net.snowflake.client.core.SFStatement(session);
        com.fasterxml.jackson.databind.JsonNode jsonNode =
            parseCommandInGS(statement, commandWithExactPath);

        if (!jsonNode.path("data").path("stageInfo").path("presignedUrl").isMissingNode()) {
          presignedUrl = jsonNode.path("data").path("stageInfo").path("presignedUrl").asText();
        }
      }
      initialClient.upload(
          session,
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
          presignedUrl,
          queryId);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }

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
      net.snowflake.client.core.HttpClientSettingsKey ocspModeAndProxyKey,
      int parallel,
      File srcFile,
      boolean uploadFromStream,
      RemoteStoreFileEncryptionMaterial encMat,
      String presignedUrl,
      String streamingIngestClientName,
      String streamingIngestClientKey,
      String queryId)
      throws SQLException, IOException {
    remoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
          remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.debug("Upload object. Location: {}, key: {}", remoteLocation.location, destFileName);

    StorageObjectMetadata meta =
        StorageClientFactory.getFactory().createStorageMetadataObj(stage.getStageType());
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
          presignedUrl,
          queryId);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }
}

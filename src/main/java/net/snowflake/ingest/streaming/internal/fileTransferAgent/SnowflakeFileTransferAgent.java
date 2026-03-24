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
 * CommandType inner enum kept (originally from SFBaseFileTransferAgent parent class).
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.isNullOrEmpty;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.systemGetProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/** Class for uploading/downloading files */
public class SnowflakeFileTransferAgent {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeFileTransferAgent.class);

  public enum CommandType {
    UPLOAD,
    DOWNLOAD
  }

  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

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

  public static List<net.snowflake.client.jdbc.SnowflakeFileTransferMetadata>
      getFileTransferMetadatas(JsonNode jsonNode) throws SnowflakeSQLException {
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
  public static List<net.snowflake.client.jdbc.SnowflakeFileTransferMetadata>
      getFileTransferMetadatas(JsonNode jsonNode, String queryId) throws SnowflakeSQLException {
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

    List<net.snowflake.client.jdbc.SnowflakeFileTransferMetadata> result = new ArrayList<>();
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
      // Bridge to JDBC's CommandType for SnowflakeFileTransferMetadataV1 constructor.
      // Temporary until SnowflakeFileTransferMetadataV1 is replicated in Step 8.
      net.snowflake.client.jdbc.SFBaseFileTransferAgent.CommandType jdbcCommandType =
          net.snowflake.client.jdbc.SFBaseFileTransferAgent.CommandType.valueOf(commandType.name());
      result.add(
          new SnowflakeFileTransferMetadataV1(
              stageInfo.getPresignedUrl(),
              sourceFileName,
              encryptionMaterial.get(0) != null
                  ? encryptionMaterial.get(0).getQueryStageMasterKey()
                  : null,
              encryptionMaterial.get(0) != null ? encryptionMaterial.get(0).getQueryId() : null,
              encryptionMaterial.get(0) != null ? encryptionMaterial.get(0).getSmkId() : null,
              jdbcCommandType,
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
}

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration;
import net.snowflake.ingest.utils.Logging;

/**
 * Factory object for abstracting the creation of storage client objects: IcebergStorageClient and
 * StorageObjectMetadata
 *
 * @author lgiakoumakis
 */
class IcebergStorageClientFactory {

  private static final Logging logger = new Logging(IcebergStorageClientFactory.class);

  private static IcebergStorageClientFactory factory;

  private IcebergStorageClientFactory() {}

  /**
   * Creates or returns the single instance of the factory object
   *
   * @return the storage client instance
   */
  public static IcebergStorageClientFactory getFactory() {
    if (factory == null) {
      factory = new IcebergStorageClientFactory();
    }

    return factory;
  }

  /**
   * Creates a storage client based on the value of stageLocationType
   *
   * @param stage the stage properties
   * @param parallel the degree of parallelism to be used by the client
   * @return a IcebergStorageClient interface to the instance created
   * @throws SnowflakeSQLException if any error occurs
   */
  public IcebergStorageClient createClient(StageInfo stage, int parallel)
      throws SnowflakeSQLException {
    logger.logDebug("Creating storage client. Client type: {}", stage.getStageType().name());

    switch (stage.getStageType()) {
      case S3:
        boolean useS3RegionalUrl = stage.getUseS3RegionalUrl();
        return createS3Client(
            stage.getCredentials(),
            parallel,
            stage.getProxyProperties(),
            stage.getRegion(),
            stage.getEndPoint(),
            stage.getIsClientSideEncrypted(),
            useS3RegionalUrl);

      case AZURE:
        return createAzureClient(stage);

      case GCS:
        return createGCSClient(stage);

      default:
        // We don't create a storage client for FS_LOCAL,
        // so we should only find ourselves here if an unsupported
        // remote storage client type is specified
        throw new IllegalArgumentException(
            "Unsupported storage client specified: " + stage.getStageType().name());
    }
  }

  /**
   * Creates a SnowflakeS3ClientObject which encapsulates the Amazon S3 client
   *
   * @param stageCredentials Map of stage credential properties
   * @param parallel degree of parallelism
   * @param stageRegion the region where the stage is located
   * @param stageEndPoint the FIPS endpoint for the stage, if needed
   * @param isClientSideEncrypted whether client-side encryption should be used
   * @param useS3RegionalUrl
   * @return the IcebergS3Client instance created
   * @throws SnowflakeSQLException failure to create the S3 client
   */
  private IcebergStorageClient createS3Client(
      Map<?, ?> stageCredentials,
      int parallel,
      Properties proxyProperties,
      String stageRegion,
      String stageEndPoint,
      boolean isClientSideEncrypted,
      boolean useS3RegionalUrl)
      throws SnowflakeSQLException {
    final int S3_TRANSFER_MAX_RETRIES = 3;

    IcebergS3Client s3Client;

    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setMaxConnections(parallel + 1);
    clientConfig.setMaxErrorRetry(S3_TRANSFER_MAX_RETRIES);
    clientConfig.setDisableSocketProxy(HttpUtil.isSocksProxyDisabled());

    // If proxy is set via connection properties or JVM settings these will be overridden later.
    // This is to prevent the aws client builder from reading proxy environment variables.
    clientConfig.setProxyHost("");
    clientConfig.setProxyPort(0);
    clientConfig.setProxyUsername("");
    clientConfig.setProxyPassword("");

    logger.logDebug(
        "S3 client configuration: maxConnection: {}, connectionTimeout: {}, "
            + "socketTimeout: {}, maxErrorRetry: {}",
        clientConfig.getMaxConnections(),
        clientConfig.getConnectionTimeout(),
        clientConfig.getSocketTimeout(),
        clientConfig.getMaxErrorRetry());

    try {
      s3Client =
          new IcebergS3Client(
              stageCredentials,
              clientConfig,
              proxyProperties,
              stageRegion,
              stageEndPoint,
              isClientSideEncrypted,
              useS3RegionalUrl);
    } catch (Exception ex) {
      logger.logDebug("Exception creating s3 client", ex);
      throw ex;
    }
    logger.logDebug("S3 Storage client created", false);

    return s3Client;
  }

  /**
   * Creates a storage provider specific metadata object, accessible via the platform independent
   * interface
   *
   * @param stageType determines the implementation to be created
   * @return the implementation of StorageObjectMetadata
   */
  public StorageObjectMetadata createStorageMetadataObj(StageInfo.StageType stageType) {
    switch (stageType) {
      case S3:
        return new IcebergS3ObjectMetadata();

      case AZURE:
      case GCS:
        // GCS's metadata object looks just like Azure's (Map<String, String>),
        // so for now we'll use the same class.
        return new IcebergCommonObjectMetadata();

      default:
        // An unsupported remote storage client type was specified
        // We don't create/implement a storage client for FS_LOCAL,
        // so we should never end up here while running on local file system
        throw new IllegalArgumentException("Unsupported stage type specified: " + stageType.name());
    }
  }

  /**
   * Creates a SnowflakeAzureClientObject which encapsulates the Azure Storage client
   *
   * @param stage Stage information
   * @return the SnowflakeAzureClientObject instance created
   */
  private IcebergStorageClient createAzureClient(StageInfo stage) throws SnowflakeSQLException {
    IcebergAzureClient azureClient;

    try {
      azureClient = IcebergAzureClient.createSnowflakeAzureClient(stage);
    } catch (Exception ex) {
      logger.logDebug("Exception creating Azure Storage client", ex);
      throw ex;
    }
    logger.logDebug("Azure Storage client created", false);

    return azureClient;
  }

  /**
   * Creates a IcebergGCSClient object which encapsulates the GCS Storage client
   *
   * @param stage Stage information
   * @return the IcebergGCSClient instance created
   */
  private IcebergStorageClient createGCSClient(StageInfo stage) throws SnowflakeSQLException {
    IcebergGCSClient gcsClient;

    try {
      gcsClient = IcebergGCSClient.createSnowflakeGCSClient(stage);
    } catch (Exception ex) {
      logger.logDebug("Exception creating GCS Storage client", ex);
      throw ex;
    }
    logger.logDebug("GCS Storage client created", false);

    return gcsClient;
  }
}

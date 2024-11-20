package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.File;
import java.io.InputStream;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState;

interface IcebergStorageClient {

  /**
   * Adds digest metadata to the StorageObjectMetadata object
   *
   * @param meta the storage metadata object to add the digest to
   * @param digest the digest metadata to add
   */
  void addDigestMetadata(StorageObjectMetadata meta, String digest);

  /**
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   *
   * @param meta the storage metadata object to add the digest to
   * @param clientName streaming ingest client name
   * @param clientKey streaming ingest client key, provided by Snowflake
   */
  void addStreamingIngestMetadata(StorageObjectMetadata meta, String clientName, String clientKey);

  /**
   * Upload a file (-stream) to remote storage
   *
   * @param parallel number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param location s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutStr stream used for uploading if not null
   * @param meta object meta data
   * @param region region name where the stage persists
   * @param presignedUrl presigned URL for upload. Used by GCP.
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  String upload(
      int parallel,
      boolean uploadFromStream,
      String location,
      File srcFile,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      StorageObjectMetadata meta,
      String region,
      String presignedUrl)
      throws SnowflakeSQLException;

  /**
   * Upload a file (-stream) to remote storage with Pre-signed URL without JDBC connection.
   *
   * <p>NOTE: This function is only supported when pre-signed URL is used.
   *
   * @param networkTimeoutInMilli Network timeout for the upload
   * @param ocspModeAndProxyKey OCSP mode and proxy settings for the upload.
   * @param parallel number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl presigned URL for upload. Used by GCP.
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  default String uploadWithPresignedUrlWithoutConnection(
      int networkTimeoutInMilli,
      HttpClientSettingsKey ocspModeAndProxyKey,
      int parallelism,
      boolean uploadFromStream,
      String remoteStorageLocation,
      File srcFile,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      StorageObjectMetadata meta,
      String stageRegion,
      String presignedUrl)
      throws SnowflakeSQLException {
    throw new SnowflakeSQLLoggedException(
        null,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        /*session = */ "uploadWithPresignedUrlWithoutConnection"
            + " only works for pre-signed URL.");
  }

  /** @return Returns the Max number of retry attempts */
  int getMaxRetries();

  /**
   * Returns the max exponent for multiplying backoff with the power of 2, the value of 4 will give
   * us 16secs as the max number of time to sleep before retry
   *
   * @return Returns the exponent
   */
  int getRetryBackoffMaxExponent();

  /** @return Returns the min number of milliseconds to sleep before retry */
  int getRetryBackoffMin();
}

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.GeneratePresignedUrlsResponse.PresignedUrlInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeStorageClient;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageClientFactory;
import net.snowflake.client.jdbc.internal.apache.http.client.HttpResponseException;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPut;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.ByteArrayEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.google.api.client.http.HttpStatusCodes;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;

/** Handles uploading files to the Iceberg Table's external volume's table data path */
class PresignedUrlExternalVolume implements IStorage {
  // TODO everywhere: static final should be named in all capitals
  private static final Logging logger = new Logging(PresignedUrlExternalVolume.class);
  private static final int DEFAULT_PRESIGNED_URL_COUNT = 10;
  private static final int DEFAULT_PRESIGNED_URL_TIMEOUT_IN_SECONDS = 900;

  // Allowing concurrent generate URL requests is a weak form of adapting to high throughput
  // clients. The low watermark ideally should be adaptive too for such clients,will wait for perf
  // runs to show its necessary.
  private static final int MAX_CONCURRENT_GENERATE_URLS_REQUESTS = 10;
  private static final int LOW_WATERMARK_FOR_EARLY_REFRESH = 5;

  private final String clientName;
  private final String clientPrefix;
  private final Long deploymentId;
  private final String role;

  // The db name, schema name and table name for this storage location
  private final TableRef tableRef;

  // The RPC client for snowflake cloud service
  private final SnowflakeServiceClient serviceClient;

  // semaphore to limit how many RPCs go out for one location concurrently
  private final Semaphore generateUrlsSemaphore;

  // thread-safe queue of unused URLs, to be disbursed whenever flush codepath is cutting the next
  // file
  private final ConcurrentLinkedQueue<PresignedUrlInfo> presignedUrlInfos;

  // sometimes-stale counter of how many URLs are remaining, to avoid calling presignedUrls.size()
  // and increasing lock contention / volatile reads on the internal data structures inside
  // ConcurrentLinkedQueue
  private final AtomicInteger numUrlsInQueue;

  private final FileLocationInfo locationInfo;
  private final SnowflakeFileTransferMetadataWithAge fileTransferMetadata;

  PresignedUrlExternalVolume(
      String clientName,
      String clientPrefix,
      Long deploymentId,
      String role,
      TableRef tableRef,
      FileLocationInfo locationInfo,
      SnowflakeServiceClient serviceClient) {
    this.clientName = clientName;
    this.clientPrefix = clientPrefix;
    this.deploymentId = deploymentId;
    this.role = role;
    this.tableRef = tableRef;
    this.serviceClient = serviceClient;
    this.locationInfo = locationInfo;
    this.presignedUrlInfos = new ConcurrentLinkedQueue<>();
    this.numUrlsInQueue = new AtomicInteger(0);
    this.generateUrlsSemaphore = new Semaphore(MAX_CONCURRENT_GENERATE_URLS_REQUESTS);

    if (this.locationInfo.getIsClientSideEncrypted()) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          "Cannot ingest into an external volume that requests client side encryption");
    }
    if ("S3".equalsIgnoreCase(this.locationInfo.getLocationType())) {
      // add dummy values so that JDBC's S3 client creation doesn't barf on initialization.
      this.locationInfo.getCredentials().put("AWS_KEY_ID", "key");
      this.locationInfo.getCredentials().put("AWS_SECRET_KEY", "secret");
    }

    try {
      this.fileTransferMetadata =
          InternalStage.createFileTransferMetadataWithAge(this.locationInfo);
    } catch (JsonProcessingException
        | SnowflakeSQLException
        | net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException e) {
      throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }

    // the caller is just setting up this object and not expecting a URL to be returned (unlike in
    // dequeueUrlInfo), thus waitUntilAcquired=false.
    generateUrls(LOW_WATERMARK_FOR_EARLY_REFRESH, false /* waitUntilAcquired */);
  }

  // TODO : Add timing ; add logging ; add retries ; add http exception handling better than
  // client.handleEx?
  @Override
  public void put(BlobPath blobPath, byte[] blob) {
    if (this.fileTransferMetadata.isLocalFS) {
      InternalStage.putLocal(this.fileTransferMetadata.localLocation, blobPath.fileName, blob);
      return;
    }

    try {
      putRemote(blobPath.blobPath, blob);
    } catch (Throwable e) {
      throw new SFException(e, ErrorCode.BLOB_UPLOAD_FAILURE);
    }
  }

  private void putRemote(String blobPath, byte[] blob)
      throws SnowflakeSQLException, URISyntaxException, IOException {
    // TODO: Add a backlog item for somehow doing multipart upload with presigned URLs (each part
    // has its own URL) for large files

    // already verified that client side encryption is disabled, in the ctor's call to generateUrls
    final Properties proxyProperties = HttpUtil.generateProxyPropertiesForJDBC();
    final HttpClientSettingsKey key =
        SnowflakeUtil.convertProxyPropertiesToHttpClientKey(OCSPMode.FAIL_OPEN, proxyProperties);

    StageInfo stageInfo = fileTransferMetadata.fileTransferMetadata.getStageInfo();
    SnowflakeStorageClient client =
        StorageClientFactory.getFactory().createClient(stageInfo, 1, null, null);

    URIBuilder uriBuilder = new URIBuilder(blobPath);
    HttpPut httpRequest = new HttpPut(uriBuilder.build());
    httpRequest.setEntity(new ByteArrayEntity(blob));

    addHeadersToHttpRequest(httpRequest, blob, stageInfo, client);

    if (stageInfo.getStageType().equals(StageInfo.StageType.AZURE)) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Azure based external volumes are not yet supported.");

      /* commenting out unverified code, will fix this when server changes are in preprod / some test deplo
      URI storageEndpoint =
          new URI(
              "https",
              stageInfo.getStorageAccount() + "." + stageInfo.getEndPoint() + "/",
              null,
              null);
      String sasToken = blobPath.substring(blobPath.indexOf("?"));
      StorageCredentials azCreds = new StorageCredentialsSharedAccessSignature(sasToken);
      CloudBlobClient azClient = new CloudBlobClient(storageEndpoint, azCreds);

      CloudBlobContainer container = azClient.getContainerReference(stageInfo.getLocation().substring(0, stageInfo.getLocation().indexOf("/")));
      CloudBlockBlob azBlob = container.getBlockBlobReference();
      azBlob.setMetadata((HashMap<String, String>) meta.getUserMetadata());

      OperationContext opContext = new OperationContext();
      net.snowflake.client.core.HttpUtil.setSessionlessProxyForAzure(proxyProperties, opContext);

      BlobRequestOptions options = new BlobRequestOptions();

      try {
          azBlob.uploadFromByteArray(blob, 0, blob.length, null, options, opContext);
      } catch (Exception ex) {
          ((SnowflakeAzureClient) client).handleStorageException(ex, 0, "upload", null, null, null);
      }
      */
    }

    CloseableHttpClient httpClient = net.snowflake.client.core.HttpUtil.getHttpClient(key);
    CloseableHttpResponse response =
        RestRequest.execute(
            httpClient,
            httpRequest,
            0, // retry timeout
            0, // auth timeout
            (int)
                net.snowflake.client.core.HttpUtil.getSocketTimeout()
                    .toMillis(), // socket timeout in ms
            1, // max retries
            0, // no socket timeout injection
            null, // no canceling signaler, TODO: wire up thread interrupt with setting this
            // AtomicBoolean to avoid retries/sleeps
            false, // no cookie
            false, // no url retry query parameters
            false, // no request_guid
            true, // retry on HTTP 403
            true, // no retry
            new ExecTimeTelemetryData());

    int statusCode = response.getStatusLine().getStatusCode();
    if (!HttpStatusCodes.isSuccess(statusCode)) {
      Exception ex =
          new HttpResponseException(
              response.getStatusLine().getStatusCode(),
              String.format(
                  "%s, body: %s",
                  response.getStatusLine().getReasonPhrase(),
                  EntityUtils.toString(response.getEntity())));

      client.handleStorageException(ex, 0, "upload", null, null, null);
    }
  }

  private void addHeadersToHttpRequest(
      HttpPut httpRequest, byte[] blob, StageInfo stageInfo, SnowflakeStorageClient client) {
    // no need to set this as it causes a Content-length header is already set error in apache's
    // http client.
    // httpRequest.setHeader("Content-Length", "" + blob.length);

    // TODO: These custom headers need to be a part of the presigned URL HMAC computation in S3,
    // we'll disable them for now until we can do presigned URL generation AFTER we have the digest.

    /*
    final String clientKey = this.clientPrefix;
    final String clientName = this.clientName;

    final byte[] digestBytes;
    try {
        digestBytes = MessageDigest.getInstance("SHA-256").digest(blob);
    } catch (NoSuchAlgorithmException e) {
        throw new SFException(e, ErrorCode.INTERNAL_ERROR);
    }

    final String digest = Base64.getEncoder().encodeToString(digestBytes);

    StorageObjectMetadata meta = StorageClientFactory.getFactory().createStorageMetadataObj(stageInfo.getStageType());

    client.addDigestMetadata(meta, digest);
    client.addStreamingIngestMetadata(meta, clientName, clientKey);

    switch (stageInfo.getStageType()) {
        case S3:
            httpRequest.setHeader("x-amz-server-side-encryption", ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            httpRequest.setHeader("x-amz-checksum-sha256", digest); // TODO why does JDBC use a custom x–amz-meta-sfc-digest header for this
            for (Map.Entry<String, String> entry : meta.getUserMetadata().entrySet()) {
                httpRequest.setHeader("x-amz-meta-" + entry.getKey(), entry.getValue());
            }

            break;

        case AZURE:
            for (Map.Entry<String, String> entry : meta.getUserMetadata().entrySet()) {
                httpRequest.setHeader("x-ms-meta-" + entry.getKey(), entry.getValue());
            }
            break;

        case GCS:
            for (Map.Entry<String, String> entry : meta.getUserMetadata().entrySet()) {
                httpRequest.setHeader("x-goog-meta-" + entry.getKey(), entry.getValue());
            }
            break;
    }
    */
  }

  PresignedUrlInfo dequeueUrlInfo() {
    // use a 60 second buffer in case the executor service is backed up / serialization takes time /
    // upload runs slow / etc.
    long validUntilAtleastTimestamp = System.currentTimeMillis() + 60 * 1000;

    // TODO: Wire in a checkStop to get out of this infinite loop.
    while (true) {
      PresignedUrlInfo info = this.presignedUrlInfos.poll();
      if (info == null) {
        // if the queue is empty, trigger a url refresh AND wait for it to complete.
        // loop around when done to try and ready from the queue again.
        generateUrls(LOW_WATERMARK_FOR_EARLY_REFRESH, true /* waitUntilAcquired */);
        continue;
      }

      // we dequeued a url, do the appropriate bookkeeping.
      int remainingUrlsInQueue = this.numUrlsInQueue.decrementAndGet();

      if (info.validUntilTimestamp < validUntilAtleastTimestamp) {
        // This url can expire by the time it gets used, loop around and dequeue another URL.
        continue;
      }

      // if we're nearing url exhaustion, go fetch another batch. Don't wait for the response as we
      // already have a valid URL to be used by the current caller of dequeueUrlInfo.
      if (remainingUrlsInQueue <= LOW_WATERMARK_FOR_EARLY_REFRESH) {
        // TODO: do this generation on a background thread to allow the current thread to make
        // progress ?  Will wait for perf runs to know this is an issue that needs addressal.
        generateUrls(LOW_WATERMARK_FOR_EARLY_REFRESH, false /* waitUntilAcquired */);
      }

      return info;
    }
  }

  // NOTE : We are intentionally NOT re-enqueuing unused URLs here as that can cause correctness
  // issues by accidentally enqueuing a URL that was actually used to write data out. Its okay to
  // allow an unused URL to go waste as we'll just go out and generate new URLs.
  // Do NOT add an enqueueUrl() method for this reason.

  /**
   * Fetches new presigned URLs from snowflake.
   *
   * @param minCountToSkipGeneration Skip the RPC if we already have this many URLs in the queue
   * @param waitUntilAcquired when true, make the current thread block on having enough URLs in the
   *     queue
   */
  private void generateUrls(int minCountToSkipGeneration, boolean waitUntilAcquired) {
    int numAcquireAttempts = 0;
    boolean acquired = false;

    while (!acquired && numAcquireAttempts++ < 300) {
      // Use an aggressive timeout value as its possible that the other requests finished and added
      // enough URLs to the queue. If we use a higher timeout value, this calling thread's flush is
      // going to unnecessarily be blocked when URLs have already been added to the queue.
      try {
        // If waitUntilAcquired is false, the caller is not interested in waiting for the results
        // The semaphore being already "full" implies there are many another requests in flight
        // and we can just early exit to the caller.
        int timeoutInSeconds = waitUntilAcquired ? 1 : 0;
        acquired = this.generateUrlsSemaphore.tryAcquire(timeoutInSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // if the thread was interrupted there's nothing we can do about it, definitely shouldn't
        // continue processing.

        // reset the interrupted flag on the thread in case someone in the callstack wants to
        // gracefully continue processing.
        boolean interrupted = Thread.interrupted();

        String message =
            String.format(
                "Semaphore acquisition in ExternalVolume.generateUrls was interrupted, likely"
                    + " because the process is shutting down. TableRef=%s Thread.interrupted=%s",
                tableRef, interrupted);
        logger.logError(message);
        throw new SFException(ErrorCode.INTERNAL_ERROR, message);
      }

      // In case Acquire took time because no permits were available, it implies we already had N
      // other threads fetching more URLs. In that case we should be content with what's in the
      // buffer instead of doing another RPC potentially unnecessarily.
      if (this.numUrlsInQueue.get() >= minCountToSkipGeneration) {
        // release the semaphore before early-exiting to avoid a leak in semaphore permits.
        if (acquired) {
          this.generateUrlsSemaphore.release();
        }

        return;
      }

      // If we couldn't acquire the semaphore, and the caller was doing an optimistic generateUrls
      // but does NOT want to wait around for a successful generatePresignedUrlsResponse, then
      // early exit and allow the caller to move on.
      if (!acquired && !waitUntilAcquired) {
        logger.logDebug(
            "Skipping generateUrls because semaphore acquisition failed AND waitUntilAcquired =="
                + " false.");
        return;
      }
    }

    // if we're here without acquiring, that implies the numAcquireAttempts went over 300. We're at
    // an impasse and so there's nothing more to be done except error out, as that gives the client
    // a chance to
    // restart.
    if (!acquired) {
      String message =
          String.format("Could not acquire semaphore to generate URLs. TableRef=%s", tableRef);
      logger.logError(message);
      throw new SFException(ErrorCode.INTERNAL_ERROR, message);
    }

    // we have acquired a semaphore permit at this point, must release before returning

    try {
      long currentTimestamp = System.currentTimeMillis();
      long validUntilTimestamp =
          currentTimestamp + (DEFAULT_PRESIGNED_URL_TIMEOUT_IN_SECONDS * 1000);
      GeneratePresignedUrlsResponse response =
          doGenerateUrls(DEFAULT_PRESIGNED_URL_TIMEOUT_IN_SECONDS);
      List<PresignedUrlInfo> urlInfos = response.getPresignedUrlInfos();
      urlInfos =
          urlInfos.stream()
              .map(
                  info -> {
                    info.validUntilTimestamp = validUntilTimestamp;
                    return info;
                  })
              .filter(
                  info -> {
                    if (info == null
                        || info.url == null
                        || info.fileName == null
                        || info.url.isEmpty()) {
                      logger.logError(
                          "Received unexpected null or empty URL in externalVolume.generateUrls"
                              + " tableRef=%s",
                          this.tableRef);
                      return false;
                    }

                    return true;
                  })
              .collect(Collectors.toList());

      // these are both thread-safe operations individually, and there is no need to do them inside
      // a lock. For an infinitesimal time the numUrlsInQueue will under represent the number of
      // entries in the queue.
      this.presignedUrlInfos.addAll(urlInfos);
      this.numUrlsInQueue.addAndGet(urlInfos.size());
    } finally {
      this.generateUrlsSemaphore.release();
    }
  }

  private GeneratePresignedUrlsResponse doGenerateUrls(int timeoutInSeconds) {
    try {
      return this.serviceClient.generatePresignedUrls(
          new GeneratePresignedUrlsRequest(
              tableRef, role, DEFAULT_PRESIGNED_URL_COUNT, timeoutInSeconds, deploymentId, true));

    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.GENERATE_PRESIGNED_URLS_FAILURE, e.getMessage());
    }
  }
}

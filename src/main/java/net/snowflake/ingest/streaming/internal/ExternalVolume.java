package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeStorageClient;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageClientFactory;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata;
import net.snowflake.client.jdbc.internal.apache.http.client.HttpResponseException;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPut;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.ByteArrayEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.google.api.client.http.HttpStatusCodes;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentials;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobClient;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Handles uploading files to the Iceberg Table's external volume's table data path */
class ExternalVolume implements IStorage {
    // TODO everywhere: static final should be named in all capitals
    private static final Logging logger = new Logging(ExternalVolume.class);
    private static final int DEFAULT_PRESIGNED_URL_COUNT = 10;

    // Allowing concurrent generate URL requests is a weak form of adapting to high throughput clients.
    // The low watermark ideally should be adaptive too for such clients,will wait for perf runs to show its necessary.
    private static final int MAX_CONCURRENT_GENERATE_URLS_REQUESTS = 10;
    private static final int LOW_WATERMARK_FOR_EARLY_REFRESH = 5;

    private String clientName;
    private String clientPrefix;

    // The db name, schema name and table name for this storage location
    private final TableRef tableRef;

    // The RPC client for snowflake cloud service
    private final SnowflakeServiceClient serviceClient;

    // semaphore to limit how many RPCs go out for one location concurrently
    private final Semaphore generateUrlsSemaphore;

    // thread-safe queue of unused URLs, to be disbursed whenever flush codepath is cutting the next file
    private final ConcurrentLinkedQueue<String> presignedUrls;

    // sometimes-stale counter of how many URLs are remaining, to avoid calling presignedUrls.size() and increasing
    // lock contention / volatile reads on the internal data structures inside ConcurrentLinkedQueue
    private final AtomicInteger numUrlsInQueue;

    private String encodedFigsId;
    private String serializationPolicy;
    private FileLocationInfo locationInfo;
    private SnowflakeFileTransferMetadataWithAge fileTransferMetadata;

    ExternalVolume(String clientName, String clientPrefix, TableRef tableRef, SnowflakeServiceClient serviceClient) {
        this.clientName = clientName;
        this.clientPrefix = clientPrefix;
        this.tableRef = tableRef;
        this.serviceClient = serviceClient;
        this.encodedFigsId = null;

        this.presignedUrls = new ConcurrentLinkedQueue<>();
        this.numUrlsInQueue = new AtomicInteger(0);
        this.generateUrlsSemaphore = new Semaphore(MAX_CONCURRENT_GENERATE_URLS_REQUESTS);
        generateUrls(true, LOW_WATERMARK_FOR_EARLY_REFRESH);
    }

    // TODO : Add timing ; add logging ; add retries
    @Override
    public void put(String blobPath, byte[] blob) {
        if (this.fileTransferMetadata.isLocalFS) {
            InternalStage.putLocal(this.fileTransferMetadata.localLocation, blobPath, blob);
            return;
        }

        try {
            putRemote(blobPath, blob);
        } catch (SnowflakeSQLException | URISyntaxException | IOException e) {
            throw new SFException(e, ErrorCode.BLOB_UPLOAD_FAILURE);
        }
    }

    private void putRemote(String blobPath, byte[] blob) throws SnowflakeSQLException, URISyntaxException, IOException {
        // already verified that client side encryption is disabled, in the ctor's call to generateUrls
        final Properties proxyProperties =  HttpUtil.generateProxyPropertiesForJDBC();
        final HttpClientSettingsKey key = SnowflakeUtil.convertProxyPropertiesToHttpClientKey(
                OCSPMode.FAIL_OPEN, proxyProperties);
        final String clientKey = this.clientPrefix;
        final String clientName = this.clientName;

        final byte[] digestBytes;
        try {
            digestBytes = MessageDigest.getInstance("SHA-256").digest(blob);
        } catch (NoSuchAlgorithmException e) {
            throw new SFException(e, ErrorCode.INTERNAL_ERROR);
        }

        final String digest = Base64.getEncoder().encodeToString(digestBytes);

        StageInfo stageInfo = fileTransferMetadata.fileTransferMetadata.getStageInfo();

        StorageClientFactory clientFactory = StorageClientFactory.getFactory();
        SnowflakeStorageClient client = clientFactory.createClient(stageInfo, 1, null, null);
        StorageObjectMetadata meta = clientFactory.createStorageMetadataObj(stageInfo.getStageType());

        // TODO: These custom headers might need to be a part of the presigned URL HMAC computation, in which case
        // we'll disable them for now and possibly add custom metadata into the parquet file instead.
        client.addDigestMetadata(meta, digest);
        client.addStreamingIngestMetadata(meta, clientName, clientKey);

        URIBuilder uriBuilder = new URIBuilder(blobPath);
        HttpPut httpRequest = new HttpPut(uriBuilder.build());
        httpRequest.addHeader("Content-Length", Long.toString(meta.getContentLength()));
        httpRequest.setEntity(new ByteArrayEntity(blob));

        switch (stageInfo.getStageType()) {
            case S3:
                // TODO: Add a backlog item for somehow doing multipart upload with presigned URLs (each part has its own URL) for large files
                httpRequest.setHeader("x-amz-server-side-encryption", ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                httpRequest.setHeader("x-amz-checksum-sha256", digest); // TODO why does JDBC use a custom x–amz-meta-sfc-digest header for this
                for (Map.Entry<String, String> entry : meta.getUserMetadata().entrySet()) {
                    httpRequest.addHeader("x-amz-meta-" + entry.getKey(), entry.getValue());
                }

                break;

            case AZURE:
                for (Map.Entry<String, String> entry : meta.getUserMetadata().entrySet()) {
                    httpRequest.addHeader("x-ms-meta-" + entry.getKey(), entry.getValue());
                }

                URI storageEndpoint = new URI("https", stageInfo.getStorageAccount() + "." + stageInfo.getEndPoint() + "/", null, null);
                String sasToken = blobPath.substring(blobPath.indexOf("?"));
                StorageCredentials azCreds = new StorageCredentialsSharedAccessSignature(sasToken);
                CloudBlobClient azClient = new CloudBlobClient(storageEndpoint, azCreds);
/*                CloudBlobContainer container = azClient.getContainerReference(stageInfo.getLocation().substring(0, stageInfo.getLocation().indexOf("/")));
                CloudBlockBlob azBlob = container.getBlockBlobReference();
                azBlob.setMetadata((HashMap<String, String>) meta.getUserMetadata());

                OperationContext opContext = new OperationContext();
                net.snowflake.client.core.HttpUtil.setSessionlessProxyForAzure(proxyProperties, opContext);

                BlobRequestOptions options = new BlobRequestOptions();

                try {
                    azBlob.uploadFromByteArray(blob, 0, blob.length, null, options, opContext);
                } catch (Exception ex) {
                    ((SnowflakeAzureClient) client).handleStorageException(ex, 0, "upload", null, null, null);
                }*/
                break;

            case GCS:
                for (Map.Entry<String, String> entry : meta.getUserMetadata().entrySet()) {
                    httpRequest.addHeader("x-goog-meta-" + entry.getKey(), entry.getValue());
                }

                break;
        }

        CloseableHttpClient httpClient = net.snowflake.client.core.HttpUtil.getHttpClient(key);
        CloseableHttpResponse response =
                RestRequest.execute(
                        httpClient,
                        httpRequest,
                        0, // retry timeout
                        0, // auth timeout
                        (int) net.snowflake.client.core.HttpUtil.getSocketTimeout().toMillis(), // socket timeout in ms
                        client.getMaxRetries(),
                        0, // no socket timeout injection
                        null, // no canceling
                        false, // no cookie
                        false, // no url retry query parameters
                        false, // no request_guid
                        true, // retry on HTTP 403
                        false, // enable retries on transient errors
                        new ExecTimeTelemetryData());

        int statusCode = response.getStatusLine().getStatusCode();
        if (!HttpStatusCodes.isSuccess(statusCode)) {
            Exception ex = new HttpResponseException(
                    response.getStatusLine().getStatusCode(),
                    String.format(
                            "%s, body: %s",
                            response.getStatusLine().getReasonPhrase(),
                            EntityUtils.toString(response.getEntity())));

            client.handleStorageException(ex, 0, "upload", null, null, null);
        }
    }

    String dequeueUrl() {
        String url = this.presignedUrls.poll();
        boolean generate = false;
        if (url == null) {
            generate = true;
        } else {
            // Since the queue had a non-null entry, there is no way numUrlsInQueue is <=0
            int remainingUrlsInQueue = this.numUrlsInQueue.decrementAndGet();
            if (remainingUrlsInQueue <= LOW_WATERMARK_FOR_EARLY_REFRESH) {
                generate = true;
                // assert remaininUrlsInQueue >= 0
            }
        }
        if (generate) {
            // TODO: do this generation on a background thread to allow the current thread to make progress
            // Will wait for perf runs to know this is an issue that needs addressal.
            generateUrls(false, LOW_WATERMARK_FOR_EARLY_REFRESH);
        }
        return url;
    }

    // NOTE : We are intentionally NOT re-enqueuing unused URLs here as that can cause correctness issues by
    // accidentally enqueuing a URL that was actually used to write data out. Its okay to allow an unused URL to go waste
    // as we'll just go out and generate new URLs.
    // Do NOT add an enqueueUrl() method for this reason.

    private void generateUrls(boolean initial, int minCountToSkipGeneration) {
        int numAcquireAttempts = 0;
        boolean acquired = false;

        while (!acquired && numAcquireAttempts++ < 300) {
            // Use an aggressive timeout value as its possible that the other requests finished and added enough
            // URLs to the queue. If we use a higher timeout value, this calling thread's flush is going to
            // unnecessarily be blocked when URLs have already been added to the queue.
            try {
                acquired = this.generateUrlsSemaphore.tryAcquire(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // if the thread was interrupted there's nothing we can do about it, definitely shouldn't continue
                // processing.
                String message = String.format("Semaphore acquisition in ExternalVolume.generateUrls was interrupted, " +
                        "likely because the process is shutting down. TableRef=%s", tableRef);
                logger.logError(message);
                throw new SFException(ErrorCode.INTERNAL_ERROR, message);
            }

            // In case Acquire took time because no permits were available, it implies we already had N other threads
            // fetching more URLs. In that case we should be content with what's in the buffer instead
            // of doing another RPC potentially unnecessarily.
            if (this.numUrlsInQueue.get() >= minCountToSkipGeneration) {
                // release the semaphore before early-exiting to avoid a leak in semaphore permits.
                if (acquired) {
                    this.generateUrlsSemaphore.release();
                }

                return;
            }
        }

        // if we're here without acquiring, that implies the numAcquireAttempts went over 300. We're at an impasse
        // and so there's nothing more to be done except error out, as that gives the client a chance to restart.
        if (!acquired) {
            String message = String.format("Could not acquire semaphore to generate URLs. TableRef=%s", tableRef);
            logger.logError(message);
            throw new SFException(ErrorCode.INTERNAL_ERROR, message);
        }

        // we have acquired a semaphore permit at this point, must release before returning

        try {
            GeneratePresignedUrlsResponse response = doGenerateUrls();

            if (initial) {
                this.encodedFigsId = response.getEncodedFigsId();
                this.serializationPolicy = response.getIcebergSerializationPolicy();
                this.locationInfo = response.getIcebergLocationInfo();
                try {
                    this.fileTransferMetadata = InternalStage.createFileTransferMetadataWithAge(this.locationInfo);
                } catch (JsonProcessingException | SnowflakeSQLException | net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException e) {
                    throw new SFException(e, ErrorCode.INTERNAL_ERROR);
                }

                if (this.locationInfo.getIsClientSideEncrypted()) {
                    throw new SFException(ErrorCode.INTERNAL_ERROR, "Cannot ingest into an external volume that requests client side encryption");
                }
                if (this.locationInfo.getLocationType() == "S3") {
                    // add dummy values so that JDBC's S3 client creation doesn't barf on initialization.
                    this.locationInfo.getCredentials().put("AWS_KEY_ID", "key");
                    this.locationInfo.getCredentials().put("AWS_SECRET_KEY", "secret");
                }
            } else {
                if (!this.encodedFigsId.equals(response.getEncodedFigsId())) {
                    logger.logError(
                            "Received unexpected encodedFigsId in ExternalVolume.generateUrls." +
                                    " tableRef=%s sent=%s received=%s",
                            this.tableRef,
                            this.encodedFigsId,
                            response.getEncodedFigsId());

                    throw new SFException(ErrorCode.INTERNAL_ERROR,"The server responded with an unexpected figs ID.");
                }

                if (!this.serializationPolicy.equals(response.getIcebergSerializationPolicy())) {
                    logger.logError(
                            "Received unexpected icebergSerializationPolicy in ExternalVolume.generateUrls." +
                                    " tableRef=%s sent=%s received=%s",
                            this.tableRef,
                            this.serializationPolicy,
                            response.getIcebergSerializationPolicy());

                    throw new SFException(
                            ErrorCode.INTERNAL_ERROR,
                            "The server responded with an unexpected iceberg serialization policy.");
                }

                List<String> urls = response.getPresignedUrls();
                for (String url: urls) {
                    if (url == null || url.isEmpty()) {
                        logger.logError(
                                "Received unexpected null or empty URL in externalVolume.generateUrls" +
                                        " tableRef=%s",
                                this.tableRef);

                        continue;
                    }
                }

                // these are both thread-safe operations individually, and there is no need to do them inside a lock.
                // For an infinitesimal time the numUrlsInQueue will under represent the number of entries in the queue.
                this.presignedUrls.addAll(urls);
                this.numUrlsInQueue.addAndGet(urls.size());
            }
        }
        finally {
            this.generateUrlsSemaphore.release();
        }
    }

    private GeneratePresignedUrlsResponse doGenerateUrls() {
        try {
            return this.serviceClient.generatePresignedUrls(
                    new GeneratePresignedUrlsRequest(tableRef, DEFAULT_PRESIGNED_URL_COUNT, this.encodedFigsId));

        } catch (IngestResponseException | IOException e) {
            throw new SFException(e, ErrorCode.GENERATE_PRESIGNED_URLS_FAILURE, e.getMessage());
        }
    }
}
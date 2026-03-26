/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/StorageObjectSummaryCollection.java
 *
 * Permitted differences: package.
 * Note: Iterator classes (S3ObjectSummariesIterator, AzureObjectSummariesIterator,
 * GcsObjectSummariesIterator) are from JDBC and not replicated here. This class
 * is only needed for the SnowflakeStorageClient interface — the iterator()
 * method is not called by the streaming ingest upload path.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.util.Iterator;
import java.util.List;

/**
 * Provides and iterator over storage object summaries from all supported cloud storage providers
 */
public class StorageObjectSummaryCollection implements Iterable<StorageObjectSummary> {

  private enum storageType {
    S3,
    AZURE,
    GCS
  };

  private final storageType sType;
  private List<S3ObjectSummary> s3ObjSummariesList = null;
  private Iterable<ListBlobItem> azCLoudBlobIterable = null;
  private Page<Blob> gcsIterablePage = null;

  // Constructs platform-agnostic collection of object summaries from S3 object summaries
  public StorageObjectSummaryCollection(List<S3ObjectSummary> s3ObjectSummaries) {
    this.s3ObjSummariesList = s3ObjectSummaries;
    sType = storageType.S3;
  }

  // Constructs platform-agnostic collection of object summaries from an Azure CloudBlobDirectory
  // object
  public StorageObjectSummaryCollection(Iterable<ListBlobItem> azCLoudBlobIterable) {
    this.azCLoudBlobIterable = azCLoudBlobIterable;
    sType = storageType.AZURE;
  }

  public StorageObjectSummaryCollection(Page<Blob> gcsIterablePage) {
    this.gcsIterablePage = gcsIterablePage;
    sType = storageType.GCS;
  }

  @Override
  public Iterator<StorageObjectSummary> iterator() {
    // Iterator classes not replicated — this method is not called by the streaming ingest path.
    throw new UnsupportedOperationException(
        "StorageObjectSummaryCollection.iterator() not supported in ingest SDK");
  }
}

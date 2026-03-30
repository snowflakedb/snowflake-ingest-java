/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/GcsObjectSummariesIterator.java
 *
 * Permitted differences: package.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import java.util.Iterator;

/**
 * Iterator class for ObjectSummary objects on GCS objects. Returns platform-independent instances
 * (StorageObjectSummary)
 */
public class GcsObjectSummariesIterator implements Iterator<StorageObjectSummary> {
  private final Iterator<Blob> blobIterator;

  public GcsObjectSummariesIterator(Page<Blob> blobs) {
    this.blobIterator = blobs.iterateAll().iterator();
  }

  @Override
  public boolean hasNext() {
    return this.blobIterator.hasNext();
  }

  @Override
  public StorageObjectSummary next() {
    Blob blob = this.blobIterator.next();
    return StorageObjectSummary.createFromGcsBlob(blob);
  }
}

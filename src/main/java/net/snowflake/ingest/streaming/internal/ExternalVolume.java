package net.snowflake.ingest.streaming.internal;

/** Handles uploading files to the Iceberg Table's external volume's table data path */
class ExternalVolume implements IStorage {
  @Override
  public void put(BlobPath blobPath, byte[] blob) {
    throw new RuntimeException("not implemented");
  }
}

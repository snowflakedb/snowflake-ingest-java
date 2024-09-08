package net.snowflake.ingest.streaming.internal;

/** Class to manage blob path strings that might have an embedded security token if its a presigned url */
public class BlobPath {
    public final String blobPath;
    public final Boolean hasToken;
    public final String blobPathWithoutToken;

    public BlobPath(String blobPath) {
        this(blobPath, false, blobPath);
    }

    public BlobPath(String blobPath, Boolean hasToken, String blobPathWithoutToken) {
        this.blobPath = blobPath;
        this.hasToken = hasToken;
        this.blobPathWithoutToken = blobPathWithoutToken;
    }
}

package net.snowflake.ingest.connection;

/**
 * Just a wrapper class which is serialised into REST request for insertFiles.
 *
 * <p>This is an optional field which can be passed with a required field "files" in the request
 * body.
 *
 * <p>Here is how the new request could look like
 *
 * <pre>
 * {
 *   "files":[
 *     {
 *       "path":"file1.csv.gz"
 *     },
 *     {
 *       "path":"file2.csv.gz"
 *     }
 *    ],
 *   "clientInfo": {
 *     "clientSequencer": 1,
 *     "offsetToken": "2"
 *    }
 * }
 * </pre>
 */
public class InsertFilesClientInfo {
  // client sequencer which the caller thinks it currently has
  private final long clientSequencer;

  // offsetToken to atomically commit with files.
  private final String offsetToken;

  /** Constructor with both fields as required. */
  public InsertFilesClientInfo(long clientSequencer, String offsetToken) {
    this.clientSequencer = clientSequencer;
    this.offsetToken = offsetToken;
  }

  /** Gets client Sequencer associated with this clientInfo record. */
  public long getClientSequencer() {
    return clientSequencer;
  }

  /** Gets offsetToken associated with this clientInfo record. */
  public String getOffsetToken() {
    return offsetToken;
  }
}

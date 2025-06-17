/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

/**
 * Status of ingest of a particular file returned by Snowflake in the FileEntry history response per
 * file
 *
 * <p>Created by vganesh on 5/22/17.
 */
public enum IngestStatus {
  /** When ingest is still in progress */
  LOAD_IN_PROGRESS(1, "Load in progress"),
  /** Success case when the ingest has completed and the file was loaded */
  LOADED(2, "Loaded"),
  /** If the ingest failed completely */
  LOAD_FAILED(3, "Load failed"),
  /** If ON_ERROR for pipe field is set and the file was loaded with a few errors */
  PARTIALLY_LOADED(4, "Partially loaded"),
  ;

  /**
   * Constructor, set id and description
   *
   * @param statusId Snowflake ID for the corresponding status
   * @param statusDesc description associated to that status
   */
  IngestStatus(int statusId, String statusDesc) {
    this.statusId = statusId;
    this.statusDesc = statusDesc;
  }

  IngestStatus(String statusDesc) throws Exception {
    IngestStatus tmp = IngestStatus.lookupByName(statusDesc);
    this.statusId = tmp.statusId;
    this.statusDesc = tmp.statusDesc;
  }

  /**
   * id of the IngestStatus object
   *
   * @return IngestStatus id
   */
  public int getId() {
    return (this.statusId);
  }

  /**
   * Given an enum name, find associated IngestStatus
   *
   * @param name Status name
   * @return associated IngestStatus type
   */
  public static IngestStatus lookupByName(String name) throws Exception {
    if (name == null) {
      return null;
    }

    return IngestStatus.valueOf(name.toUpperCase());
  }

  /** id of the file Status */
  private final int statusId;

  /** description associated to that file Status */
  private final String statusDesc;
}

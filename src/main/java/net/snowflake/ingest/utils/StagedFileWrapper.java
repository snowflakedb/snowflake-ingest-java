/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteFileWrapper for a file path in the target stage as well as an optional size
 *
 * @author obabarinsa
 */
public class StagedFileWrapper {
  // a logger for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(StagedFileWrapper.class);

  // the name of this file in the stage
  private String path;

  // the size of this file
  private Long size;

  /**
   * StagedFileWrapper - just wraps the path of a file and its size for serialization
   *
   * @param filepath the filepath for this file
   * @param filesize the size of this file
   */
  public StagedFileWrapper(String filepath, Long filesize) {
    // the filepath shouldn't be null
    if (filepath == null) {
      LOGGER.error("Null filepath provided");
      throw new IllegalArgumentException();
    }

    // if we have a negative file size, throw
    if (filesize != null && filesize.longValue() < 0) {
      LOGGER.error("Negative file size provided");
      throw new IllegalArgumentException();
    }

    // set our variables
    path = filepath;
    size = filesize;
  }

  /**
   * StagedFileWrapper - just setting the path without a size
   *
   * @param filepath the filepath for this file
   */
  public StagedFileWrapper(String filepath) {
    this(filepath, null);
  }

  /**
   * getPath - returns the path of this file
   *
   * @return the file path
   */
  public String getPath() {
    return path;
  }

  /**
   * getSize - get the size of this file
   *
   * @return the file size
   */
  public Long getSize() {
    return size;
  }
}

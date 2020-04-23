/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * HistoryResponse - an object containing a response
 * we've received from the history endpoint
 *
 * @author obabarinsa
 */
public class HistoryResponse
{
  private HistoryStats statistics;
  private Boolean completeResult;
  private String pipe;
  private String nextBeginMark;

  /**the list of file status objects*/
  public List<FileEntry> files;

  public HistoryResponse()
  {
    this.files = new ArrayList<>();
  }
  /**
   * HistoryStats - The statistics reported back by the service
   * about the currently loading set of files
   *
   * @author obabarinsa
   */
  static class HistoryStats
  {
    /**how many files are currently processing*/
    public Long activeFiles;
  }

  @Override
  public String toString()
  {
    StringBuilder result = new StringBuilder();
    result.append("\nHistory Result:\n");
    result.append("Pipe: ").append(pipe).append("\n");
    String sep = "";
    for (HistoryResponse.FileEntry file: files)
    {
      result.append(sep).append("{\n");
      result.append(file.toString());
      result.append("}");
      sep = ",\n";
    }
    result.append("\n-------------\n");
    return result.toString();
  }

  /**
   * FileEntry - a pojo containing  all of the data about a file
   * reported back from the service
   */
  public static class FileEntry
  {
    private String path;
    private Long fileSize;
    private String timeReceived;
    private String lastInsertTime;
    private Long rowsInserted;
    private Boolean complete;
    private long rowsParsed;
    private Long errorsSeen;
    private Long errorLimit;
    private String firstError;
    private Long firstErrorLineNum;
    private Long firstErrorCharacterPos;
    private String firstErrorColumnName;
    private String systemError;
    private String stageLocation;
    private IngestStatus status;

    @Override
    public String toString()
    {
      StringBuilder result = new StringBuilder();
      result.append("Path:").append(path).append("\n")
              .append("FileSize: ").append(fileSize) .append("\n")
              .append("TimeReceived: ").append(timeReceived ).append("\n")
              .append("LastInsertTime: ").append(lastInsertTime).append("\n")
              .append("RowsInserted: ").append(rowsInserted).append("\n")
              .append("RowsParsed: ").append(rowsParsed).append("\n")
              .append("ErrorsSeen: ").append(errorsSeen).append("\n")
              .append("ErrorsLimit: ").append(errorLimit).append("\n");
      if (errorsSeen != 0) {
        result.append("FirstError: ").append(firstError).append("\n")
                .append("FirstErrorLineNum: ").append(firstErrorLineNum).append("\n")
                .append("FirstErrorCharacterPos: ").append(firstErrorCharacterPos).append("\n")
                .append("FirstErrorColumnName: ").append(firstErrorColumnName).append("\n")
                .append("SystemError: ").append(systemError).append("\n");
      }

      result.append("StageLocation: ").append(stageLocation).append("\n")
              .append("Status: ").append(status).append("\n")
              .append("Complete: ").append(complete).append("\n");
      return result.toString();
    }

    /** The file path relative to the stage location. */
    public String getPath()
    {
      return path;
    }

    /** The file path relative to the stage location. */
    public void setPath(String path)
    {
      this.path = path;
    }

    /**The size of the file as measured by the service */
    public Long getFileSize()
    {
      return fileSize;
    }

    /**The size of the file as measured by the service */
    public void setFileSize(Long fileSize)
    {
      this.fileSize = fileSize;
    }

    /**The time at which this file was enqueued by the service  ISO 8601 UTC */
    public LocalDateTime getTimeReceived()
    {
      return LocalDateTime.parse(timeReceived, DateTimeFormatter.ISO_DATE_TIME);
    }

    /**The time at which this file was enqueued by the service  ISO 8601 UTC */
    public void setTimeReceived(String timeReceived)
    {
      this.timeReceived = timeReceived;
    }

    /**
     * getLastInsertTime - converts the ISO formatted lastInsertTime string
     * into a LocalDateTime
     *
     * @return a LocalDateTime object representation of our current time
     */
    public LocalDateTime getLastInsertTime()
    {
      return LocalDateTime.parse(lastInsertTime, DateTimeFormatter.ISO_DATE_TIME);
    }

    /**Time data from this file was last inserted into the table. ISO 8601 UTC */
    public void setLastInsertTime(String lastInsertTime)
    {
      this.lastInsertTime = lastInsertTime;
    }

    /**Number of rows inserted into the target table from the file. */
    public Long getRowsInserted()
    {
      return rowsInserted;
    }

    /**Number of rows inserted into the target table from the file. */
    public void setRowsInserted(Long rowsInserted)
    {
      this.rowsInserted = rowsInserted;
    }

    /**Indicates whether the was file completely processed successfully. */
    public Boolean isComplete()
    {
      return complete;
    }

    /**Indicates whether the was file completely processed successfully. */
    public void setComplete(Boolean complete)
    {
      this.complete = complete;
    }

    /** Number of rows parsed from the file. Rows with errors may be skipped. */
    public long getRowsParsed()
    {
      return rowsParsed;
    }

    /** Number of rows parsed from the file. Rows with errors may be skipped. */
    public void setRowsParsed(long rowsParsed)
    {
      this.rowsParsed = rowsParsed;
    }

    /**Number of errors seen in the file*/
    public Long getErrorsSeen()
    {
      return errorsSeen;
    }

    /**Number of errors seen in the file*/
    public void setErrorsSeen(Long errorsSeen)
    {
      this.errorsSeen = errorsSeen;
    }

    /** Number of errors allowed in the file before it is considered failed
     *(based on ON_ERROR copy option).*/
    public Long getErrorLimit()
    {
      return errorLimit;
    }

    /** Number of errors allowed in the file before it is considered failed
     *(based on ON_ERROR copy option).*/
    public void setErrorLimit(Long errorLimit)
    {
      this.errorLimit = errorLimit;
    }

    /** Error message for the first error encountered in this file.*/
    public String getFirstError()
    {
      return firstError;
    }

    /** Error message for the first error encountered in this file.*/
    public void setFirstError(String firstError)
    {
      this.firstError = firstError;
    }

    /** Line number of the first error. */
    public Long getFirstErrorLineNum()
    {
      return firstErrorLineNum;
    }

    /** Line number of the first error. */
    public void setFirstErrorLineNum(Long firstErrorLineNum)
    {
      this.firstErrorLineNum = firstErrorLineNum;
    }

    /** Character position of the first error. */
    public Long getFirstErrorCharacterPos()
    {
      return firstErrorCharacterPos;
    }

    /** Character position of the first error. */
    public void setFirstErrorCharacterPos(Long firstErrorCharacterPos)
    {
      this.firstErrorCharacterPos = firstErrorCharacterPos;
    }

    /** Column name where the first error occurred. */
    public String getFirstErrorColumnName()
    {
      return firstErrorColumnName;
    }

    /** Column name where the first error occurred. */
    public void setFirstErrorColumnName(String firstErrorColumnName)
    {
      this.firstErrorColumnName = firstErrorColumnName;
    }

    /** General error describing why the file was not processed. */
    public String getSystemError()
    {
      return systemError;
    }

    /** General error describing why the file was not processed. */
    public void setSystemError(String systemError)
    {
      this.systemError = systemError;
    }

    /**
     * Either the stage ID (internal stage) or the S3 bucket
     * (external stage) defined in the pipe.
     */
    public String getStageLocation()
    {
      return stageLocation;
    }

    /**
     * Either the stage ID (internal stage) or the S3 bucket
     * (external stage) defined in the pipe.
     */
    public void setStageLocation(String stageLocation)
    {
      this.stageLocation = stageLocation;
    }

    /**
     * Load status for the file:
     *   LOAD_IN_PROGRESS: Part of the file has been loaded into the table,
     *                     but the load process has not completed yet.
     *   LOADED: The entire file has been loaded into the table.
     *   LOAD_FAILED: The file load failed.
     *   PARTIALLY_LOADED: Some rows from this file were loaded successfully,
     *                     but others were not loaded due to errors.
     *                     Processing of this file is completed.
     */
    public IngestStatus getStatus()
    {
      return status;
    }

    /**
     * Load status for the file:
     *   LOAD_IN_PROGRESS: Part of the file has been loaded into the table,
     *                     but the load process has not completed yet.
     *   LOADED: The entire file has been loaded into the table.
     *   LOAD_FAILED: The file load failed.
     *   PARTIALLY_LOADED: Some rows from this file were loaded successfully,
     *                     but others were not loaded due to errors.
     *                     Processing of this file is completed.
     */
    public void setStatus(IngestStatus status)
    {
      this.status = status;
    }

  }

  /**the statistics reported back by the service*/
  public HistoryStats getStatistics()
  {
    return statistics;
  }

  /**the statistics reported back by the service*/
  public void setStatistics(HistoryStats statistics)
  {
    this.statistics = statistics;
  }

  /**False if an event was missed between the supplied beginMark and the
   * first event in this report history. Otherwise, true.*/
  public Boolean isCompleteResult()
  {
    return completeResult;
  }

  /**False if an event was missed between the supplied beginMark and the
   * first event in this report history. Otherwise, true.*/
  public void setCompleteResult(Boolean completeResult)
  {
    this.completeResult = completeResult;
  }

  /**fully qualified pipe name*/
  public String getPipe()
  {
    return pipe;
  }

  /**fully qualified pipe name*/
  public void setPipe(String pipe)
  {
    this.pipe = pipe;
  }

  /**beginMark to use on the next request to avoid seeing duplicate records.
   * (Note that this value is a hint. Duplicates can still occasionally occur.)*/
  public String getNextBeginMark()
  {
    return nextBeginMark;
  }

  /**beginMark to use on the next request to avoid seeing duplicate records.
   * (Note that this value is a hint. Duplicates can still occasionally occur.)*/
  public void setNextBeginMark(String nextBeginMark)
  {
    this.nextBeginMark = nextBeginMark;
  }
}



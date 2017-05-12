package net.snowflake.ingest.connection;

import java.time.LocalDate;
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
  //the statistics reported back by the service
  public HistoryStats statistics;

  //is this a complete response for the request made?
  public Boolean completeResult;

  //fully qualified pipe name
  public String pipe;

  public String nextBeginMark;

  //the list of file status objects
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
    //how many files are currently processing?
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
    //the path in the stage to the file
    public String path;

    //the size of the file as measured by the service
    public Long fileSize;

    //the time at which this file was enqueued by the service  ISO 8601 UTC
    public String timeReceived;

    //the most recent time at which data was inserted. ISO 8601 UTC
    public String lastInsertTime;

    //how many rows have we inserted so far
    public Long rowsInserted;

    //have we completed loading this file
    public Boolean complete;

    public long rowsParsed; //so far
    public Long errorsSeen;
    public Long errorLimit;
    public String firstError;
    public Long firstErrorLineNum;
    public Long firstErrorCharacterPos;
    public String firstErrorColumnName;
    public String systemError;

    public String stageLocation;
    public IngestStatus status;

    /**
     * getLastInsertTime - converts the ISO formatted lastInsertTime string
     * into a LocalDate
     *
     * @return a LocalDate object representation of our current time
     */
    public LocalDate getLastInsertTime()
    {
      return LocalDate.parse(lastInsertTime, DateTimeFormatter.ISO_DATE_TIME);
    }

    //returns the time received as a date time
    public LocalDate getTimeReceived()
    {
      return LocalDate.parse(timeReceived, DateTimeFormatter.ISO_DATE_TIME);
    }

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
  }

}



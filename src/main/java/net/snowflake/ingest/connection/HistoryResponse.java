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
  //the name of the table we're for which we're responding
  public String table;

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
  public static class HistoryStats
  {
    //how many files are currently processing?
    public Long activeFiles;
  }

    @Override
    public String toString()
    {
      String result = "\nHistory Result:\n";
      result += "Table: " + table + "\n";
      result += "Pipe: " + pipe + "\n";
      String sep = "";
      for (HistoryResponse.FileEntry file: files)
      {
        result += sep + "{\n";
        result += file.toString();
        result += "}";
        sep = ",\n";
      }
      result += "\n-------------\n";
      return result;
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
    public String status;

    @Override
    public String toString()
    {
      String result = "Path: "          + path +"\n"
                    + "FileSize: "      + fileSize + "\n"
                    + "TimeReceived: "  + timeReceived + "\n"
                    + "LastInsertTime: "+ lastInsertTime + "\n"
                    + "RowsInserted: "  + rowsInserted + "\n"
                    + "RowsParsed: "    + rowsParsed + "\n"
                    + "ErrorsSeen: "    + errorsSeen + "\n"
                    + "ErrorsLimit: "   + errorLimit + "\n";
      if (errorsSeen != 0) {
        result += "FirstError: " + firstError + "\n"
                + "FirstErrorLineNum: " + firstErrorLineNum + "\n"
                + "FirstErrorCharacterPos: " + firstErrorCharacterPos + "\n"
                + "FirstErrorColumnName: " + firstErrorColumnName + "\n"
                + "SystemError: " + systemError + "\n";
      }

      result += "StageLocation: "  + stageLocation + "\n"
               + "Status: "        + status + "\n"
               + "Complete: "      + complete + "\n";
      return result;
    }
 }

}



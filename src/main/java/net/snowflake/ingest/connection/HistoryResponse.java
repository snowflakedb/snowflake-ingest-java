package net.snowflake.ingest.connection;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

  //the list of file status objects
  public List<FileEntry> files;

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

    //the time at which this file was enqueued by the service
    public String timeReceived;

    //the most recent time at which data was inserted
    public String lastInsertTime;


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

    //how many rows have we inserted so far
    public Long rowsInserted;

    //have we completed loading this file
    public Boolean complete;
  }

}


